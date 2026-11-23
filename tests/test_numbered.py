"""请求号幂等覆盖、回执持久化与搬运续跑测试。"""

import asyncio
import base64
import contextlib
import os
import time

import httpx
import pytest

from app import store as sm


def v(ch: str, block_size: int = 8) -> bytes:
    return (ch * block_size).encode()


# --------------------------------------------------------------------- store
def test_numbered_receipt_fields_and_replay(tmp_path):
    s = sm.PartitionStore(str(tmp_path / "p.db"), 3, 8)
    first = s.try_write(0, 1, v("a"), request_no=1)
    assert first["status"] == "written"
    receipt = first["receipt"]
    assert receipt["request_no"] == 1
    assert receipt["data_base64"] == base64.b64encode(v("a")).decode()
    assert receipt["data_ascii"] == "a" * 8
    assert receipt["seq"] == first["seq"]
    assert receipt["region_id"] == first["region_id"]
    assert receipt["slot_index"] == first["slot_index"]

    before = len(s.region_records(first["region_id"]))
    replay = s.try_write(0, 1, v("a"), request_no=1)
    assert replay["status"] == "replayed"
    assert replay["receipt"] == receipt
    # 同号同块同内容不再落盘
    assert len(s.region_records(first["region_id"])) == before
    assert s.get_receipt(0) == receipt
    s.close()


def test_numbered_conflicts_do_not_change_disk(tmp_path):
    s = sm.PartitionStore(str(tmp_path / "p.db"), 3, 8)
    s.try_write(0, 0, v("a"), request_no=5)
    usage_before = s.region_usage()
    records_before = {
        rid: s.region_records(rid) for rid in range(sm.REGION_COUNT)
    }

    with pytest.raises(sm.RequestConflict):
        s.try_write(0, 0, v("a"), request_no=4)  # 号回退
    with pytest.raises(sm.RequestConflict):
        s.try_write(0, 1, v("a"), request_no=5)  # 同号换块
    with pytest.raises(sm.RequestConflict):
        s.try_write(0, 0, v("z"), request_no=5)  # 同号换内容

    assert s.region_usage() == usage_before
    assert {rid: s.region_records(rid) for rid in range(sm.REGION_COUNT)} == records_before
    assert s.read_latest(0, 0)["data"] == v("a")
    s.close()


def test_receipt_survives_region_recycle_without_rewrite(tmp_path):
    s = sm.PartitionStore(str(tmp_path / "p.db"), 3, 8)
    for b in range(3):
        s.try_write(0, b, v("a"), request_no=b + 1)
    s.start_migration(0, 0, v("z"), request_no=9)
    while s.get_migration() is not None:
        s.migration_step()
    receipt = s.get_receipt(0)
    assert receipt["request_no"] == 9 and receipt["status"] == "migrated"
    active = [r for r in s.region_usage() if r["group_id"] == 0][0]
    assert receipt["region_id"] == active["region_id"]

    # 整区被回收为 spare 后，原回执仍可回放且不写盘
    recycled = active["region_id"]
    s.admin_reset_region(recycled)
    assert s.read_latest(0, 0) is None
    usage_before = s.region_usage()
    replay = s.check_completed_receipt(0, 9, 0, v("z"))
    assert replay == receipt
    assert s.region_usage() == usage_before
    with pytest.raises(sm.RequestConflict):
        s.check_completed_receipt(0, 9, 1, v("z"))
    s.close()


def test_receipt_replaced_only_when_next_numbered_round_finishes(tmp_path):
    s = sm.PartitionStore(str(tmp_path / "p.db"), 2, 8)
    s.try_write(0, 0, v("a"), request_no=1)
    s.try_write(0, 1, v("b"), request_no=2)
    # 第一轮带号覆盖搬完 -> 回执 3
    s.start_migration(0, 0, v("z"), request_no=3)
    while s.get_migration() is not None:
        s.migration_step()
    assert s.get_receipt(0)["request_no"] == 3

    # 再次写满，第二轮带号覆盖 5：搬运中途旧回执保持，done 才替换
    full = s.try_write(0, 0, v("w"), request_no=4)
    assert full["status"] == "full"  # 满区且尚未启动搬迁：不产生完成回执
    assert s.get_receipt(0)["request_no"] == 3
    started = s.start_migration(0, 1, v("q"), request_no=5)
    assert started["status"] == "started"
    assert s.get_receipt(0)["request_no"] == 3
    while s.get_migration() is not None:
        step = s.migration_step()
        if step["kind"] != "done":
            assert s.get_receipt(0)["request_no"] == 3
        else:
            assert s.get_receipt(0)["request_no"] == 5
    assert s.get_receipt(0)["block_id"] == 1
    s.close()


def test_migration_step_rejects_stale_round(tmp_path):
    s = sm.PartitionStore(str(tmp_path / "p.db"), 2, 8)
    s.try_write(0, 0, v("a"), request_no=1)
    s.try_write(0, 1, v("b"), request_no=2)
    started = s.start_migration(0, 0, v("z"), request_no=3)
    with pytest.raises(sm.NoMigration):
        s.migration_step(round_id="stale-round")
    assert s.get_migration()["round_id"] == started["round_id"]
    s.migration_step()  # 仍然可以正常推进当前轮
    s.close()


# ----------------------------------------------------------------------- API
@pytest.fixture()
def client(tmp_path, monkeypatch):
    db = tmp_path / "api.db"
    monkeypatch.setenv("DB_PATH", str(db))
    monkeypatch.setenv("SLOTS_PER_REGION", "3")
    monkeypatch.setenv("BLOCK_SIZE", "8")
    monkeypatch.setenv("MIGRATION_STEP_DELAY", "0")
    monkeypatch.setenv("ENABLE_ADMIN", "1")
    from fastapi.testclient import TestClient
    import app.main as main_mod

    with TestClient(main_mod.create_app()) as c:
        yield c


def payload(ch: str, request_no: int | None = None) -> dict:
    body = {"data": ch * 8}
    if request_no is not None:
        body["request_no"] = request_no
    return body


def test_api_numbered_replay_and_409(client):
    r = client.put("/groups/0/blocks/1", json=payload("a", 1))
    assert r.status_code == 200
    receipt = r.json()["receipt"]
    assert receipt["data_ascii"] == "a" * 8
    assert receipt["region_id"] == r.json()["region_id"]
    assert receipt["slot_index"] == r.json()["slot_index"]

    used = client.get("/regions").json()
    r = client.put("/groups/0/blocks/1", json=payload("a", 1))
    assert r.status_code == 200
    assert r.json()["replayed"] is True
    assert r.json()["seq"] == receipt["seq"]
    assert r.json()["region_id"] == receipt["region_id"]
    assert r.json()["slot_index"] == receipt["slot_index"]
    assert client.get("/regions").json() == used  # 未再写盘

    assert client.put("/groups/0/blocks/1", json=payload("a", 0)).status_code == 409
    assert client.put("/groups/0/blocks/2", json=payload("a", 1)).status_code == 409
    assert client.put("/groups/0/blocks/1", json=payload("z", 1)).status_code == 409
    assert client.get("/regions").json() == used
    assert client.get("/groups/0/blocks/1").json()["data_ascii"] == "a" * 8


def test_api_migrated_receipt_and_replay(client):
    for b in range(3):
        assert client.put(f"/groups/0/blocks/{b}", json=payload("a", b + 1)).status_code == 200
    r = client.put("/groups/0/blocks/0", json=payload("z", 9))
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "migrated"
    assert body["round_id"] == body["receipt"]["round_id"]
    assert body["last_step"]["kind"] == "done"
    receipt = body["receipt"]
    assert receipt["status"] == "migrated"
    assert receipt["request_no"] == 9
    assert receipt["data_ascii"] == "z" * 8
    assert receipt["region_id"] == body["region_id"]
    assert receipt["slot_index"] == body["slot_index"]

    used = client.get("/regions").json()
    replay = client.put("/groups/0/blocks/0", json=payload("z", 9))
    assert replay.status_code == 200
    assert replay.json()["replayed"] is True
    assert replay.json()["seq"] == receipt["seq"]
    assert replay.json()["region_id"] == receipt["region_id"]
    assert replay.json()["slot_index"] == receipt["slot_index"]
    assert client.get("/regions").json() == used

    # 回执持久化在库里，且每组只有最近一笔
    got = client.get("/groups/0/receipt").json()
    assert got["request_no"] == 9
    assert client.get("/groups/1/receipt").status_code == 404


def test_restart_resend_numbered_overwrite_takes_over_round(tmp_path, monkeypatch):
    db = tmp_path / "crash.db"
    monkeypatch.setenv("DB_PATH", str(db))
    monkeypatch.setenv("SLOTS_PER_REGION", "3")
    monkeypatch.setenv("BLOCK_SIZE", "8")
    monkeypatch.setenv("MIGRATION_STEP_DELAY", "0")
    from fastapi.testclient import TestClient
    import app.main as main_mod

    # 第一生命周期：写满 -> 登记带号搬迁 -> 只推进 pending 后“停机”
    with TestClient(main_mod.create_app()) as c1:
        for b in range(3):
            c1.put(f"/groups/0/blocks/{b}", json=payload("a", b + 1))
        store = c1.app.state.store
        started = store.start_migration(0, 0, v("z"), request_no=7)
        store.migration_step()  # pending 已落盘，但客户端还没收到回包
        assert store.get_receipt(0)["request_no"] == 3  # 旧回执尚未替换

    # 第二生命周期：原 SQLite 卷重启后，同号覆盖重发即接续原轮
    with TestClient(main_mod.create_app()) as c2:
        assert c2.get("/migrations").json()["in_progress"] is True
        assert c2.get("/migrations").json()["round_id"] == started["round_id"]
        r = c2.put("/groups/0/blocks/0", json=payload("z", 7))
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "migrated"
        assert body["round_id"] == started["round_id"]
        assert body["receipt"]["request_no"] == 7
        assert body["receipt"]["data_ascii"] == "z" * 8
        assert body["last_step"]["kind"] == "done"
        assert c2.get("/migrations").json()["in_progress"] is False
        # 本轮 pending 只落了一次盘：新 active 区恰好 3 个槽位
        active = [x for x in c2.get("/regions").json() if x["group_id"] == 0][0]
        assert active["used_slots"] == 3
        assert [x for x in c2.get("/regions").json() if x["status"] == "spare"]

        # 回包后再次重发：同号直接回放原回执，不写盘
        used = c2.get("/regions").json()
        again = c2.put("/groups/0/blocks/0", json=payload("z", 7))
        assert again.json()["replayed"] is True
        assert again.json()["seq"] == body["seq"]
        assert c2.get("/regions").json() == used


def test_resume_wrong_round_id_is_noop(client):
    for b in range(3):
        client.put(f"/groups/0/blocks/{b}", json=payload("a", b + 1))
    store = client.app.state.store
    started = store.start_migration(0, 0, v("z"), request_no=4)
    r = client.post("/migrations/resume", params={"round_id": "not-this-round"})
    assert r.json()["status"] == "no_migration"
    assert client.get("/migrations").json()["in_progress"] is True
    r = client.post("/migrations/resume", params={"round_id": started["round_id"]})
    assert r.json()["status"] == "completed"
    assert r.json()["round_id"] == started["round_id"]
    assert r.json()["receipt"]["request_no"] == 4
    assert client.get("/migrations").json()["in_progress"] is False


def test_late_resend_cannot_overwrite_newer_value(client):
    """回包丢失后重发旧号，不得盖掉后写（更大号）已生效的值。"""
    for b in range(3):
        client.put(f"/groups/0/blocks/{b}", json=payload("a", b + 1))
    first = client.put("/groups/0/blocks/0", json=payload("z", 4))
    assert first.status_code == 200 and first.json()["status"] == "migrated"
    # 后写：更大号覆盖同一块
    later = client.put("/groups/0/blocks/0", json=payload("w", 5))
    assert later.status_code == 200
    assert client.get("/groups/0/blocks/0").json()["data_ascii"] == "w" * 8

    used = client.get("/regions").json()
    stale = client.put("/groups/0/blocks/0", json=payload("z", 4))
    assert stale.status_code == 409
    assert client.get("/regions").json() == used
    assert client.get("/groups/0/blocks/0").json()["data_ascii"] == "w" * 8
    assert client.get("/groups/0/receipt").json()["request_no"] == 5


def test_in_progress_round_conflicts_are_409_and_disk_unchanged(client):
    for b in range(3):
        client.put(f"/groups/0/blocks/{b}", json=payload("a", b + 1))
    store = client.app.state.store
    store.start_migration(0, 0, v("z"), request_no=4)
    before_regions = client.get("/regions").json()
    before_records = {
        rid: client.get(f"/regions/{rid}").json()["records"] for rid in range(4)
    }

    # 同号换块 / 换内容：409，盘面不变
    assert client.put("/groups/0/blocks/1", json=payload("z", 4)).status_code == 409
    assert client.put("/groups/0/blocks/0", json=payload("q", 4)).status_code == 409
    # 号回退：409
    assert client.put("/groups/0/blocks/0", json=payload("z", 3)).status_code == 409
    # 更大的新号也需等本轮搬完：503（spare 仍被占用），不是错误地另起一轮
    assert client.put("/groups/0/blocks/0", json=payload("q", 5)).status_code == 503
    assert client.get("/regions").json() == before_regions
    assert {
        rid: client.get(f"/regions/{rid}").json()["records"] for rid in range(4)
    } == before_records
    # 同号同块同内容：接续原轮并搬完，回执替换为这一笔
    r = client.put("/groups/0/blocks/0", json=payload("z", 4))
    assert r.status_code == 200
    assert r.json()["status"] == "migrated"
    assert r.json()["receipt"]["request_no"] == 4
    assert client.get("/migrations").json()["in_progress"] is False
    assert client.get("/groups/0/receipt").json()["request_no"] == 4


# --------------------------------------------------- 停顿 / 并发 / 取消（异步）
@contextlib.asynccontextmanager
async def async_app(tmp_path, monkeypatch, *, delay: float):
    db = tmp_path / "async.db"
    monkeypatch.setenv("DB_PATH", str(db))
    monkeypatch.setenv("SLOTS_PER_REGION", "3")
    monkeypatch.setenv("BLOCK_SIZE", "8")
    monkeypatch.setenv("MIGRATION_STEP_DELAY", str(delay))
    monkeypatch.setenv("ENABLE_ADMIN", "1")
    import app.main as main_mod

    app = main_mod.create_app()
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            yield app, c


async def _fill_group0(c, request_prefix: int = 1):
    for b in range(3):
        r = await c.put(
            f"/groups/0/blocks/{b}",
            json={"data": "a" * 8, "request_no": request_prefix + b},
        )
        assert r.status_code == 200


async def _wait_for_step(c, kind: str | None = None, timeout: float = 5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        m = (await c.get("/migrations")).json()
        if m["in_progress"] and (kind is None or (m.get("last_step") or {}).get("kind") == kind):
            return m
        await asyncio.sleep(0.01)
    raise AssertionError("等待搬迁步骤超时")


@pytest.mark.asyncio
async def test_concurrent_resume_waits_for_step_delay(tmp_path, monkeypatch):
    delay = 0.15
    async with async_app(tmp_path, monkeypatch, delay=delay) as (app, c):
        await _fill_group0(c)
        store = app.state.store
        store.start_migration(0, 0, v("z"), request_no=4)
        # pending + 2 个拷贝 + done = 4 步 -> 至少 3 段停顿
        loop = asyncio.get_running_loop()
        t0 = loop.time()
        r1, r2 = await asyncio.gather(
            c.post("/migrations/resume"), c.post("/migrations/resume")
        )
        elapsed = loop.time() - t0
        assert r1.json()["status"] == "completed"
        assert r2.json()["status"] == "completed"
        assert r1.json()["round_id"] == r2.json()["round_id"]
        assert elapsed >= delay * 2.5, elapsed
        assert (await c.get("/migrations")).json()["in_progress"] is False


@pytest.mark.asyncio
async def test_other_group_write_lands_during_pause(tmp_path, monkeypatch):
    delay = 0.6
    async with async_app(tmp_path, monkeypatch, delay=delay) as (app, c):
        await _fill_group0(c)
        store = app.state.store
        store.start_migration(0, 0, v("z"), request_no=4)
        task = asyncio.create_task(c.post("/migrations/resume"))
        # 第一段停顿开始（pending 步已提交）
        await _wait_for_step(c, "pending")
        t0 = time.monotonic()
        r = await c.put(
            "/groups/1/blocks/0", json={"data": "g" * 8, "request_no": 1}
        )
        elapsed = time.monotonic() - t0
        assert r.status_code == 200
        assert r.json()["status"] == "written"
        assert elapsed < delay / 2, elapsed  # 在停顿内落盘，未等整轮搬迁
        assert (await c.get("/migrations")).json()["in_progress"] is True
        assert (await c.get("/groups/1/blocks/0")).json()["data_ascii"] == "g" * 8
        await task


@pytest.mark.asyncio
async def test_cancel_resume_mid_pause_still_finishes(tmp_path, monkeypatch):
    delay = 0.25
    async with async_app(tmp_path, monkeypatch, delay=delay) as (app, c):
        await _fill_group0(c)
        store = app.state.store
        started = store.start_migration(0, 0, v("z"), request_no=4)
        task = asyncio.create_task(c.post("/migrations/resume"))
        await _wait_for_step(c, "pending")
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # 不再发新请求，driver 按原定下一步搬完并腾出 spare
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            m = (await c.get("/migrations")).json()
            if not m["in_progress"]:
                break
            await asyncio.sleep(0.02)
        assert (await c.get("/migrations")).json()["in_progress"] is False
        regions = (await c.get("/regions")).json()
        assert [r for r in regions if r["status"] == "spare"]
        assert (await c.get("/groups/0/blocks/0")).json()["data_ascii"] == "z" * 8
        receipt = (await c.get("/groups/0/receipt")).json()
        assert receipt["request_no"] == 4
        assert receipt["round_id"] == started["round_id"]
