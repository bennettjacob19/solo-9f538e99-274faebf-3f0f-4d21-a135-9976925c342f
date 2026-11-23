"""FastAPI 路由层。

并发模型
========
* 所有写事务由单把 ``asyncio.Lock`` 串行化（SQLite 单连接）。
* 每一轮搬迁由一个**独立的后台 driver 任务**推进：每步单独取锁、独立事务
  提交，提交后释放锁并 ``asyncio.sleep(step_delay)`` 让出暂停窗口。
  - 暂停期间其他组的写请求可以立刻拿到锁并在本段停顿内落盘；
  - 多个并发的 ``/migrations/resume`` 或同号覆盖请求会附着到同一个 driver，
    因此都要等当前这一段停顿结束才会看到下一步；
  - 推进请求在暂停中被取消不会连坐 driver：driver 与原请求解耦，按原定
    下一步继续搬完并腾出 spare，无需新请求。
* 读请求不加锁，搬迁中途已提交的块立即可读。
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse

from . import store as store_mod
from .schemas import (
    BlockResponse,
    ConfigResponse,
    MigrationResponse,
    RegionDetailResponse,
    RegionResponse,
    SlotRecord,
    WriteRequest,
)

RETRY_AFTER_SECONDS = "1"


def _to_block_response(group_id: int, block_id: int, row: dict) -> BlockResponse:
    raw: bytes = row["data"]
    return BlockResponse(
        group_id=group_id,
        block_id=block_id,
        seq=row["seq"],
        region_id=row["region_id"],
        slot_index=row["slot_index"],
        length=len(raw),
        data_ascii="".join(chr(b) if 32 <= b < 127 else "." for b in raw),
        data_base64=base64.b64encode(raw).decode("ascii"),
    )


def _block_from_receipt(receipt: dict[str, Any]) -> dict[str, Any]:
    raw = base64.b64decode(receipt["data_base64"])
    return {
        "group_id": receipt["group_id"],
        "block_id": receipt["block_id"],
        "seq": receipt["seq"],
        "region_id": receipt["region_id"],
        "slot_index": receipt["slot_index"],
        "length": len(raw),
        "data_ascii": receipt["data_ascii"],
        "data_base64": receipt["data_base64"],
    }


def create_app() -> FastAPI:
    db_path = os.environ.get("DB_PATH", "/data/partition.db")
    slots = int(os.environ.get("SLOTS_PER_REGION", "8"))
    block_size = int(os.environ.get("BLOCK_SIZE", "16"))
    admin_enabled = os.environ.get("ENABLE_ADMIN", "1") not in ("0", "false", "False")
    step_delay = float(os.environ.get("MIGRATION_STEP_DELAY", "0"))

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        directory = os.path.dirname(db_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        app.state.store = store_mod.PartitionStore(db_path, slots, block_size)
        app.state.write_lock = asyncio.Lock()
        app.state.last_step = None
        app.state.round_steps: dict[str, dict] = {}
        app.state.drivers: dict[str, asyncio.Task] = {}
        app.state.step_delay = step_delay
        # 重启恢复：补完可能停在 finalize 中途的搬迁
        app.state.recovery = app.state.store.recover()
        try:
            yield
        finally:
            drivers = list(app.state.drivers.values())
            for task in drivers:
                task.cancel()
            for task in drivers:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            app.state.store.close()

    app = FastAPI(
        title="分区盘演练 API",
        version="1.1.0",
        description=(
            "4 个等大物理区 / 3 个逻辑组的 append-only 分区盘。"
            "物理槽位只能尾部追加，整区重置才释放；满区覆盖触发 spare 区搬迁，"
            "搬迁分步持久化，崩溃可恢复。写入可带组内递增请求号实现幂等重发。"
            "交互式文档见 /docs。"
        ),
        lifespan=lifespan,
    )

    def get_store(req: Request) -> store_mod.PartitionStore:
        return req.app.state.store

    # ------------------------------------------------------------ 搬迁 driver
    def _public_step(step: dict[str, Any] | None) -> dict[str, Any] | None:
        """对外展示的步骤描述：去掉内嵌的完整回执，避免层层嵌套。"""
        if step is None:
            return None
        return {k: v for k, v in step.items() if k != "write"}

    async def _round_driver(app: FastAPI, round_id: str) -> dict[str, Any] | None:
        """独立推进某一轮搬迁直到 done。每步独立事务、锁外暂停。"""
        store: store_mod.PartitionStore = app.state.store
        last: dict[str, Any] | None = app.state.round_steps.get(round_id)
        while True:
            async with app.state.write_lock:
                m = store.get_migration()
                if m is None or m["round_id"] != round_id:
                    return last
                try:
                    step = store.migration_step(round_id)
                except store_mod.NoMigration:
                    # 轮次刚被切走 / 被管理接口清理：安静收尾
                    return last
            app.state.last_step = _public_step(step)
            app.state.round_steps[round_id] = _public_step(step)
            # 只保留最近若干轮的步骤历史，避免长跑服务内存缓慢增长
            while len(app.state.round_steps) > 64:
                oldest = next(iter(app.state.round_steps))
                if oldest == round_id:
                    break
                app.state.round_steps.pop(oldest, None)
            last = step
            if step["kind"] == "done":
                return step
            # 锁外停顿：其他组可在此窗口内写入；并发请求也等这段停顿结束
            if app.state.step_delay:
                await asyncio.sleep(app.state.step_delay)
            else:
                await asyncio.sleep(0)

    async def _take_over_round(req: Request, round_id: str) -> dict[str, Any] | None:
        """附着到该轮的 driver（没有则创建）并等待其完成。

        driver 与请求任务解耦：请求被取消时 ``asyncio.shield`` 不会取消它，
        搬迁仍按原定下一步继续。
        """
        app = req.app
        task = app.state.drivers.get(round_id)
        if task is not None and task.done():
            # 刚完成的 driver 可能还没被回调移出表；直接复用结果
            return task.result()
        if task is None:
            task = asyncio.create_task(_round_driver(app, round_id))
            app.state.drivers[round_id] = task

            def _forget(done: asyncio.Task, *, rid: str = round_id) -> None:
                # 仅当表里仍是自己时移除，避免误删同号新任务
                if app.state.drivers.get(rid) is done:
                    app.state.drivers.pop(rid, None)
                # 取走异常，避免无人 await 时出现 “Task exception never retrieved”
                if not done.cancelled():
                    with contextlib.suppress(Exception):
                        done.exception()

            task.add_done_callback(_forget)
        return await asyncio.shield(task)

    def _migrated_response(
        group_id: int,
        block_id: int,
        step: dict[str, Any] | None,
        store,
        round_id: str | None = None,
    ) -> dict[str, Any]:
        receipt = step.get("write") if step else None
        if receipt is not None:
            write = _block_from_receipt(receipt)
        else:
            # 无号写入（或轮次已被别的请求完成）：读回该块最新值
            row = store.read_latest(group_id, block_id)
            write = _to_block_response(group_id, block_id, row).model_dump()
        body = {
            "status": "migrated",
            "detail": "原 active 区已满，已完成整区搬迁并写入新值",
            "write": write,
            "receipt": receipt,
            "last_step": _public_step(step),
            "round_id": (step or {}).get("round_id") or round_id,
        }
        if write:
            body.update(
                seq=write["seq"],
                region_id=write["region_id"],
                slot_index=write["slot_index"],
            )
        return body

    def _replay_response(receipt: dict[str, Any]) -> dict[str, Any]:
        write = _block_from_receipt(receipt)
        body = {
            # 回放原回执：状态/内容/序号/物理区号/槽位均与首次完成时一致
            "status": receipt.get("status", "written"),
            "replayed": True,
            "detail": "同号同块同内容，回放原完成回执（未再次写盘）",
            "write": write,
            "receipt": receipt,
            "last_step": receipt.get("last_step"),
            "round_id": receipt.get("round_id"),
            "seq": write["seq"],
            "region_id": write["region_id"],
            "slot_index": write["slot_index"],
        }
        return body

    # ---------------------------------------------------------------- 异常映射
    @app.exception_handler(store_mod.StoreError)
    async def store_error_handler(_: Request, exc: store_mod.StoreError):
        code = status.HTTP_409_CONFLICT
        if isinstance(exc, store_mod.NoSpare):
            code = status.HTTP_503_SERVICE_UNAVAILABLE
        if isinstance(exc, store_mod.NoActiveRegion):
            code = status.HTTP_503_SERVICE_UNAVAILABLE
        return JSONResponse(status_code=code, content={"detail": str(exc)})

    # ---------------------------------------------------------------- 元信息
    @app.get("/config", response_model=ConfigResponse, tags=["meta"])
    def get_config(req: Request):
        store = get_store(req)
        return ConfigResponse(
            regions=store_mod.REGION_COUNT,
            groups=store_mod.GROUP_COUNT,
            slots_per_region=store.slots_per_region,
            block_size=store.block_size,
        )

    @app.get("/health", tags=["meta"])
    def health():
        return {"status": "ok"}

    # ---------------------------------------------------------------- 块读写
    @app.get(
        "/groups/{group_id}/blocks/{block_id}",
        response_model=BlockResponse,
        tags=["blocks"],
        summary="按组号+块号读取定长数据的最新值",
    )
    def read_block(group_id: int, block_id: int, req: Request):
        store = get_store(req)
        if not (0 <= group_id < store_mod.GROUP_COUNT):
            raise HTTPException(422, f"group_id 须在 0..{store_mod.GROUP_COUNT - 1}")
        if not (0 <= block_id < store.slots_per_region):
            raise HTTPException(422, f"block_id 须在 0..{store.slots_per_region - 1}")
        row = store.read_latest(group_id, block_id)
        if row is None:
            raise HTTPException(404, f"组 {group_id} 块 {block_id} 尚无数据")
        return _to_block_response(group_id, block_id, row)

    @app.put(
        "/groups/{group_id}/blocks/{block_id}",
        tags=["blocks"],
        summary="按组号+块号写入定长数据（尾部追加；满区覆盖触发搬迁）",
    )
    async def write_block(group_id: int, block_id: int, body: WriteRequest, req: Request):
        store = get_store(req)
        if not (0 <= group_id < store_mod.GROUP_COUNT):
            raise HTTPException(422, f"group_id 须在 0..{store_mod.GROUP_COUNT - 1}")
        if not (0 <= block_id < store.slots_per_region):
            raise HTTPException(422, f"block_id 须在 0..{store.slots_per_region - 1}")
        try:
            data = body.decode(store.block_size)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        if len(data) != store.block_size:
            raise HTTPException(
                400,
                f"数据长度须等于 BLOCK_SIZE={store.block_size} 字节，实际 {len(data)} 字节",
            )
        request_no = body.request_no
        round_id: str | None = None

        async with req.app.state.write_lock:
            # 1) 已完成回执优先：同号同块同内容直接回放，绝不再次写盘
            if request_no is not None:
                replay = store.check_completed_receipt(
                    group_id, request_no, block_id, data
                )
                if replay is not None:
                    return _replay_response(replay)

            m = store.get_migration()
            if m is not None:
                if m["group_id"] == group_id:
                    # 2) 同号同行内容覆盖：接续原轮，不另起一轮、不重写 pending
                    if request_no is not None:
                        takeover = store.begin_takeover(
                            group_id, request_no, block_id, data
                        )
                        if takeover is not None:
                            round_id = takeover["round_id"]
                        else:
                            # 号更大但本轮尚未搬完：spare 仍被占用，稍后重试
                            return JSONResponse(
                                status_code=503,
                                headers={"Retry-After": RETRY_AFTER_SECONDS},
                                content={
                                    "detail": (
                                        f"组 {group_id} 正在搬迁（物理区 "
                                        f"{m['src_region_id']} -> {m['dst_region_id']}），"
                                        "上一笔带号覆盖搬完后才能继续，请稍后重试"
                                    )
                                },
                            )
                    else:
                        return JSONResponse(
                            status_code=503,
                            headers={"Retry-After": RETRY_AFTER_SECONDS},
                            content={
                                "detail": (
                                    f"组 {group_id} 正在搬迁（物理区 "
                                    f"{m['src_region_id']} -> {m['dst_region_id']}），"
                                    "请稍后重试"
                                )
                            },
                        )
                else:
                    # 其他组的搬迁占用着 spare；本组有槽位仍可写
                    result = store.try_write(
                        group_id, block_id, data, request_no=request_no
                    )
                    if result["status"] == "replayed":
                        return _replay_response(result["receipt"])
                    if result["status"] == "full":
                        return JSONResponse(
                            status_code=503,
                            headers={"Retry-After": RETRY_AFTER_SECONDS},
                            content={
                                "detail": (
                                    f"组 {group_id} 的物理区 {result['region_id']} 已满，"
                                    "spare 区正被其他组搬迁占用，请稍后重试"
                                )
                            },
                        )
                    return {"status": "written", **result}
            else:
                result = store.try_write(group_id, block_id, data, request_no=request_no)
                if result["status"] == "replayed":
                    return _replay_response(result["receipt"])
                if result["status"] == "written":
                    return {"status": "written", **result}

                # 本组 active 区已满：登记一轮搬迁（pending 首步由 driver 落盘）
                started = store.start_migration(
                    group_id, block_id, data, request_no=request_no
                )
                if started["status"] == "replayed":
                    if started.get("receipt") is not None:
                        return _replay_response(started["receipt"])
                    round_id = started["round_id"]  # 同号轮次仍在进行：接管
                else:
                    round_id = started["round_id"]

            # 锁内创建 driver，保证同一轮不会被并发起两个
            if round_id is not None:
                task = req.app.state.drivers.get(round_id)
                if task is None or task.done():
                    req.app.state.drivers[round_id] = asyncio.create_task(
                        _round_driver(req.app, round_id)
                    )

        if round_id is None:  # pragma: no cover - 分支已全部提前返回
            raise HTTPException(500, "搬迁轮次缺失")
        step = await _take_over_round(req, round_id)
        return _migrated_response(group_id, block_id, step, store, round_id)

    # ---------------------------------------------------------------- 物理区
    @app.get(
        "/regions",
        response_model=list[RegionResponse],
        tags=["regions"],
        summary="查看各物理区状态与槽位占用",
    )
    def list_regions(req: Request):
        return get_store(req).region_usage()

    @app.get(
        "/regions/{region_id}",
        response_model=RegionDetailResponse,
        tags=["regions"],
        summary="查看某物理区每个槽位的实际内容",
    )
    def get_region(region_id: int, req: Request):
        store = get_store(req)
        if not (0 <= region_id < store_mod.REGION_COUNT):
            raise HTTPException(422, f"region_id 须在 0..{store_mod.REGION_COUNT - 1}")
        usage = next(
            (u for u in store.region_usage() if u["region_id"] == region_id), None
        )
        records = [
            SlotRecord(
                seq=r["seq"],
                slot_index=r["slot_index"],
                group_id=r["group_id"],
                block_id=r["block_id"],
                data_base64=base64.b64encode(r["data"]).decode("ascii"),
            )
            for r in store.region_records(region_id)
        ]
        return RegionDetailResponse(**usage, records=records)

    # ---------------------------------------------------------------- 搬迁
    @app.get(
        "/migrations",
        response_model=MigrationResponse,
        tags=["migration"],
        summary="查看当前搬迁进度",
    )
    def get_migration(req: Request):
        m = get_store(req).get_migration()
        if m is None:
            return MigrationResponse(in_progress=False)
        return MigrationResponse(
            in_progress=True,
            last_step=req.app.state.round_steps.get(m["round_id"]),
            **m,
        )

    @app.get(
        "/groups/{group_id}/receipt",
        tags=["blocks"],
        summary="查看该组最近一笔带号完成回执",
    )
    def get_receipt(group_id: int, req: Request):
        store = get_store(req)
        if not (0 <= group_id < store_mod.GROUP_COUNT):
            raise HTTPException(422, f"group_id 须在 0..{store_mod.GROUP_COUNT - 1}")
        receipt = store.get_receipt(group_id)
        if receipt is None:
            raise HTTPException(404, f"组 {group_id} 尚无带号完成回执")
        return receipt

    @app.post(
        "/migrations/resume",
        tags=["migration"],
        summary="重启后续跑中断的搬迁（崩溃恢复 / 重试覆盖）",
    )
    async def resume_migration(req: Request, round_id: str | None = None):
        store = get_store(req)
        async with req.app.state.write_lock:
            m = store.get_migration()
            if m is None:
                return {"status": "no_migration", "detail": "当前没有进行中的搬迁"}
            # 只接手调用时刻的那一轮；之后若轮次被切走则不再推进
            target = round_id or m["round_id"]
            if target != m["round_id"]:
                return {
                    "status": "no_migration",
                    "detail": f"轮次 {target} 已不是当前搬迁",
                }
            task = req.app.state.drivers.get(target)
            if task is None or task.done():
                req.app.state.drivers[target] = asyncio.create_task(
                    _round_driver(req.app, target)
                )
        step = await _take_over_round(req, target)
        if step is None:  # pragma: no cover - 竞态兜底
            return {"status": "no_migration", "detail": "搬迁已不在进行中"}
        return {
            "status": "completed",
            "round_id": target,
            "last_step": _public_step(step),
            "receipt": step.get("write"),
        }

    # ---------------------------------------------------------------- 管理
    if admin_enabled:

        @app.post(
            "/admin/regions/{region_id}/reset",
            tags=["admin"],
            summary="【演练】整区重置：释放该区全部槽位并回收为 spare",
        )
        async def reset_region(region_id: int, req: Request):
            if not (0 <= region_id < store_mod.REGION_COUNT):
                raise HTTPException(422, f"region_id 须在 0..{store_mod.REGION_COUNT - 1}")
            async with req.app.state.write_lock:
                get_store(req).admin_reset_region(region_id)
            return {"status": "reset", "region_id": region_id}

        @app.post("/admin/reset-all", tags=["admin"], summary="【演练】清空全部数据")
        async def reset_all(req: Request):
            async with req.app.state.write_lock:
                get_store(req).admin_reset_all()
            req.app.state.last_step = None
            req.app.state.round_steps.clear()
            return Response(status_code=status.HTTP_204_NO_CONTENT)

    return app


# uvicorn --factory app.main:create_app 启动；测试中也直接调用 create_app()
__all__ = ["create_app"]
