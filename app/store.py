"""分区盘存储引擎。

物理模型
========
* 4 个等大物理区（region），每区 ``slots_per_region`` 个定长槽位。
* 3 个逻辑组（group 0..2），每组块号 0..slots_per_region-1，各占一个 active 区。
* 始终保留 1 个 spare 区，用于满区搬迁。

不变量
======
* 记录只能尾部追加（INSERT），物理槽位永不原地更新；读取取 (group, block)
  序列号最大的那条记录。
* 只有整区重置（搬迁收尾时的 DELETE + 状态翻转）才能释放槽位。
* 搬迁每一步都是独立事务，提交后即持久化，因此：
  - 已拷贝 / 已写入的块在搬迁中途即可读到新值；
  - 任意时刻停机，重启后状态都自洽，搬迁可幂等续跑。

区状态机：unused -> active -> migrating -> (源区) reset -> spare
          unused/spare 可在首次写入时被分配为 active

幂等覆盖（请求号）
==================
写入请求可携带“组内递增请求号”（``request_no``）。存储层为每个组只保留
**最近一笔带号完成回执**（``group_receipts`` 表，每组一行）：

* 同号 + 同块 + 同内容：直接回放原回执，不再写盘（即使槽位已被整区回收）；
* 请求号小于最近完成号，或同号但块号 / 内容不同：抛 :class:`RequestConflict`
  （API 映射为 409），事务回滚，盘面不变；
* 同一个号还可以用于续跑接手进行中的轮次：同号同块同内容命中
  ``migration.request_no`` 时，只推进该轮，不重复落盘 pending；
* 带号写入触发满区搬迁时，旧回执保持不变；只有该轮搬迁 finalize 的同一个
  事务里才用新回执替换它。
"""

from __future__ import annotations

import base64
import json
import sqlite3
import uuid
from typing import Any

REGION_COUNT = 4
GROUP_COUNT = 3

# 区状态
ST_UNUSED = "unused"
ST_SPARE = "spare"
ST_ACTIVE = "active"
ST_MIGRATING = "migrating"          # 搬迁目标区（新区）
ST_MIGRATING_SRC = "migrating_src"  # 搬迁源区（旧区，冻结只读）
ST_RESET = "reset"  # 仅在 finalize 事务内瞬间出现


class StoreError(Exception):
    """存储层基础异常（API 默认映射为 409）。"""


class RequestConflict(StoreError):
    """请求号与已有完成回执 / 进行中的轮次冲突（409）。"""


class RegionFull(StoreError):
    """目标区槽位已满。"""


class NoSpare(StoreError):
    """没有可用的 spare 区，无法启动搬迁。"""


class NoActiveRegion(StoreError):
    """该组当前没有可写的 active 区（搬迁进行中）。"""


class NoMigration(StoreError):
    """当前没有进行中的搬迁，或指定的轮次已被切换。"""


def _ascii(data: bytes) -> str:
    return "".join(chr(b) if 32 <= b < 127 else "." for b in data)


def _receipt_payload(
    *,
    group_id: int,
    block_id: int,
    request_no: int | None,
    seq: int,
    region_id: int,
    slot_index: int,
    status: str,
    round_id: str | None,
    data: bytes,
) -> dict[str, Any]:
    """回执内容：当时的内容、序号、物理区号、槽位。"""
    return {
        "group_id": group_id,
        "block_id": block_id,
        "request_no": request_no,
        "seq": seq,
        "region_id": region_id,
        "slot_index": slot_index,
        "status": status,
        "round_id": round_id,
        "data_base64": base64.b64encode(data).decode("ascii"),
        "data_ascii": _ascii(data),
    }


class PartitionStore:
    """分区盘引擎；所有方法内部自带事务，线程安全依赖外层串行化。"""

    def __init__(
        self,
        db_path: str,
        slots_per_region: int,
        block_size: int,
    ) -> None:
        if slots_per_region <= 0 or block_size <= 0:
            raise ValueError("slots_per_region 与 block_size 必须为正数")
        self._slots = int(slots_per_region)
        self._block_size = int(block_size)
        # check_same_thread=False：FastAPI 端点线程与 lifespan 线程不同；
        # 写操作由应用层 asyncio.Lock 串行化，读为单条短事务，安全
        self._conn = sqlite3.connect(
            db_path, isolation_level=None, check_same_thread=False
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()

    # ------------------------------------------------------------------ 基础
    def close(self) -> None:
        self._conn.close()

    @property
    def slots_per_region(self) -> int:
        return self._slots

    @property
    def block_size(self) -> int:
        return self._block_size

    def _init_schema(self) -> None:
        c = self._conn
        c.execute("BEGIN IMMEDIATE")
        try:
            # 注意：不能用 executescript，它会隐式提交当前事务
            for ddl in (
                """
                CREATE TABLE IF NOT EXISTS config (
                    key   TEXT PRIMARY KEY,
                    value INTEGER NOT NULL
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS regions (
                    region_id INTEGER PRIMARY KEY,
                    status    TEXT NOT NULL,
                    group_id  INTEGER
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS records (
                    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
                    region_id  INTEGER NOT NULL REFERENCES regions(region_id),
                    slot_index INTEGER NOT NULL,
                    group_id   INTEGER NOT NULL,
                    block_id   INTEGER NOT NULL,
                    data       BLOB NOT NULL
                )
                """,
                "CREATE INDEX IF NOT EXISTS idx_records_lookup"
                " ON records(group_id, block_id, seq)",
                "CREATE INDEX IF NOT EXISTS idx_records_region"
                " ON records(region_id, slot_index)",
                """
                CREATE TABLE IF NOT EXISTS migration (
                    id                    INTEGER PRIMARY KEY CHECK (id = 1),
                    round_id              TEXT,
                    src_region_id         INTEGER NOT NULL REFERENCES regions(region_id),
                    dst_region_id         INTEGER NOT NULL REFERENCES regions(region_id),
                    group_id              INTEGER NOT NULL,
                    request_no            INTEGER,
                    pending_block_id      INTEGER NOT NULL,
                    pending_data          BLOB NOT NULL,
                    pending_applied       INTEGER NOT NULL DEFAULT 0,
                    pending_slot_index    INTEGER,
                    pending_seq           INTEGER,
                    last_copied_block_id  INTEGER
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS group_receipts (
                    group_id     INTEGER PRIMARY KEY,
                    request_no   INTEGER NOT NULL,
                    block_id     INTEGER NOT NULL,
                    data         BLOB NOT NULL,
                    receipt      TEXT NOT NULL
                )
                """,
            ):
                c.execute(ddl)
            self._migrate_schema(c)
            # 配置持久化：库已存在时以库内参数为准，保证重启布局不变
            cfg = {
                r["key"]: r["value"]
                for r in c.execute("SELECT key, value FROM config").fetchall()
            }
            if not cfg:
                c.executemany(
                    "INSERT INTO config(key, value) VALUES(?, ?)",
                    [
                        ("slots_per_region", self._slots),
                        ("block_size", self._block_size),
                    ],
                )
            else:
                self._slots = int(cfg["slots_per_region"])
                self._block_size = int(cfg["block_size"])

            if c.execute("SELECT COUNT(*) FROM regions").fetchone()[0] == 0:
                # 0 号区作为初始 spare，其余三个等待分配给三个组
                c.execute(
                    "INSERT INTO regions(region_id, status, group_id) VALUES (0, ?, NULL)",
                    (ST_SPARE,),
                )
                for rid in range(1, REGION_COUNT):
                    c.execute(
                        "INSERT INTO regions(region_id, status, group_id) VALUES (?, ?, NULL)",
                        (rid, ST_UNUSED),
                    )
            c.execute("COMMIT")
        except Exception:
            self._rollback_safely()
            raise

    def _migrate_schema(self, c: sqlite3.Connection) -> None:
        """兼容旧卷：为缺少的迁移列补默认值（SQLite ADD COLUMN 可空）。"""
        columns = {
            r["name"]
            for r in c.execute("PRAGMA table_info(migration)").fetchall()
        }
        additions = (
            ("round_id", "TEXT"),
            ("request_no", "INTEGER"),
            ("pending_slot_index", "INTEGER"),
            ("pending_seq", "INTEGER"),
        )
        for name, decl in additions:
            if name not in columns:
                c.execute(f"ALTER TABLE migration ADD COLUMN {name} {decl}")
        if "round_id" not in columns:
            # 旧行补一个稳定的轮次标识，续跑时可被接管
            for row in c.execute(
                "SELECT id FROM migration WHERE round_id IS NULL"
            ).fetchall():
                c.execute(
                    "UPDATE migration SET round_id=? WHERE id=?",
                    (uuid.uuid4().hex, row["id"]),
                )

    def _check_data(self, data: bytes) -> None:
        if not isinstance(data, (bytes, bytearray)):
            raise StoreError("data 必须是 bytes")
        if len(data) != self._block_size:
            raise StoreError(
                f"数据长度必须等于 BLOCK_SIZE={self._block_size} 字节，"
                f"实际 {len(data)} 字节"
            )

    def _rollback_safely(self) -> None:
        try:
            self._conn.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass  # 事务已不存在（如隐式提交），原始异常照常向上抛

    def _append(self, region_id: int, group_id: int, block_id: int, data: bytes) -> dict:
        """在指定区尾部追加一条记录（调用方需已开启事务）。"""
        used = self._conn.execute(
            "SELECT COUNT(*) FROM records WHERE region_id=?", (region_id,)
        ).fetchone()[0]
        if used >= self._slots:
            raise RegionFull(f"物理区 {region_id} 槽位已满")
        cur = self._conn.execute(
            "INSERT INTO records(region_id, slot_index, group_id, block_id, data)"
            " VALUES (?, ?, ?, ?, ?)",
            (region_id, used, group_id, block_id, sqlite3.Binary(bytes(data))),
        )
        return {"seq": cur.lastrowid, "region_id": region_id, "slot_index": used}

    def _find_active(self, group_id: int) -> int | None:
        row = self._conn.execute(
            "SELECT region_id FROM regions WHERE group_id=? AND status=?",
            (group_id, ST_ACTIVE),
        ).fetchone()
        return None if row is None else row[0]

    # --------------------------------------------------------------- 回执
    def _get_receipt_row(self, group_id: int) -> sqlite3.Row | None:
        return self._conn.execute(
            "SELECT * FROM group_receipts WHERE group_id=?", (group_id,)
        ).fetchone()

    @staticmethod
    def _decode_receipt(row: sqlite3.Row) -> dict[str, Any]:
        return json.loads(row["receipt"])

    def get_receipt(self, group_id: int) -> dict[str, Any] | None:
        """该组最近一笔带号完成回执（没有则为 None）。"""
        row = self._get_receipt_row(group_id)
        return None if row is None else self._decode_receipt(row)

    def check_completed_receipt(
        self, group_id: int, request_no: int, block_id: int, data: bytes
    ) -> dict[str, Any] | None:
        """只读校验请求号：同号同块同内容返回原回执，否则按规则抛冲突。

        该检查不开启写事务；调用方须已持有应用层写锁。
        """
        self._check_data(data)
        return self._check_request_no(
            group_id, request_no, block_id, data, self._get_receipt_row(group_id)
        )

    def _save_receipt(
        self,
        group_id: int,
        request_no: int,
        block_id: int,
        data: bytes,
        receipt: dict[str, Any],
    ) -> None:
        self._conn.execute(
            "INSERT INTO group_receipts(group_id, request_no, block_id, data, receipt)"
            " VALUES (?, ?, ?, ?, ?)"
            " ON CONFLICT(group_id) DO UPDATE SET"
            "   request_no=excluded.request_no,"
            "   block_id=excluded.block_id,"
            "   data=excluded.data,"
            "   receipt=excluded.receipt",
            (
                group_id,
                request_no,
                block_id,
                sqlite3.Binary(bytes(data)),
                json.dumps(receipt, ensure_ascii=False, sort_keys=True),
            ),
        )

    def _check_request_no(
        self,
        group_id: int,
        request_no: int,
        block_id: int,
        data: bytes,
        receipt_row: sqlite3.Row | None,
    ) -> dict[str, Any] | None:
        """请求号前置校验。

        返回非 None 表示“同号同块同内容”的已完成回执，可直接回放；
        冲突时抛 :class:`RequestConflict`（调用方负责回滚事务）。
        """
        last = receipt_row["request_no"] if receipt_row is not None else None
        if last is not None:
            if request_no < last:
                raise RequestConflict(
                    f"请求号 {request_no} 小于组 {group_id} 最近完成号 {last}"
                )
            if request_no == last:
                same_block = receipt_row["block_id"] == block_id
                same_data = bytes(receipt_row["data"]) == bytes(data)
                if not (same_block and same_data):
                    raise RequestConflict(
                        f"请求号 {request_no} 已用于组 {group_id} 块 "
                        f"{receipt_row['block_id']}，不能改用于块 {block_id} 或其他内容"
                    )
                return self._decode_receipt(receipt_row)
        return None

    # ------------------------------------------------------------------ 读路径
    def read_latest(self, group_id: int, block_id: int) -> dict[str, Any] | None:
        """返回逻辑块的最新值；搬迁期间新区已落盘的记录会自然胜出。"""
        row = self._conn.execute(
            "SELECT seq, region_id, slot_index, data FROM records"
            " WHERE group_id=? AND block_id=? ORDER BY seq DESC LIMIT 1",
            (group_id, block_id),
        ).fetchone()
        if row is None:
            return None
        return {
            "seq": row["seq"],
            "region_id": row["region_id"],
            "slot_index": row["slot_index"],
            "data": bytes(row["data"]),
        }

    def region_usage(self) -> list[dict[str, Any]]:
        """各区槽位占用：used=已追加槽位（含历史版本），valid=当前有效逻辑块数。"""
        rows = self._conn.execute(
            """
            SELECT r.region_id, r.status, r.group_id,
                   (SELECT COUNT(*) FROM records rec
                      WHERE rec.region_id = r.region_id) AS used_slots,
                   (SELECT COUNT(DISTINCT rec.block_id) FROM records rec
                      WHERE rec.region_id = r.region_id) AS valid_blocks
              FROM regions r
             ORDER BY r.region_id
            """
        ).fetchall()
        return [
            {
                "region_id": r["region_id"],
                "status": r["status"],
                "group_id": r["group_id"],
                "used_slots": r["used_slots"],
                "total_slots": self._slots,
                "free_slots": self._slots - r["used_slots"],
                "valid_blocks": r["valid_blocks"],
            }
            for r in rows
        ]

    def region_records(self, region_id: int) -> list[dict[str, Any]]:
        """查看某物理区每个槽位的实际内容（演练可视化用）。"""
        rows = self._conn.execute(
            "SELECT seq, slot_index, group_id, block_id, data FROM records"
            " WHERE region_id=? ORDER BY slot_index",
            (region_id,),
        ).fetchall()
        return [
            {
                "seq": r["seq"],
                "slot_index": r["slot_index"],
                "group_id": r["group_id"],
                "block_id": r["block_id"],
                "data": bytes(r["data"]),
            }
            for r in rows
        ]

    # ------------------------------------------------------------------ 写路径
    def try_write(
        self,
        group_id: int,
        block_id: int,
        data: bytes,
        request_no: int | None = None,
    ) -> dict[str, Any]:
        """尝试在本组 active 区尾部追加。

        返回 ``{"status": "written", ...}``、``{"status": "replayed", ...}``
        或 ``{"status": "full", ...}``；若该组没有 active 区（搬迁中）抛
        :class:`NoActiveRegion`，带号请求冲突抛 :class:`RequestConflict`。
        """
        self._check_data(data)
        c = self._conn
        c.execute("BEGIN IMMEDIATE")
        try:
            receipt_row = self._get_receipt_row(group_id)
            receipt = None
            if request_no is not None:
                receipt = self._check_request_no(
                    group_id, request_no, block_id, data, receipt_row
                )
                if receipt is not None:
                    self._rollback_safely()
                    return {"status": "replayed", "receipt": receipt}

            region_id = self._find_active(group_id)
            if region_id is None:
                # 若本组正在搬迁，其源区已冻结、新区未转正：没有可写区。
                # unused 区是留给其他组的，绝不能挪用，否则破坏
                # “一组一区、一个 spare”的布局不变量。
                m = c.execute(
                    "SELECT 1 FROM migration WHERE id=1 AND group_id=?",
                    (group_id,),
                ).fetchone()
                if m is not None:
                    self._rollback_safely()
                    raise NoActiveRegion(f"组 {group_id} 搬迁进行中，暂无 active 区")
                unused = c.execute(
                    "SELECT region_id FROM regions WHERE status=? LIMIT 1",
                    (ST_UNUSED,),
                ).fetchone()
                if unused is None:
                    self._rollback_safely()
                    raise NoActiveRegion(f"组 {group_id} 暂无可写物理区")
                region_id = unused[0]
                c.execute(
                    "UPDATE regions SET status=?, group_id=? WHERE region_id=?",
                    (ST_ACTIVE, group_id, region_id),
                )
            used = c.execute(
                "SELECT COUNT(*) FROM records WHERE region_id=?", (region_id,)
            ).fetchone()[0]
            if used >= self._slots:
                self._rollback_safely()
                return {"status": "full", "region_id": region_id}
            meta = self._append(region_id, group_id, block_id, data)
            receipt = _receipt_payload(
                group_id=group_id,
                block_id=block_id,
                request_no=request_no,
                seq=meta["seq"],
                region_id=region_id,
                slot_index=meta["slot_index"],
                status="written",
                round_id=None,
                data=data,
            )
            if request_no is not None:
                self._save_receipt(group_id, request_no, block_id, data, receipt)
            c.execute("COMMIT")
            return {"status": "written", **meta, "receipt": receipt}
        except Exception:
            self._rollback_safely()
            raise

    # ------------------------------------------------------------------ 搬迁
    def get_migration(self) -> dict[str, Any] | None:
        row = self._conn.execute("SELECT * FROM migration WHERE id=1").fetchone()
        if row is None:
            return None
        dst_used = self._conn.execute(
            "SELECT COUNT(*) FROM records WHERE region_id=?", (row["dst_region_id"],)
        ).fetchone()[0]
        src_blocks = self._conn.execute(
            "SELECT COUNT(DISTINCT block_id) FROM records WHERE region_id=?",
            (row["src_region_id"],),
        ).fetchone()[0]
        return {
            "round_id": row["round_id"],
            "src_region_id": row["src_region_id"],
            "dst_region_id": row["dst_region_id"],
            "group_id": row["group_id"],
            "request_no": row["request_no"],
            "pending_block_id": row["pending_block_id"],
            "pending_applied": bool(row["pending_applied"]),
            "last_copied_block_id": row["last_copied_block_id"],
            "dst_used_slots": dst_used,
            "src_distinct_blocks": src_blocks,
        }

    def start_migration(
        self,
        group_id: int,
        block_id: int,
        data: bytes,
        request_no: int | None = None,
    ) -> dict[str, Any]:
        """把 spare 区切成 migrating 目标区并登记触发写入；

        本组 active 区冻结为 migrating_src（只读），pending 新值由
        下一步 :meth:`migration_step` 落盘到目标区。

        带号请求在此做完整冲突校验；若该号正好命中一个**已存在**且
        同块同内容的轮次（例如停机后重发），返回 ``{"replayed": True,
        "round_id": ...}``，由调用方推进该轮而不是另起新轮。
        """
        self._check_data(data)
        c = self._conn
        c.execute("BEGIN IMMEDIATE")
        try:
            receipt_row = self._get_receipt_row(group_id)
            if request_no is not None:
                replay = self._check_request_no(
                    group_id, request_no, block_id, data, receipt_row
                )
                if replay is not None:
                    self._rollback_safely()
                    return {"status": "replayed", "receipt": replay}
            migration_active = c.execute(
                "SELECT * FROM migration WHERE id=1"
            ).fetchone()
            if migration_active is not None:
                if migration_active["group_id"] == group_id:
                    if request_no is not None:
                        same = (
                            migration_active["request_no"] == request_no
                            and migration_active["pending_block_id"] == block_id
                            and bytes(migration_active["pending_data"]) == bytes(data)
                        )
                        if same:
                            self._rollback_safely()
                            return {
                                "status": "replayed",
                                "round_id": migration_active["round_id"],
                                "in_progress": True,
                            }
                        self._rollback_safely()
                        raise RequestConflict(
                            f"组 {group_id} 已有一轮请求号 "
                            f"{migration_active['request_no']} 的搬迁进行中，"
                            f"不能以请求号 {request_no} 另起新一轮"
                        )
                    self._rollback_safely()
                    raise NoActiveRegion(f"组 {group_id} 搬迁进行中，暂无 active 区")
                # 其他组占用着 spare：与旧行为一致，报“无可用 spare”
                self._rollback_safely()
                raise NoSpare("spare 区已被占用，无法启动搬迁")
            src = self._find_active(group_id)
            if src is None:
                self._rollback_safely()
                raise NoActiveRegion(f"组 {group_id} 没有可写的 active 区")
            spare = c.execute(
                "SELECT region_id FROM regions WHERE status=?", (ST_SPARE,)
            ).fetchone()
            if spare is None:
                self._rollback_safely()
                raise NoSpare("spare 区已被占用，无法启动搬迁")
            dst = spare[0]
            # spare 区必须是空区（整区重置后才会回到 spare）
            assert c.execute(
                "SELECT COUNT(*) FROM records WHERE region_id=?", (dst,)
            ).fetchone()[0] == 0
            # 源区冻结为只读搬迁源，新区接管写入（pending 第一步即落盘）
            c.execute(
                "UPDATE regions SET status=? WHERE region_id=?",
                (ST_MIGRATING_SRC, src),
            )
            c.execute(
                "UPDATE regions SET status=?, group_id=? WHERE region_id=?",
                (ST_MIGRATING, group_id, dst),
            )
            round_id = uuid.uuid4().hex
            c.execute(
                "INSERT INTO migration(id, round_id, src_region_id, dst_region_id,"
                " group_id, request_no, pending_block_id, pending_data,"
                " pending_applied)"
                " VALUES (1, ?, ?, ?, ?, ?, ?, ?, 0)",
                (
                    round_id,
                    src,
                    dst,
                    group_id,
                    request_no,
                    block_id,
                    sqlite3.Binary(bytes(data)),
                ),
            )
            c.execute("COMMIT")
            return {
                "status": "started",
                "round_id": round_id,
                "src_region_id": src,
                "dst_region_id": dst,
                "group_id": group_id,
                "request_no": request_no,
                "pending_block_id": block_id,
            }
        except Exception:
            self._rollback_safely()
            raise

    def migration_step(self, round_id: str | None = None) -> dict[str, Any]:
        """推进一步搬迁，每步一个事务，返回步骤描述；done 表示搬迁完成。

        步骤顺序（天然幂等，崩溃重放不会产生重复槽位）：
        1. pending：触发覆盖的新值先落盘到新区，立刻可读；
        2. copied：把源区一个逻辑块的最新版本追加到新区；
        3. done：源区整区重置（释放全部槽位），翻转 active/spare；
           若本轮带请求号，在同一事务里用 pending 回执替换旧的完成回执。

        ``round_id`` 非空时只推进该轮；轮次已被切走则抛 NoMigration。
        """
        c = self._conn
        c.execute("BEGIN IMMEDIATE")
        try:
            m = c.execute("SELECT * FROM migration WHERE id=1").fetchone()
            if m is None:
                self._rollback_safely()
                raise NoMigration("没有进行中的搬迁")
            if round_id is not None and m["round_id"] != round_id:
                self._rollback_safely()
                raise NoMigration(f"轮次 {round_id} 已不再是当前搬迁")
            src, dst = m["src_region_id"], m["dst_region_id"]
            group_id, pending_block = m["group_id"], m["pending_block_id"]

            # 步骤 1：触发写入的新值优先落盘
            if not m["pending_applied"]:
                exists = c.execute(
                    "SELECT seq, slot_index FROM records"
                    " WHERE region_id=? AND group_id=? AND block_id=?"
                    " ORDER BY seq DESC LIMIT 1",
                    (dst, group_id, pending_block),
                ).fetchone()
                if exists is None:
                    meta = self._append(dst, group_id, pending_block, m["pending_data"])
                    seq, slot_index = meta["seq"], meta["slot_index"]
                else:
                    # 上一轮在 append 后、更新 pending_applied 前崩溃
                    seq, slot_index = exists["seq"], exists["slot_index"]
                c.execute(
                    "UPDATE migration SET pending_applied=1,"
                    " pending_slot_index=?, pending_seq=? WHERE id=1",
                    (slot_index, seq),
                )
                c.execute("COMMIT")
                return {
                    "kind": "pending",
                    "round_id": m["round_id"],
                    "group_id": group_id,
                    "block_id": pending_block,
                    "src_region_id": src,
                    "dst_region_id": dst,
                    "seq": seq,
                    "slot_index": slot_index,
                }

            # 步骤 2：拷贝源区里尚未出现在新区的下一个逻辑块（按块号升序，确定性）
            nxt = c.execute(
                """
                SELECT DISTINCT block_id FROM records
                 WHERE region_id=?
                   AND block_id NOT IN (
                       SELECT block_id FROM records
                        WHERE region_id=? AND group_id=?
                   )
                 ORDER BY block_id LIMIT 1
                """,
                (src, dst, group_id),
            ).fetchone()
            if nxt is not None:
                block_id = nxt[0]
                latest = c.execute(
                    "SELECT data FROM records WHERE region_id=? AND block_id=?"
                    " ORDER BY seq DESC LIMIT 1",
                    (src, block_id),
                ).fetchone()
                meta = self._append(dst, group_id, block_id, latest["data"])
                c.execute(
                    "UPDATE migration SET last_copied_block_id=? WHERE id=1",
                    (block_id,),
                )
                c.execute("COMMIT")
                return {
                    "kind": "copied",
                    "round_id": m["round_id"],
                    "group_id": group_id,
                    "block_id": block_id,
                    "src_region_id": src,
                    "dst_region_id": dst,
                    **meta,
                }

            # 步骤 3：源区整区重置；新区转正，源区变 spare
            data = bytes(m["pending_data"])
            request_no = m["request_no"]
            if m["pending_slot_index"] is None or m["pending_seq"] is None:
                row = c.execute(
                    "SELECT seq, slot_index FROM records"
                    " WHERE region_id=? AND group_id=? AND block_id=?"
                    " ORDER BY seq DESC LIMIT 1",
                    (dst, group_id, pending_block),
                ).fetchone()
                pending_seq = row["seq"]
                pending_slot = row["slot_index"]
            else:
                pending_seq = m["pending_seq"]
                pending_slot = m["pending_slot_index"]
            meta = {
                "seq": pending_seq,
                "region_id": dst,
                "slot_index": pending_slot,
            }
            receipt = _receipt_payload(
                group_id=group_id,
                block_id=pending_block,
                request_no=request_no,
                seq=pending_seq,
                region_id=dst,
                slot_index=pending_slot,
                status="migrated",
                round_id=m["round_id"],
                data=data,
            )
            receipt["last_step"] = {
                "kind": "done",
                "round_id": m["round_id"],
                "group_id": group_id,
                "block_id": pending_block,
                "src_region_id": src,
                "dst_region_id": dst,
                "seq": pending_seq,
                "slot_index": pending_slot,
            }
            c.execute(
                "UPDATE regions SET status=?, group_id=NULL WHERE region_id=?",
                (ST_RESET, src),
            )
            c.execute("DELETE FROM records WHERE region_id=?", (src,))
            c.execute(
                "UPDATE regions SET status=? WHERE region_id=?", (ST_SPARE, src)
            )
            c.execute(
                "UPDATE regions SET status=? WHERE region_id=?", (ST_ACTIVE, dst)
            )
            if request_no is not None:
                self._save_receipt(group_id, request_no, pending_block, data, receipt)
            c.execute("DELETE FROM migration WHERE id=1")
            c.execute("COMMIT")
            return {
                "kind": "done",
                "round_id": m["round_id"],
                "group_id": group_id,
                "block_id": pending_block,
                "src_region_id": src,
                "dst_region_id": dst,
                "write": receipt,
                **meta,
            }
        except Exception:
            self._rollback_safely()
            raise

    def begin_takeover(
        self, group_id: int, request_no: int, block_id: int, data: bytes
    ) -> dict[str, Any] | None:
        """为“带号覆盖 → 满区”寻找可接手的轮次。

        返回 ``None`` 表示无需接手（无进行中的本组搬迁）；
        返回迁移状态 dict 表示应推进该轮。冲突（号不匹配 / 内容不同）抛
        :class:`RequestConflict`。
        """
        self._check_data(data)
        c = self._conn
        c.execute("BEGIN IMMEDIATE")
        try:
            m = c.execute("SELECT * FROM migration WHERE id=1").fetchone()
            if m is None or m["group_id"] != group_id:
                self._rollback_safely()
                return None
            if m["request_no"] == request_no:
                if (
                    m["pending_block_id"] == block_id
                    and bytes(m["pending_data"]) == bytes(data)
                ):
                    self._rollback_safely()
                    return {
                        "round_id": m["round_id"],
                        "group_id": m["group_id"],
                        "request_no": m["request_no"],
                        "pending_block_id": m["pending_block_id"],
                    }
                self._rollback_safely()
                raise RequestConflict(
                    f"请求号 {request_no} 已绑定进行中的轮次，"
                    f"块 {m['pending_block_id']} 与本次块 {block_id} / 内容不一致"
                )
            if m["request_no"] is not None and request_no < m["request_no"]:
                self._rollback_safely()
                raise RequestConflict(
                    f"请求号 {request_no} 小于进行中轮次的请求号 "
                    f"{m['request_no']}"
                )
            self._rollback_safely()
            return None
        except Exception:
            self._rollback_safely()
            raise

    def recover(self) -> dict[str, Any] | None:
        """重启恢复。

        正常进行中的搬迁保持原状（可 resume 续跑）；需要补动作的是
        finalize 事务中途停机：源区记录已删（可能恰好为零条，也可能还没
        来得及删完），状态还没翻转完。判断依据是 dst 是否仍处于 migrating。
        """
        m = self.get_migration()
        if m is None:
            return None
        dst_status = self._conn.execute(
            "SELECT status FROM regions WHERE region_id=?", (m["dst_region_id"],)
        ).fetchone()[0]
        if dst_status != ST_MIGRATING:
            # dst 已转正，说明 finalize 的状态翻转已落盘，仅需清理残留行
            c = self._conn
            c.execute("BEGIN IMMEDIATE")
            try:
                c.execute("DELETE FROM migration WHERE id=1")
                c.execute("COMMIT")
            except Exception:
                self._rollback_safely()
                raise
            return {"status": "finalized", **m}
        src_remaining = self._conn.execute(
            "SELECT COUNT(*) FROM records WHERE region_id=?", (m["src_region_id"],)
        ).fetchone()[0]
        if src_remaining > 0:
            return {"status": "interrupted", **m}
        c = self._conn
        c.execute("BEGIN IMMEDIATE")
        try:
            c.execute(
                "UPDATE regions SET status=? WHERE region_id=?",
                (ST_ACTIVE, m["dst_region_id"]),
            )
            c.execute(
                "UPDATE regions SET status=?, group_id=NULL WHERE region_id=?",
                (ST_SPARE, m["src_region_id"]),
            )
            # 若本轮带请求号，补登完成回执（正常路径在 finalize 同事务写入）
            if m["request_no"] is not None:
                row = c.execute(
                    "SELECT seq, slot_index FROM records"
                    " WHERE region_id=? AND group_id=? AND block_id=?"
                    " ORDER BY seq DESC LIMIT 1",
                    (m["dst_region_id"], m["group_id"], m["pending_block_id"]),
                ).fetchone()
                if row is not None:
                    data = bytes(m["pending_data"])
                    receipt = _receipt_payload(
                        group_id=m["group_id"],
                        block_id=m["pending_block_id"],
                        request_no=m["request_no"],
                        seq=row["seq"],
                        region_id=m["dst_region_id"],
                        slot_index=row["slot_index"],
                        status="migrated",
                        round_id=m["round_id"],
                        data=data,
                    )
                    receipt["last_step"] = {
                        "kind": "done",
                        "round_id": m["round_id"],
                        "group_id": m["group_id"],
                        "block_id": m["pending_block_id"],
                        "src_region_id": m["src_region_id"],
                        "dst_region_id": m["dst_region_id"],
                        "seq": row["seq"],
                        "slot_index": row["slot_index"],
                    }
                    self._save_receipt(
                        m["group_id"],
                        m["request_no"],
                        m["pending_block_id"],
                        data,
                        receipt,
                    )
            c.execute("DELETE FROM migration WHERE id=1")
            c.execute("COMMIT")
            return {"status": "finalized", **m}
        except Exception:
            self._rollback_safely()
            raise

    # ------------------------------------------------------------------ 管理
    def admin_reset_region(self, region_id: int) -> None:
        """演练用：整区重置并回收为 spare（搬迁进行中禁止）。"""
        c = self._conn
        c.execute("BEGIN IMMEDIATE")
        try:
            if c.execute("SELECT 1 FROM migration WHERE id=1").fetchone() is not None:
                self._rollback_safely()
                raise StoreError("搬迁进行中，不能重置物理区")
            exists = c.execute(
                "SELECT 1 FROM regions WHERE region_id=?", (region_id,)
            ).fetchone()
            if exists is None:
                self._rollback_safely()
                raise StoreError(f"物理区 {region_id} 不存在")
            c.execute("DELETE FROM records WHERE region_id=?", (region_id,))
            c.execute(
                "UPDATE regions SET status=?, group_id=NULL WHERE region_id=?",
                (ST_SPARE, region_id),
            )
            c.execute("COMMIT")
        except Exception:
            self._rollback_safely()
            raise

    def admin_reset_all(self) -> None:
        """演练用：清空全部数据，恢复初始四区布局。"""
        c = self._conn
        c.execute("BEGIN IMMEDIATE")
        try:
            c.execute("DELETE FROM records")
            c.execute("DELETE FROM migration")
            c.execute("DELETE FROM group_receipts")
            c.execute("UPDATE regions SET status=?, group_id=NULL", (ST_UNUSED,))
            c.execute(
                "UPDATE regions SET status=? WHERE region_id=0", (ST_SPARE,)
            )
            c.execute("COMMIT")
        except Exception:
            self._rollback_safely()
            raise
