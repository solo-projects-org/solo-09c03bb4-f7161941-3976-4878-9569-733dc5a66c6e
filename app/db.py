"""SQLite 访问层。

双实例（多进程）共享同一个数据库文件：
- WAL 模式 + NORMAL 同步，保证多进程一写多读不互斥阻塞；
- 所有写事务使用 BEGIN IMMEDIATE，配合 busy_timeout 将写串行化，
  避免 SQLITE_BUSY 直接打到请求上；
- 每个进程一个连接（uvicorn worker 内的多线程通过 RLock 串行使用）。

Schema 演进：
- v1：receipts(UPLOADING/PUBLISHED) + blobs(sha,size,ref_count)
- v2：receipts 增加 QUARANTINED 状态（原件字节核验失败，不可供数/待补传）；
      blobs 增加隔离标记、原因与核验缓存（last_size/last_mtime_ns/last_verified_at）。
"""

from __future__ import annotations

import os
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager

SCHEMA_VERSION = 3

_SCHEMA = """
CREATE TABLE IF NOT EXISTS receipts (
    receipt_id    TEXT PRIMARY KEY,
    station_id    TEXT NOT NULL,
    obs_date      TEXT NOT NULL,
    declared_size INTEGER NOT NULL CHECK (declared_size >= 0),
    declared_sha256 TEXT NOT NULL,
    state         TEXT NOT NULL CHECK (state IN ('UPLOADING', 'PUBLISHED', 'QUARANTINED')),
    bytes_received INTEGER NOT NULL DEFAULT 0,
    actual_sha256  TEXT,
    blob_sha256    TEXT,
    row_count      INTEGER,
    last_error     TEXT,
    publishing     INTEGER NOT NULL DEFAULT 0 CHECK (publishing IN (0, 1)),
    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_receipts_state      ON receipts(state);
CREATE INDEX IF NOT EXISTS idx_receipts_station_day ON receipts(station_id, obs_date);

-- 内容寻址成品：sha256 唯一；ref_count 是引用它的接收数（PUBLISHED + QUARANTINED）。
-- quarantined=1 表示磁盘字节与登记摘要/长度不符，原件隔离、关联接收不可供数，
-- 但文件与额度保留，等待任一关联接收补传正确原包后原子替换并恢复。
CREATE TABLE IF NOT EXISTS blobs (
    sha256     TEXT PRIMARY KEY,
    size       INTEGER NOT NULL CHECK (size >= 0),
    ref_count  INTEGER NOT NULL DEFAULT 0 CHECK (ref_count >= 0),
    quarantined INTEGER NOT NULL DEFAULT 0 CHECK (quarantined IN (0, 1)),
    quarantine_reason TEXT,
    last_size  INTEGER,
    last_mtime_ns INTEGER,
    last_verified_at TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

# v1 -> v2 迁移：receipts 的 state CHECK 需要放宽，SQLite 只能重建表
_RECEIPTS_V2_DDL = """
CREATE TABLE receipts_new (
    receipt_id    TEXT PRIMARY KEY,
    station_id    TEXT NOT NULL,
    obs_date      TEXT NOT NULL,
    declared_size INTEGER NOT NULL CHECK (declared_size >= 0),
    declared_sha256 TEXT NOT NULL,
    state         TEXT NOT NULL CHECK (state IN ('UPLOADING', 'PUBLISHED', 'QUARANTINED')),
    bytes_received INTEGER NOT NULL DEFAULT 0,
    actual_sha256  TEXT,
    blob_sha256    TEXT,
    row_count      INTEGER,
    last_error     TEXT,
    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
)
"""


class Database:
    """进程级单例式连接封装。"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._lock = threading.RLock()
        self._conn = self._connect()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            f"file:{self.db_path}?uri=true",
            uri=True,
            check_same_thread=False,
            isolation_level=None,  # 事务完全由代码显式管理
            timeout=30.0,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(_SCHEMA)
            row = self._conn.execute(
                "SELECT value FROM schema_meta WHERE key = 'schema_version'"
            ).fetchone()
            if row is None:
                # 全新库：_SCHEMA 已建成最新结构
                self._conn.execute(
                    "INSERT INTO schema_meta(key, value) VALUES ('schema_version', ?)",
                    (str(SCHEMA_VERSION),),
                )
                return
            version = int(row["value"])
            if version == SCHEMA_VERSION:
                return
            if version > SCHEMA_VERSION:  # pragma: no cover - 降级保护
                raise RuntimeError(
                    f"数据库 schema 版本 {version} 新于程序 {SCHEMA_VERSION}，拒绝启动"
                )
            if version == 1:
                self._migrate_1_to_2(self._conn)
                version = 2
            if version == 2:
                self._migrate_2_to_3(self._conn)
                version = 3
            if version != SCHEMA_VERSION:  # pragma: no cover - 后续版本预留
                raise RuntimeError(f"未实现的 schema 迁移：{version} -> {SCHEMA_VERSION}")

    @staticmethod
    def _migrate_2_to_3(conn: sqlite3.Connection) -> None:
        """v2 -> v3：receipts 增加 publishing 发布临界标记列。"""
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(receipts)").fetchall()}
        if "publishing" not in cols:
            conn.execute(
                "ALTER TABLE receipts ADD COLUMN publishing "
                "INTEGER NOT NULL DEFAULT 0"
            )
        conn.execute(
            "UPDATE schema_meta SET value = '3' WHERE key = 'schema_version'"
        )

    @staticmethod
    def _migrate_1_to_2(conn: sqlite3.Connection) -> None:
        """v1 -> v2：blobs 增补隔离/核验列；receipts 重建以放宽 state CHECK。

        注意：DDL 在 SQLite 中无法随 DML 回滚，这里不使用显式 BEGIN；
        各语句通过 autocommit 连接顺序执行，失败即拒绝启动（库仍停留在 v1，
        修复后重启可重跑——ALTER 采用 IF NOT EXISTS 语义的列存在性判断）。
        """
        blob_cols = {r["name"] for r in conn.execute("PRAGMA table_info(blobs)").fetchall()}
        add_columns = (
            ("quarantined", "ALTER TABLE blobs ADD COLUMN quarantined "
                            "INTEGER NOT NULL DEFAULT 0"),
            ("quarantine_reason", "ALTER TABLE blobs ADD COLUMN quarantine_reason TEXT"),
            ("last_size", "ALTER TABLE blobs ADD COLUMN last_size INTEGER"),
            ("last_mtime_ns", "ALTER TABLE blobs ADD COLUMN last_mtime_ns INTEGER"),
            ("last_verified_at", "ALTER TABLE blobs ADD COLUMN last_verified_at TEXT"),
        )
        for name, ddl in add_columns:
            if name not in blob_cols:
                conn.execute(ddl)

        conn.execute(_RECEIPTS_V2_DDL)
        conn.execute(
            "INSERT INTO receipts_new(receipt_id, station_id, obs_date, declared_size, "
            "declared_sha256, state, bytes_received, actual_sha256, blob_sha256, "
            "row_count, last_error, created_at, updated_at) "
            "SELECT receipt_id, station_id, obs_date, declared_size, "
            "declared_sha256, state, bytes_received, actual_sha256, blob_sha256, "
            "row_count, last_error, created_at, updated_at FROM receipts"
        )
        conn.execute("DROP TABLE receipts")
        conn.execute("ALTER TABLE receipts_new RENAME TO receipts")
        conn.execute("CREATE INDEX idx_receipts_state ON receipts(state)")
        conn.execute("CREATE INDEX idx_receipts_station_day ON receipts(station_id, obs_date)")
        conn.execute(
            "UPDATE schema_meta SET value = '2' WHERE key = 'schema_version'"
        )

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        """串行化写事务上下文。

        immediate=True 使用 BEGIN IMMEDIATE，进入即取写锁，
        配合 busy_timeout 实现跨进程写串行化。
        """
        with self._lock:
            if immediate:
                self._conn.execute("BEGIN IMMEDIATE")
            else:
                self._conn.execute("BEGIN")
            try:
                yield self._conn
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def query_one(self, sql: str, params: tuple = ()) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(sql, params or ()).fetchone()

    def query_all(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self._lock:
            return list(self._conn.execute(sql, params or ()).fetchall())

    def close(self) -> None:
        with self._lock:
            self._conn.close()
