"""SQLite 访问层。

双实例（多进程）共享同一个数据库文件：
- WAL 模式 + NORMAL 同步，保证多进程一写多读不互斥阻塞；
- 所有写事务使用 BEGIN IMMEDIATE，配合 busy_timeout 将写串行化，
  避免 SQLITE_BUSY 直接打到请求上；
- 每个进程一个连接（uvicorn worker 内的多线程通过 RLock 串行使用）。
"""

from __future__ import annotations

import os
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager

SCHEMA_VERSION = 1

_SCHEMA = """
CREATE TABLE IF NOT EXISTS receipts (
    receipt_id    TEXT PRIMARY KEY,
    station_id    TEXT NOT NULL,
    obs_date      TEXT NOT NULL,
    declared_size INTEGER NOT NULL CHECK (declared_size >= 0),
    declared_sha256 TEXT NOT NULL,
    state         TEXT NOT NULL CHECK (state IN ('UPLOADING', 'PUBLISHED')),
    bytes_received INTEGER NOT NULL DEFAULT 0,
    actual_sha256  TEXT,
    blob_sha256    TEXT,
    row_count      INTEGER,
    last_error     TEXT,
    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_receipts_state      ON receipts(state);
CREATE INDEX IF NOT EXISTS idx_receipts_station_day ON receipts(station_id, obs_date);

-- 内容寻址成品：sha256 唯一；ref_count 是引用它的已发布接收数
CREATE TABLE IF NOT EXISTS blobs (
    sha256     TEXT PRIMARY KEY,
    size       INTEGER NOT NULL CHECK (size >= 0),
    ref_count  INTEGER NOT NULL DEFAULT 0 CHECK (ref_count >= 0),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
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
                self._conn.execute(
                    "INSERT INTO schema_meta(key, value) VALUES ('schema_version', ?)",
                    (str(SCHEMA_VERSION),),
                )
            elif int(row["value"]) != SCHEMA_VERSION:  # pragma: no cover - 迁移预留
                raise RuntimeError(
                    f"数据库 schema 版本 {row['value']} 与程序 {SCHEMA_VERSION} 不兼容"
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
