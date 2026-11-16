"""pytest 夹具：隔离的数据目录 + 直接 ASGI 调用的 httpx 客户端。"""

from __future__ import annotations

import datetime as dt
import hashlib
import os
import pathlib
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from app import main as main_mod
from app.config import Settings, load_settings
from app.db import Database
from app.storage import ensure_dirs
from tests.asgi_client import SyncASGIClient


@pytest.fixture()
def data_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    d = tmp_path / "data"
    d.mkdir()
    return d


@pytest.fixture()
def settings(data_dir: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    # 每个测试独立 DB 文件，避免上一个测试后台线程延迟关闭连接时
    # 触碰下一个测试的数据库文件
    monkeypatch.setenv("OBS_DATA_DIR", str(data_dir))
    monkeypatch.setenv("OBS_QUOTA_BYTES", str(64 * 1024 * 1024))
    monkeypatch.setenv("OBS_INSTANCE_ID", "test-instance")
    monkeypatch.setenv("OBS_RECONCILE_INTERVAL", "3600")
    base = load_settings()
    return Settings(
        data_dir=base.data_dir,
        db_path=os.path.join(str(data_dir), f"obs-{uuid.uuid4().hex}.db"),
        staging_dir=base.staging_dir,
        blob_dir=base.blob_dir,
        lock_dir=base.lock_dir,
        quota_bytes=base.quota_bytes,
        instance_id=base.instance_id,
        reconcile_interval=base.reconcile_interval,
        chunk_size=base.chunk_size,
    )


@pytest.fixture()
def database(settings: Settings):
    ensure_dirs(settings)
    db = Database(settings.db_path)
    yield db
    db.close()


@pytest.fixture()
def client(settings: Settings):
    """不走 lifespan，直接装配 app 全局状态，避免事件循环嵌套。"""
    from app.service import shutdown_io_pool

    ensure_dirs(settings)
    main_mod.settings = settings
    main_mod.db = Database(settings.db_path)
    c = SyncASGIClient(main_mod.app)
    try:
        yield c
    finally:
        # 先排空 I/O 线程池（其内 sqlite 连接随之关闭），再关事件循环与 DB
        shutdown_io_pool()
        c.close()
        main_mod.db.close()


def make_parquet(
    path: str | pathlib.Path,
    *,
    station_id: str = "BUOY-001",
    date: dt.date = dt.date(2026, 11, 15),
    rows: int = 24,
    salt: float = 0.0,
    station_override: str | None = None,
    day_offset: int = 0,
    tz: str | None = "UTC",
    string_ts: bool = False,
) -> bytes:
    """生成一个合法（或可定制异常）的观测 Parquet，返回文件字节。"""
    timestamps = [
        dt.datetime(date.year, date.month, date.day, h, 0, 0, tzinfo=dt.timezone.utc)
        + dt.timedelta(days=day_offset)
        for h in range(rows)
    ]
    if string_ts:
        ts = pa.array([t.strftime("%Y-%m-%dT%H:%M:%SZ") for t in timestamps], pa.string())
    elif tz is None:
        ts = pa.array([t.replace(tzinfo=None) for t in timestamps], pa.timestamp("us"))
    else:
        ts = pa.array(timestamps, pa.timestamp("us", tz))
    table = pa.table(
        {
            "station_id": pa.array([station_override or station_id] * rows, pa.string()),
            "timestamp_utc": ts,
            "salinity": pa.array([35.0 + salt + h * 0.01 for h in range(rows)], pa.float64()),
        }
    )
    pq.write_table(table, str(path))
    return pathlib.Path(path).read_bytes()


def sha256_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def declare_payload(receipt_id: str, data: bytes, **kw) -> dict:
    payload = {
        "receipt_id": receipt_id,
        "station_id": kw.pop("station_id", "BUOY-001"),
        "obs_date": kw.pop("obs_date", "2026-11-15"),
        "size": len(data),
        "sha256": sha256_of(data),
    }
    payload.update(kw)
    return payload


@pytest.fixture()
def helpers():
    class H:
        make_parquet = staticmethod(make_parquet)
        sha256_of = staticmethod(sha256_of)
        declare_payload = staticmethod(declare_payload)

    return H()
