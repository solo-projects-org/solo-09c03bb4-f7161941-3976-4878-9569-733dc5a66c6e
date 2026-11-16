"""服务配置，全部通过环境变量注入，便于双实例共用同一镜像。"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    raw = _env(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:  # pragma: no cover - 配置错误直接失败
        raise RuntimeError(f"环境变量 {name} 必须是整数，实际为 {raw!r}") from exc
    if value <= 0:
        raise RuntimeError(f"环境变量 {name} 必须为正整数，实际为 {raw!r}")
    return value


@dataclass(frozen=True)
class Settings:
    # 数据根目录：SQLite、暂存区、成品 blob 全部放在这里，双实例挂载同一个卷
    data_dir: str
    db_path: str
    staging_dir: str
    blob_dir: str
    lock_dir: str

    # 原包字节总额度（暂存 + 成品共用），默认 1 GiB
    quota_bytes: int

    # 单实例实例标识，仅用于排查日志
    instance_id: str

    # 启动后周期性对账（回收崩溃残留）的间隔秒数
    reconcile_interval: int

    # 单次流式上传的分块大小
    chunk_size: int

    @property
    def db_url(self) -> str:
        return f"file:{self.db_path}?uri=true"


def load_settings() -> Settings:
    data_dir = os.path.abspath(_env("OBS_DATA_DIR", "/data"))
    return Settings(
        data_dir=data_dir,
        db_path=os.path.join(data_dir, "obs.db"),
        staging_dir=os.path.join(data_dir, "staging"),
        blob_dir=os.path.join(data_dir, "blobs"),
        lock_dir=os.path.join(data_dir, "locks"),
        quota_bytes=_env_int("OBS_QUOTA_BYTES", 1024 * 1024 * 1024),
        instance_id=_env("OBS_INSTANCE_ID", "instance-0"),
        reconcile_interval=_env_int("OBS_RECONCILE_INTERVAL", 30),
        chunk_size=_env_int("OBS_CHUNK_SIZE", 1024 * 1024),
    )
