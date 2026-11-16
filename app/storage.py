"""文件布局、内容寻址存储、文件锁与崩溃回收。

目录结构（双实例共享同一个数据卷）：
  data/
    obs.db                  SQLite（WAL 模式，多进程共享）
    staging/<receipt>.part  未完成接收的暂存字节，按接收号命名，解析归属
    blobs/ab/<sha256>       成品原包，内容寻址，同内容跨接收只存一份
    locks/rcpt-<id>.lock    接收级互斥锁（同一接收同时只能有一个上传者）
    locks/global.lock       发布/对账全局锁，串行化跨进程回收动作

配额口径：已使用字节 = 全部成品 blob 大小之和 + 全部暂存文件实际大小之和。
声明时先按声明长度预留暂存额度；发布时若内容已存在则释放预留、不重复占额。
"""

from __future__ import annotations

import errno
import fcntl
import os
from contextlib import contextmanager
from typing import Iterator

from .config import Settings
from .db import Database
from .errors import ObsError, ReceiptBusy


class FileLock:
    """基于 flock 的跨进程互斥锁；进程崩溃时内核自动释放。"""

    def __init__(self, path: str):
        self.path = path
        self._fh: int | None = None

    def acquire(self, *, blocking: bool = True) -> bool:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        fh = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        flags = fcntl.LOCK_EX if blocking else fcntl.LOCK_EX | fcntl.LOCK_NB
        try:
            fcntl.flock(fh, flags)
        except OSError as exc:
            os.close(fh)
            if exc.errno in (errno.EACCES, errno.EAGAIN):
                return False
            raise
        self._fh = fh
        return True

    def release(self) -> None:
        if self._fh is None:
            return
        try:
            fcntl.flock(self._fh, fcntl.LOCK_UN)
        finally:
            os.close(self._fh)
            self._fh = None

    def __enter__(self) -> "FileLock":
        if not self.acquire(blocking=True):  # pragma: no cover - 阻塞模式不会失败
            raise ReceiptBusy("获取文件锁失败")
        return self

    def __exit__(self, *exc: object) -> None:
        self.release()


@contextmanager
def try_receipt_lock(settings: Settings, receipt_id: str) -> Iterator[FileLock]:
    """非阻塞获取接收锁，拿不到抛 ReceiptBusy。"""
    lock = FileLock(os.path.join(settings.lock_dir, f"rcpt-{receipt_id}.lock"))
    if not lock.acquire(blocking=False):
        raise ReceiptBusy(f"接收 {receipt_id} 正在被另一个上传会话占用，请稍后重试")
    try:
        yield lock
    finally:
        lock.release()


@contextmanager
def global_lock(settings: Settings) -> Iterator[None]:
    """全局锁：发布与对账互斥，保证跨实例回收不会误删在途/已发布文件。"""
    with FileLock(os.path.join(settings.lock_dir, "global.lock")):
        yield


def blob_path(settings: Settings, sha256: str) -> str:
    return os.path.join(settings.blob_dir, sha256[:2], sha256)


def staging_path(settings: Settings, receipt_id: str) -> str:
    return os.path.join(settings.staging_dir, f"{receipt_id}.part")


def ensure_dirs(settings: Settings) -> None:
    for d in (settings.staging_dir, settings.blob_dir, settings.lock_dir):
        os.makedirs(d, exist_ok=True)
    # blob 的 256 个前缀子目录在发布时按需创建，避免启动即铺大量空目录


def safe_remove(path: str) -> bool:
    """删除文件；不存在视为成功，绝不抛 FileNotFoundError。"""
    try:
        os.remove(path)
        return True
    except FileNotFoundError:
        return False


def hardlink_or_copy(src: str, dst: str) -> None:
    """原子发布辅助：优先硬链接（同卷秒级完成且不额外占额），失败则拷贝。

    目标路径先写入 .tmp 再 rename，避免崩溃留下半个成品。
    """
    tmp = f"{dst}.tmp.{os.getpid()}"
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    try:
        os.link(src, tmp)
    except OSError:
        with open(src, "rb") as fin, open(tmp, "wb") as fout:
            while True:
                chunk = fin.read(8 * 1024 * 1024)
                if not chunk:
                    break
                fout.write(chunk)
            fout.flush()
            os.fsync(fout.fileno())
    os.replace(tmp, dst)


def _physical_usage(settings: Settings) -> tuple[int, list[str], list[str]]:
    """盘点磁盘上的真实占用：返回(字节数, 暂存文件列表, blob 文件列表)。"""
    staging_files: list[str] = []
    staging_bytes = 0
    if os.path.isdir(settings.staging_dir):
        for name in os.listdir(settings.staging_dir):
            full = os.path.join(settings.staging_dir, name)
            if os.path.isfile(full):
                staging_files.append(name)
                staging_bytes += os.path.getsize(full)
    blob_files: list[str] = []
    blob_bytes = 0
    if os.path.isdir(settings.blob_dir):
        for prefix in os.listdir(settings.blob_dir):
            pdir = os.path.join(settings.blob_dir, prefix)
            if not os.path.isdir(pdir):
                continue
            for name in os.listdir(pdir):
                full = os.path.join(pdir, name)
                if not os.path.isfile(full):
                    continue
                if ".tmp." in name or name.endswith(".tmp"):
                    # 崩溃留下的半成品硬链接：直接清理，不计入 blob 盘点
                    safe_remove(full)
                    continue
                blob_files.append(name)
                blob_bytes += os.path.getsize(full)
    return staging_bytes + blob_bytes, staging_files, blob_files


def reconcile(settings: Settings, db: Database) -> dict[str, int]:
    """启动与周期性对账：在全局锁 + 写事务内修复所有崩溃残留。

    修复项：
    1. 暂存区里没有对应 UPLOADING 接收的文件（含其他实例崩溃残留）→ 删除；
    2. UPLOADING 接收缺失暂存文件（写盘/登记中断）→ 清零进度，接收可重传；
    3. 已发布接收缺成品文件 → 回退为 UPLOADING 并解除 blob 引用，
       绝不允许存在“成功记录但缺件”；
    4. 无任何接收引用的 blob → 删除并注销；
    5. 数据库与磁盘不一致的 blob 条目 → 按磁盘真相修正。
    """
    removed_staging = 0
    removed_blobs = 0
    reset_receipts = 0

    with global_lock(settings):
        _total, staging_names, blob_names = _physical_usage(settings)
        with db.transaction() as conn:
            uploading = {
                r["receipt_id"]: r
                for r in conn.execute(
                    "SELECT receipt_id, bytes_received FROM receipts WHERE state = 'UPLOADING'"
                ).fetchall()
            }
            published_missing: list[str] = []
            for r in conn.execute(
                "SELECT receipt_id, blob_sha256 FROM receipts WHERE state = 'PUBLISHED'"
            ).fetchall():
                if not r["blob_sha256"] or not os.path.isfile(
                    blob_path(settings, r["blob_sha256"])
                ):
                    published_missing.append(r["receipt_id"])

            # 1 & 2：暂存区对账
            live_staging = set()
            for name in staging_names:
                receipt_id = name[: -len(".part")] if name.endswith(".part") else None
                if receipt_id in uploading:
                    live_staging.add(receipt_id)
                else:
                    safe_remove(os.path.join(settings.staging_dir, name))
                    removed_staging += 1
            for receipt_id, row in uploading.items():
                if receipt_id not in live_staging and row["bytes_received"] > 0:
                    conn.execute(
                        "UPDATE receipts SET bytes_received = 0, actual_sha256 = NULL, "
                        "last_error = '检测到暂存文件缺失（上次会话中断），请重新上传', "
                        "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') "
                        "WHERE receipt_id = ?",
                        (receipt_id,),
                    )
                    reset_receipts += 1

            # 3：缺件的“已发布”记录回退为可重传
            for receipt_id in published_missing:
                row = conn.execute(
                    "SELECT blob_sha256 FROM receipts WHERE receipt_id = ?",
                    (receipt_id,),
                ).fetchone()
                sha = row["blob_sha256"] if row else None
                if sha:
                    conn.execute(
                        "UPDATE blobs SET ref_count = MAX(ref_count - 1, 0) WHERE sha256 = ?",
                        (sha,),
                    )
                conn.execute(
                    "UPDATE receipts SET state = 'UPLOADING', bytes_received = 0, "
                    "actual_sha256 = NULL, blob_sha256 = NULL, row_count = NULL, "
                    "last_error = '成品文件缺失，已自动回退为待上传，请重新上传', "
                    "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') "
                    "WHERE receipt_id = ?",
                    (receipt_id,),
                )
                reset_receipts += 1

            # 4 & 5：blob 对账（以磁盘为准）
            db_shas = {r["sha256"]: r for r in conn.execute("SELECT * FROM blobs").fetchall()}
            disk_shas = set(blob_names)
            for sha in disk_shas - db_shas.keys():
                conn.execute(
                    "INSERT OR IGNORE INTO blobs(sha256, size, ref_count) VALUES (?, ?, 0)",
                    (sha, os.path.getsize(blob_path(settings, sha))),
                )
            refs: dict[str, int] = {}
            for r in conn.execute(
                "SELECT blob_sha256, COUNT(*) AS n FROM receipts "
                "WHERE state = 'PUBLISHED' AND blob_sha256 IS NOT NULL GROUP BY blob_sha256"
            ).fetchall():
                refs[r["blob_sha256"]] = r["n"]
            for sha, row in list(db_shas.items()):
                if sha not in disk_shas:
                    conn.execute("DELETE FROM blobs WHERE sha256 = ?", (sha,))
                    continue
                real_refs = refs.get(sha, 0)
                real_size = os.path.getsize(blob_path(settings, sha))
                if row["ref_count"] != real_refs or row["size"] != real_size:
                    conn.execute(
                        "UPDATE blobs SET ref_count = ?, size = ? WHERE sha256 = ?",
                        (real_refs, real_size, sha),
                    )
            for r in conn.execute("SELECT sha256, ref_count FROM blobs").fetchall():
                sha = r["sha256"]
                if sha not in disk_shas:
                    continue
                if r["ref_count"] == 0:
                    safe_remove(blob_path(settings, sha))
                    conn.execute("DELETE FROM blobs WHERE sha256 = ?", (sha,))
                    removed_blobs += 1

    return {
        "removed_staging_files": removed_staging,
        "removed_orphan_blobs": removed_blobs,
        "reset_receipts": reset_receipts,
    }


def current_usage(settings: Settings, db: Database) -> dict[str, int]:
    """当前配额口径：成品 blob 实际字节 + 在途暂存实际字节。"""
    blob_bytes = db.query_one("SELECT COALESCE(SUM(size), 0) AS n FROM blobs")["n"]
    staging_bytes = 0
    if os.path.isdir(settings.staging_dir):
        for name in os.listdir(settings.staging_dir):
            full = os.path.join(settings.staging_dir, name)
            if os.path.isfile(full):
                staging_bytes += os.path.getsize(full)
    return {
        "quota_bytes": settings.quota_bytes,
        "used_bytes": int(blob_bytes) + staging_bytes,
        "blob_bytes": int(blob_bytes),
        "staging_bytes": staging_bytes,
        "available_bytes": settings.quota_bytes - int(blob_bytes) - staging_bytes,
    }


def assert_staging_fits(settings: Settings, db: Database, staging_bytes: int) -> None:
    """落盘暂存前的配额核算。

    口径：已发布成品 blob 占用 + 本次暂存占用。暂存文件按接收号命名、
    同一接收重传会截断重写，活动暂存互不叠加；声明长度本身已限制为
    不超过总额度，因此多个在途暂存的总和不会失控。这样“额度恰好等于
    包大小”的同内容重发不会被误拒（其成品去重、不产生新物理占用）。
    """
    blob_bytes = int(
        db.query_one("SELECT COALESCE(SUM(size), 0) AS n FROM blobs")["n"]
    )
    if blob_bytes + staging_bytes > settings.quota_bytes:
        raise ObsError(
            f"原包字节额度不足：本次暂存需要 {staging_bytes} 字节，"
            f"成品已占 {blob_bytes} / 额度 {settings.quota_bytes}，"
            f"剩余 {max(settings.quota_bytes - blob_bytes, 0)} 字节",
            code="quota_exceeded",
            status_code=507,
        )
