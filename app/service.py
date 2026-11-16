"""接收流程编排：声明 → 上传 → 校验 → 发布，全部崩溃安全。"""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import os
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from .config import Settings
from .db import Database
from .errors import (
    AlreadyFinalized,
    ContentMismatch,
    DeclarationConflict,
    InvalidPackage,
    ObsError,
    ReceiptBusy,
    ReceiptNotFound,
    UploadNotComplete,
)
from .schemas import DeclarationIn
from .storage import (
    FileLock,
    assert_staging_fits,
    blob_path,
    current_usage,
    global_lock,
    hardlink_or_copy,
    safe_remove,
    staging_path,
    try_receipt_lock,
)
from .validation import validate_parquet

# 所有阻塞型 SQLite/文件/Parquet 操作共用的线程池，保证事件循环不被拖住
_io_pool: ThreadPoolExecutor | None = None


def _get_io_pool() -> ThreadPoolExecutor:
    global _io_pool
    if _io_pool is None:
        _io_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="obs-io")
    return _io_pool


def shutdown_io_pool() -> None:
    """关闭当前线程池；下次使用时自动重建（主要用于测试隔离与优雅停机）。"""
    global _io_pool
    if _io_pool is not None:
        _io_pool.shutdown(wait=True)
        _io_pool = None


async def _offload(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_get_io_pool(), partial(func, *args, **kwargs))

# 声明中允许的状态流转集合
_UPLOADING = "UPLOADING"
_PUBLISHED = "PUBLISHED"


def _row_to_dict(row: object) -> dict:
    return dict(row)  # type: ignore[arg-type]


def declare_receipt(settings: Settings, db: Database, decl: DeclarationIn) -> dict:
    """按接收号声明；同号同声明幂等，异声明 409。"""
    with db.transaction() as conn:
        row = conn.execute(
            "SELECT * FROM receipts WHERE receipt_id = ?", (decl.receipt_id,)
        ).fetchone()
        if row is not None:
            if (
                row["station_id"] != decl.station_id
                or row["obs_date"] != decl.obs_date.isoformat()
                or row["declared_size"] != decl.size
                or row["declared_sha256"] != decl.sha256
            ):
                raise DeclarationConflict(
                    f"接收号 {decl.receipt_id} 已存在且声明不一致："
                    f"已登记 站={row['station_id']} 日={row['obs_date']} "
                    f"长={row['declared_size']} sha={row['declared_sha256']}；"
                    f"本次 站={decl.station_id} 日={decl.obs_date.isoformat()} "
                    f"长={decl.size} sha={decl.sha256}"
                )
            return _row_to_dict(row)

        # 新接收：声明长度本身不能超过总额度（暂存与成品共用同一额度）。
        # 注意此处不做“已用 + 声明长度”的全额预留：相同内容重发在发布时会
        # 内容寻址去重、不产生新物理占用，全额预留会把这种合法重发误判为超额。
        # 真正的占用约束在落盘前（_stream_to_staging）与发布前按实际内容核算。
        if decl.size > settings.quota_bytes:
            raise ObsError(
                f"原包字节额度不足：声明长度 {decl.size} 字节已超过总额度 "
                f"{settings.quota_bytes} 字节",
                code="quota_exceeded",
                status_code=507,
            )
        conn.execute(
            "INSERT INTO receipts(receipt_id, station_id, obs_date, declared_size, "
            "declared_sha256, state) VALUES (?, ?, ?, ?, ?, ?)",
            (
                decl.receipt_id,
                decl.station_id,
                decl.obs_date.isoformat(),
                decl.size,
                decl.sha256,
                _UPLOADING,
            ),
        )
        return _row_to_dict(
            conn.execute(
                "SELECT * FROM receipts WHERE receipt_id = ?", (decl.receipt_id,)
            ).fetchone()
        )


def get_receipt(db: Database, receipt_id: str) -> dict:
    row = db.query_one("SELECT * FROM receipts WHERE receipt_id = ?", (receipt_id,))
    if row is None:
        raise ReceiptNotFound(f"接收号 {receipt_id} 不存在")
    return _row_to_dict(row)


def list_receipts(
    db: Database, *, state: str | None, station_id: str | None, limit: int, offset: int
) -> list[dict]:
    sql = "SELECT * FROM receipts WHERE 1=1"
    params: list[object] = []
    if state:
        sql += " AND state = ?"
        params.append(state)
    if station_id:
        sql += " AND station_id = ?"
        params.append(station_id)
    sql += " ORDER BY created_at DESC, receipt_id LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    return [_row_to_dict(r) for r in db.query_all(sql, tuple(params))]


def _finish_upload(
    settings: Settings,
    db: Database,
    *,
    receipt_id: str,
    total_bytes: int,
    digest: str,
) -> dict:
    """字节收齐后的事务内登记；返回最新接收行。"""
    with db.transaction() as conn:
        row = conn.execute(
            "SELECT * FROM receipts WHERE receipt_id = ?", (receipt_id,)
        ).fetchone()
        if row is None:  # pragma: no cover - 锁内不可能被删，防御性
            raise ReceiptNotFound(f"接收号 {receipt_id} 不存在")
        if row["state"] == _PUBLISHED:
            raise AlreadyFinalized(f"接收 {receipt_id} 已发布，无需重复上传")
        if total_bytes != row["declared_size"]:
            msg = (
                f"长度不符：实际收到 {total_bytes} 字节，声明 {row['declared_size']} 字节"
            )
            conn.execute(
                "UPDATE receipts SET last_error = ?, "
                "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE receipt_id = ?",
                (msg, receipt_id),
            )
            raise ContentMismatch(msg)
        if digest != row["declared_sha256"]:
            msg = f"摘要不符：实际 SHA-256={digest}，声明={row['declared_sha256']}"
            conn.execute(
                "UPDATE receipts SET bytes_received = ?, actual_sha256 = ?, last_error = ?, "
                "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE receipt_id = ?",
                (total_bytes, digest, msg, receipt_id),
            )
            raise ContentMismatch(msg)
        conn.execute(
            "UPDATE receipts SET bytes_received = ?, actual_sha256 = ?, last_error = NULL, "
            "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE receipt_id = ?",
            (total_bytes, digest, receipt_id),
        )
        return _row_to_dict(
            conn.execute(
                "SELECT * FROM receipts WHERE receipt_id = ?", (receipt_id,)
            ).fetchone()
        )


def _publish(settings: Settings, db: Database, receipt: dict) -> dict:
    """校验 Parquet 并发布为内容寻址成品（全局锁，跨实例互斥）。"""
    receipt_id = receipt["receipt_id"]
    part = staging_path(settings, receipt_id)
    if not os.path.isfile(part):
        raise UploadNotComplete(f"接收 {receipt_id} 的暂存文件不存在，请重新上传")

    # 先做内容校验；失败时成品区无任何变动，接收保持 UPLOADING 可重传
    result = validate_parquet(
        part,
        station_id=receipt["station_id"],
        obs_date=dt.date.fromisoformat(receipt["obs_date"]),
    )

    with global_lock(settings):
        with db.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM receipts WHERE receipt_id = ?", (receipt_id,)
            ).fetchone()
            if row["state"] == _PUBLISHED:
                return _row_to_dict(row)
            sha = row["declared_sha256"]
            target = blob_path(settings, sha)
            blob_row = conn.execute(
                "SELECT * FROM blobs WHERE sha256 = ?", (sha,)
            ).fetchone()
            if blob_row is None:
                # 新内容：发布前按成品口径核算（blob 已用 + 本包 ≤ 额度）。
                # 暂存在事务提交后即删除，不与成品叠加；硬链接同卷不额外占物理空间。
                blob_used = conn.execute(
                    "SELECT COALESCE(SUM(size), 0) AS n FROM blobs"
                ).fetchone()["n"]
                if int(blob_used) + row["declared_size"] > settings.quota_bytes:
                    raise ObsError(
                        f"原包字节额度不足：成品需要 {row['declared_size']} 字节，"
                        f"成品已占 {blob_used} / 额度 {settings.quota_bytes}",
                        code="quota_exceeded",
                        status_code=507,
                    )
                # 硬链接进成品区（同卷不额外占物理空间，逻辑上计入 blob 额度）
                hardlink_or_copy(part, target)
                conn.execute(
                    "INSERT INTO blobs(sha256, size, ref_count) VALUES (?, ?, 1)",
                    (sha, row["declared_size"]),
                )
            else:
                if not os.path.isfile(target):
                    # 磁盘文件丢失但账本仍在：用本暂存重建，避免缺件成品
                    hardlink_or_copy(part, target)
                conn.execute(
                    "UPDATE blobs SET ref_count = ref_count + 1 WHERE sha256 = ?",
                    (sha,),
                )
            conn.execute(
                "UPDATE receipts SET state = 'PUBLISHED', blob_sha256 = ?, row_count = ?, "
                "last_error = NULL, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') "
                "WHERE receipt_id = ?",
                (sha, result.row_count, receipt_id),
            )
            published = _row_to_dict(
                conn.execute(
                    "SELECT * FROM receipts WHERE receipt_id = ?", (receipt_id,)
                ).fetchone()
            )

        # 事务提交后再删暂存并释放其占用；删除失败不影响已发布事实，
        # 下一轮 reconcile 会按“无 UPLOADING 归属”清掉它。
        safe_remove(part)
        return published


def _stream_to_staging(
    settings: Settings, db: Database, receipt: dict, chunks: list[bytes]
) -> tuple[str, int, str]:
    """线程池中执行的纯同步落盘：截断重写、同步刷盘、计算摘要。

    写入前做实际配额核算（按真实字节，而非声明预留），空间不足明确拒收。
    若内容已存在于成品区（同内容重发），硬链接发布不产生新物理占用，
    因此跳过暂存配额检查。
    """
    total_size = sum(len(c) for c in chunks)
    hasher = hashlib.sha256()
    for c in chunks:
        hasher.update(c)
    digest = hasher.hexdigest()
    existing = db.query_one("SELECT 1 FROM blobs WHERE sha256 = ?", (digest,))
    if existing is None:
        assert_staging_fits(settings, db, total_size)
    part = staging_path(settings, receipt["receipt_id"])
    os.makedirs(os.path.dirname(part), exist_ok=True)
    total = 0
    with open(part, "wb") as fh:
        for chunk in chunks:
            total += len(chunk)
            if total > receipt["declared_size"]:
                fh.flush()
                os.fsync(fh.fileno())
                raise ContentMismatch(
                    f"长度超出声明：已收到 {total} 字节 > "
                    f"声明 {receipt['declared_size']} 字节"
                )
            fh.write(chunk)
        fh.flush()
        os.fsync(fh.fileno())
    return part, total, digest


async def upload_package(
    settings: Settings,
    db: Database,
    receipt_id: str,
    stream: AsyncIterator[bytes],
    declared_content_length: int | None,
) -> dict:
    """流式接收原包：边收边攒块，全部收完后在线程池落盘、校验、发布。

    网络读取走 async 迭代不阻塞事件循环；文件、SQLite、Parquet 等同步重活
    全部进入 I/O 线程池，因此慢上传期间其他包的接收与查询照常处理。
    """
    receipt = await _offload(get_receipt, db, receipt_id)
    if receipt["state"] == _PUBLISHED:
        # 幂等重发：内容完全一致则视为成功，否则拒绝且不动既有成品
        if (
            declared_content_length is not None
            and declared_content_length != receipt["declared_size"]
        ):
            raise ContentMismatch(
                f"接收 {receipt_id} 已发布，重发长度 {declared_content_length} "
                f"与登记 {receipt['declared_size']} 不一致"
            )
        return receipt

    # 同一接收同时只允许一个上传会话（跨进程 flock，进程崩溃内核自动释放）
    lock = FileLock(os.path.join(settings.lock_dir, f"rcpt-{receipt_id}.lock"))
    if not await _offload(lock.acquire, blocking=False):
        raise ReceiptBusy(f"接收 {receipt_id} 正在被另一个上传会话占用，请稍后重试")
    try:
        # 锁内重新读一次，等待期间状态可能已变化
        receipt = await _offload(get_receipt, db, receipt_id)
        if receipt["state"] == _PUBLISHED:
            return receipt

        chunks: list[bytes] = []
        total = 0
        try:
            async for chunk in stream:
                if not chunk:
                    continue
                total += len(chunk)
                if total > receipt["declared_size"]:
                    raise ContentMismatch(
                        f"长度超出声明：已收到 {total} 字节 > "
                        f"声明 {receipt['declared_size']} 字节"
                    )
                chunks.append(chunk)
        except ContentMismatch:
            await _offload(_record_failure, db, receipt_id, "上传中断或超出声明长度，请重新上传")
            raise

        try:
            _part, total_written, digest = await _offload(
                _stream_to_staging, settings, db, receipt, chunks
            )
        except ContentMismatch:
            await _offload(_record_failure, db, receipt_id, "上传超出声明长度，请重新上传")
            raise
        except ObsError:
            # 配额不足：没有落盘，直接向上抛 507
            raise

        try:
            receipt = await _offload(
                _finish_upload, settings, db,
                receipt_id=receipt_id, total_bytes=total_written, digest=digest,
            )
            return await _offload(_publish, settings, db, receipt)
        except (InvalidPackage, ContentMismatch) as exc:
            # 校验失败：不产生任何可下载成品，记录原因，接收保持可重传。
            # 长度/摘要进度由 _finish_upload 已登记的保留，仅补充失败说明。
            await _offload(_record_publish_failure, db, receipt_id, exc.message)
            raise
    finally:
        await _offload(lock.release)


def _record_failure(db: Database, receipt_id: str, message: str) -> None:
    with db.transaction() as conn:
        conn.execute(
            "UPDATE receipts SET bytes_received = 0, actual_sha256 = NULL, last_error = ?, "
            "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE receipt_id = ?",
            (message, receipt_id),
        )


def _record_publish_failure(db: Database, receipt_id: str, message: str) -> None:
    with db.transaction() as conn:
        conn.execute(
            "UPDATE receipts SET last_error = ?, "
            "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE receipt_id = ?",
            (f"原包校验失败：{message}", receipt_id),
        )


def abort_receipt(settings: Settings, db: Database, receipt_id: str) -> dict:
    """中止一个未完成接收：删暂存、回收占额（保留审计记录）。"""
    with try_receipt_lock(settings, receipt_id):
        with db.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM receipts WHERE receipt_id = ?", (receipt_id,)
            ).fetchone()
            if row is None:
                raise ReceiptNotFound(f"接收号 {receipt_id} 不存在")
            if row["state"] == _PUBLISHED:
                raise AlreadyFinalized(f"接收 {receipt_id} 已发布，不能中止")
            conn.execute(
                "UPDATE receipts SET bytes_received = 0, actual_sha256 = NULL, "
                "last_error = '接收已人工中止，额度已释放', "
                "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE receipt_id = ?",
                (receipt_id,),
            )
        safe_remove(staging_path(settings, receipt_id))
        return get_receipt(db, receipt_id)


def delete_receipt(settings: Settings, db: Database, receipt_id: str) -> None:
    """删除接收记录；若为唯一引用则成品 blob 一并回收。"""
    with global_lock(settings):
        with try_receipt_lock(settings, receipt_id):
            with db.transaction() as conn:
                row = conn.execute(
                    "SELECT * FROM receipts WHERE receipt_id = ?", (receipt_id,)
                ).fetchone()
                if row is None:
                    raise ReceiptNotFound(f"接收号 {receipt_id} 不存在")
                if row["state"] == _PUBLISHED and row["blob_sha256"]:
                    sha = row["blob_sha256"]
                    conn.execute(
                        "UPDATE blobs SET ref_count = MAX(ref_count - 1, 0) WHERE sha256 = ?",
                        (sha,),
                    )
                    blob = conn.execute(
                        "SELECT ref_count FROM blobs WHERE sha256 = ?", (sha,)
                    ).fetchone()
                    if blob is not None and blob["ref_count"] == 0:
                        safe_remove(blob_path(settings, sha))
                        conn.execute("DELETE FROM blobs WHERE sha256 = ?", (sha,))
                conn.execute("DELETE FROM receipts WHERE receipt_id = ?", (receipt_id,))
            safe_remove(staging_path(settings, receipt_id))


def get_quota(settings: Settings, db: Database) -> dict:
    return current_usage(settings, db)
