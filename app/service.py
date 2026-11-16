"""接收流程编排：声明 → 上传 → 校验 → 发布，全部崩溃安全。

v2 增加原件完整性交接：
- 磁盘共享原件被改坏（长度/SHA-256 不符）时，blob 与全部关联接收进入
  QUARANTINED（隔离、不可供数），下载拒绝并给出补传原因；
- 任一关联接收按原声明补传，通过长度、SHA-256 与 Parquet 站日三关后，
  原子替换坏件并一次性恢复全部关联接收，同内容仍只占一份额度；
- 所有状态变更在全局 flock + 写事务内完成，双实例交错也不会误标/误删/重复计额。
"""

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
    ArtifactQuarantined,
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
    restore_blob_healthy,
    safe_remove,
    staging_path,
    try_receipt_lock,
    verify_blob_bytes,
)
from .validation import validate_parquet

# 所有阻塞型 SQLite/文件/Parquet 操作共用的线程池，保证事件循环不被拖住
_io_pool: ThreadPoolExecutor | None = None

_UPLOADING = "UPLOADING"
_PUBLISHED = "PUBLISHED"
_QUARANTINED = "QUARANTINED"


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


def _row_to_dict(row: object) -> dict:
    return dict(row)  # type: ignore[arg-type]


def _set_publishing(db: Database, receipt_id: str, value: int) -> None:
    """登记/清除发布临界标记（与状态同库，跨实例对账可见）。"""
    with db.transaction() as conn:
        conn.execute(
            "UPDATE receipts SET publishing = ?, "
            "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') "
            "WHERE receipt_id = ?",
            (value, receipt_id),
        )


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
    """校验 Parquet 并发布为内容寻址成品（全局锁，跨实例互斥）。

    三种情形：
    1. 全新内容：配额核算后硬链接发布、登记 blob；
    2. 已存在健康 blob：增引用（隔离/待补传接收不重复增 ref_count）；
    3. 已登记 blob 但磁盘损坏/隔离/缺件：用本次通过三关的暂存原子替换坏件，
       解除隔离并恢复全部关联接收，同内容不新增物理占用、不重复计额。
    """
    receipt_id = receipt["receipt_id"]
    part = staging_path(settings, receipt_id)
    if not os.path.isfile(part):
        raise UploadNotComplete(f"接收 {receipt_id} 的暂存文件不存在，请重新上传")

    # 先做内容校验（站号/站日逐行）；失败时成品区无任何变动，接收保持可重传
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
            if row is None:
                # 等待全局锁期间该接收被并发删除：无可发布对象，清理本会话暂存
                safe_remove(part)
                raise ReceiptNotFound(f"接收号 {receipt_id} 已被删除，发布中止")
            if row["state"] == _PUBLISHED:
                return _row_to_dict(row)
            sha = row["declared_sha256"]
            target = blob_path(settings, sha)
            blob_row = conn.execute(
                "SELECT * FROM blobs WHERE sha256 = ?", (sha,)
            ).fetchone()
            prior_state = row["state"]

            def _link_or_replace(dst: str) -> None:
                # 暂存可能在取全局锁期间被并发删除流程回收：转成可重试业务错误，
                # 绝不向上抛裸 FileNotFoundError（HTTP 500）
                if not os.path.isfile(part):
                    raise UploadNotComplete(
                        f"接收 {receipt_id} 的暂存文件在发布前消失（可能被并发删除），"
                        "请重新上传"
                    )
                hardlink_or_copy(part, dst)

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
                _link_or_replace(target)
                st = os.stat(target)
                conn.execute(
                    "INSERT INTO blobs(sha256, size, ref_count, quarantined, "
                    "last_size, last_mtime_ns, last_verified_at) "
                    "VALUES (?, ?, 1, 0, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
                    (sha, row["declared_size"], st.st_size, st.st_mtime_ns),
                )
            else:
                if not os.path.isfile(target):
                    needs_replace = True
                else:
                    needs_replace = (
                        blob_row["quarantined"]
                        or os.path.getsize(target) != row["declared_size"]
                    )
                if needs_replace:
                    # 补传交接：正确原包原子替换坏件/缺件，物理占用不变（同长度）
                    _link_or_replace(target)
                    st = os.stat(target)
                    restore_blob_healthy(
                        conn, sha, size=st.st_size,
                        mtime_ns=st.st_mtime_ns, row_count=result.row_count,
                    )
                    # 本接收若原本是 UPLOADING（缺件回退场景），仍需补一个引用；
                    # 原本 QUARANTINED 的引用一直保留，ref_count 已正确，不重复加。
                    if prior_state == _UPLOADING:
                        conn.execute(
                            "UPDATE blobs SET ref_count = ref_count + 1 "
                            "WHERE sha256 = ?",
                            (sha,),
                        )
                else:
                    if prior_state == _UPLOADING:
                        conn.execute(
                            "UPDATE blobs SET ref_count = ref_count + 1 "
                            "WHERE sha256 = ?",
                            (sha,),
                        )
            conn.execute(
                "UPDATE receipts SET state = 'PUBLISHED', blob_sha256 = ?, "
                "row_count = ?, last_error = NULL, "
                "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') "
                "WHERE receipt_id = ?",
                (sha, result.row_count, receipt_id),
            )
            # 兜底：按真实关联接收重算 ref_count，杜绝任何交错造成的重复计额
            conn.execute(
                "UPDATE blobs SET ref_count = ("
                "SELECT COUNT(*) FROM receipts WHERE blob_sha256 = ? "
                "AND state IN ('PUBLISHED', 'QUARANTINED')) WHERE sha256 = ?",
                (sha, sha),
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
    若内容已存在于成品区（同内容重发/隔离件补传），硬链接发布不产生新物理占用，
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
    QUARANTINED（隔离待补传）与 UPLOADING 一样允许补传；PUBLISHED 走幂等分支。
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

        # 落盘前置发布临界标记：消除“已落盘但未登记”的无保护窗口，
        # 使并发对账在整个落盘+发布期间都不会把该暂存当孤儿清理。
        await _offload(_set_publishing, db, receipt_id, 1)
        try:
            # 至多重试一次：极端交错下暂存可能在标记生效的同一瞬间被上一轮对账
            # 判定，重新落盘即可自愈，不向调用方返回 409/500。
            for attempt in (1, 2):
                try:
                    _part, total_written, digest = await _offload(
                        _stream_to_staging, settings, db, receipt, chunks
                    )
                except ContentMismatch:
                    await _offload(
                        _record_failure, db, receipt_id, "上传超出声明长度，请重新上传"
                    )
                    raise
                except ObsError:
                    # 配额不足：没有落盘，直接向上抛 507
                    raise

                receipt = await _offload(
                    _finish_upload, settings, db,
                    receipt_id=receipt_id, total_bytes=total_written, digest=digest,
                )
                try:
                    return await _offload(_publish, settings, db, receipt)
                except UploadNotComplete:
                    # 暂存在发布前消失（与并发删除/对账交错）：尚有一次重试机会
                    if attempt == 2:
                        raise
        except (InvalidPackage, ContentMismatch) as exc:
            # 校验失败：不产生任何可下载成品，记录原因，接收保持可重传/待补传。
            # 隔离接收的 QUARANTINED 状态与 blob 隔离标记保持不变，等待合格补传。
            await _offload(_record_publish_failure, db, receipt_id, exc.message)
            raise
        finally:
            # 发布结束（成功或失败）即退出临界态；成功时暂存已删，失败时保留可重传
            await _offload(_set_publishing, db, receipt_id, 0)
    finally:
        await _offload(lock.release)


def prepare_download(settings: Settings, db: Database, receipt_id: str) -> dict:
    """下载前准备：核验原件字节，返回最新接收行；坏件直接抛 ArtifactQuarantined。"""
    row = get_receipt(db, receipt_id)
    if row["state"] != _PUBLISHED or not row["blob_sha256"]:
        if row["state"] == _QUARANTINED:
            raise ArtifactQuarantined(
                f"接收 {receipt_id} 的共享原件已隔离、暂停供数，"
                f"请按原声明补传正确原包：{row['last_error']}"
            )
        raise ArtifactQuarantined(
            f"接收 {receipt_id} 尚未发布成功，无可下载成品",
        )
    # 惰性核验：长度/SHA-256 不符会在此隔离并抛错，绝不返回带原摘要的坏字节
    verify_blob_bytes(settings, db, row["blob_sha256"])
    # 核验可能改写状态，且期间接收可能被并发删除：重新读一次并兜底
    latest = db.query_one(
        "SELECT * FROM receipts WHERE receipt_id = ?", (receipt_id,)
    )
    if latest is None:
        raise ReceiptNotFound(f"接收号 {receipt_id} 已被删除，无法下载")
    latest = dict(latest)
    if latest["state"] != _PUBLISHED or not latest["blob_sha256"]:
        raise ArtifactQuarantined(
            f"接收 {receipt_id} 当前无可下载成品：{latest.get('last_error') or '状态已变更'}"
        )
    return latest


def _record_failure(db: Database, receipt_id: str, message: str) -> None:
    with db.transaction() as conn:
        row = conn.execute(
            "SELECT state FROM receipts WHERE receipt_id = ?", (receipt_id,)
        ).fetchone()
        # 隔离待补传的接收不因一次失败补传而改变状态/解除隔离
        if row is not None and row["state"] == _QUARANTINED:
            conn.execute(
                "UPDATE receipts SET last_error = ?, "
                "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE receipt_id = ?",
                (f"补传未通过：{message}", receipt_id),
            )
            return
        conn.execute(
            "UPDATE receipts SET bytes_received = 0, actual_sha256 = NULL, last_error = ?, "
            "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE receipt_id = ?",
            (message, receipt_id),
        )


def _record_publish_failure(db: Database, receipt_id: str, message: str) -> None:
    with db.transaction() as conn:
        row = conn.execute(
            "SELECT state FROM receipts WHERE receipt_id = ?", (receipt_id,)
        ).fetchone()
        prefix = "补传原包校验失败" if (row and row["state"] == _QUARANTINED) else "原包校验失败"
        conn.execute(
            "UPDATE receipts SET last_error = ?, "
            "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE receipt_id = ?",
            (f"{prefix}：{message}", receipt_id),
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
            if row["state"] == _QUARANTINED:
                # 隔离接收仍被共享原件引用，不能中止；只能补传恢复或整体删除
                raise AlreadyFinalized(
                    f"接收 {receipt_id} 的原件已隔离，不能中止；请补传正确原包或删除该接收"
                )
            conn.execute(
                "UPDATE receipts SET bytes_received = 0, actual_sha256 = NULL, "
                "last_error = '接收已人工中止，额度已释放', "
                "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE receipt_id = ?",
                (receipt_id,),
            )
        safe_remove(staging_path(settings, receipt_id))
        return get_receipt(db, receipt_id)


def delete_receipt(settings: Settings, db: Database, receipt_id: str) -> None:
    """删除接收记录；若为唯一引用则成品 blob 一并回收（含隔离坏件）。"""
    with global_lock(settings):
        with try_receipt_lock(settings, receipt_id):
            with db.transaction() as conn:
                row = conn.execute(
                    "SELECT * FROM receipts WHERE receipt_id = ?", (receipt_id,)
                ).fetchone()
                if row is None:
                    raise ReceiptNotFound(f"接收号 {receipt_id} 不存在")
                if row["state"] in (_PUBLISHED, _QUARANTINED) and row["blob_sha256"]:
                    sha = row["blob_sha256"]
                    conn.execute(
                        "UPDATE blobs SET ref_count = MAX(ref_count - 1, 0) "
                        "WHERE sha256 = ?",
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
