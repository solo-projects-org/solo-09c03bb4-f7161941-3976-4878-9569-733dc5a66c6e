"""文件布局、内容寻址存储、文件锁、原件完整性核验与崩溃回收。

目录结构（双实例共享同一个数据卷）：
  data/
    obs.db                  SQLite（WAL 模式，多进程共享）
    staging/<receipt>.part  未完成接收的暂存字节，按接收号命名，解析归属
    blobs/ab/<sha256>       成品原包，内容寻址，同内容跨接收只存一份
    locks/rcpt-<id>.lock    接收级互斥锁（同一接收同时只能有一个上传者）
    locks/global.lock       发布/核验/对账全局锁，串行化跨进程状态变更

原件完整性（v2 新增）：
  磁盘上的共享 blob 可能被“等长改坏”（长度不变、SHA-256 已变）。下载与对账均会
  核验真实字节：摘要/长度不符即把该 blob 及其全部关联接收置为 QUARANTINED（隔离、
  不可供数，保留文件与额度），任一关联接收按原声明补传通过长度、摘要与站日三关后，
  原子替换坏件并恢复全部关联接收，同内容始终只占一份额度。
"""

from __future__ import annotations

import datetime as dt
import errno
import fcntl
import hashlib
import os
from contextlib import contextmanager
from typing import Iterator

from .config import Settings
from .db import Database
from .errors import ArtifactQuarantined, ObsError, ReceiptBusy

# 引用 blob 的“有效接收”状态：隔离期间引用不解除，故两者都计入 ref_count
_REF_STATES = ("PUBLISHED", "QUARANTINED")
_QUARANTINED = "QUARANTINED"
_PUBLISHED = "PUBLISHED"


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
    """全局锁：发布、隔离/恢复与对账互斥，保证跨实例状态变更不互相覆盖。"""
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


def replace_blob_atomic(src: str, dst: str) -> None:
    """用补传通过的正确原包原子替换坏件（同卷优先硬链接到 .tmp 再 rename）。"""
    hardlink_or_copy(src, dst)


def hash_file(path: str) -> str:
    """流式 SHA-256，避免一次性读入大文件。"""
    hasher = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(8 * 1024 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _quarantine_blob(conn, sha: str, reason: str, size: int, mtime_ns: int) -> None:
    """调用方须已持有全局锁与写事务：隔离 blob 并把关联接收全部置为不可供数。"""
    conn.execute(
        "UPDATE blobs SET quarantined = 1, quarantine_reason = ?, last_size = ?, "
        "last_mtime_ns = ?, last_verified_at = ? WHERE sha256 = ?",
        (reason, size, mtime_ns, _now_iso(), sha),
    )
    conn.execute(
        "UPDATE receipts SET state = 'QUARANTINED', last_error = ?, "
        "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') "
        "WHERE blob_sha256 = ? AND state = 'PUBLISHED'",
        (f"原件完整性核验未通过，已停止供数并等待补传：{reason}", sha),
    )


def restore_blob_healthy(conn, sha: str, *, size: int, mtime_ns: int,
                         row_count: int | None) -> int:
    """调用方须已持有全局锁与写事务：清除隔离并恢复全部关联接收供数。

    返回恢复为 PUBLISHED 的接收数（含原本就 PUBLISHED 的不计，仅计 QUARANTINED）。
    """
    conn.execute(
        "UPDATE blobs SET quarantined = 0, quarantine_reason = NULL, last_size = ?, "
        "last_mtime_ns = ?, last_verified_at = ? WHERE sha256 = ?",
        (size, mtime_ns, _now_iso(), sha),
    )
    cur = conn.execute(
        "UPDATE receipts SET state = 'PUBLISHED', row_count = COALESCE(?, row_count), "
        "last_error = NULL, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') "
        "WHERE blob_sha256 = ? AND state = 'QUARANTINED'",
        (row_count, sha),
    )
    return cur.rowcount


def verify_blob_bytes(settings: Settings, db: Database, sha: str) -> dict:
    """下载前的原件字节核验（在 I/O 线程池调用）。

    - 长度/摘要未变（size + mtime_ns 缓存命中）直接放行，避免每次下载重算哈希；
    - 等长改坏必然改变 mtime，缓存失效后重算 SHA-256；
    - 摘要或长度不符：在全局锁+事务内隔离 blob 及全部关联接收后抛 ArtifactQuarantined，
      绝不把坏字节连同原摘要返回给分析员；
    - 已隔离 blob 直接拒绝供数。
    """
    target = blob_path(settings, sha)
    row = db.query_one("SELECT * FROM blobs WHERE sha256 = ?", (sha,))
    if row is None:
        raise ArtifactQuarantined(f"原件 {sha} 未登记，无法供数")
    if row["quarantined"]:
        raise ArtifactQuarantined(
            f"原件已隔离、暂停供数，须按原声明补传：{row['quarantine_reason']}"
        )
    try:
        st = os.stat(target)
    except FileNotFoundError:
        # 缺件交给对账回退；这里拒绝供数，不留缺件成功响应
        raise ArtifactQuarantined(f"原件文件缺失（sha={sha}），等待对账回退与补传")

    size, mtime_ns = st.st_size, st.st_mtime_ns
    cached = (
        row["last_size"] is not None
        and row["last_mtime_ns"] is not None
        and row["last_size"] == size
        and row["last_mtime_ns"] == mtime_ns
        and not row["quarantined"]
    )
    if cached:
        return dict(row)

    # 缓存未命中：重算真实字节摘要（慢操作在全局锁之外执行，不拖住其他接收）
    digest = hash_file(target)
    reason: str | None = None
    if size != row["size"]:
        reason = f"磁盘长度 {size} 与登记长度 {row['size']} 不符"
    elif digest != sha:
        reason = (
            f"磁盘字节 SHA-256={digest} 与登记摘要 {sha} 不符"
            "（原件疑似被等长改坏）"
        )

    # 隔离结论必须在事务提交后再抛异常，否则会被事务上下文回滚
    pending_error: ArtifactQuarantined | None = None
    with global_lock(settings):
        with db.transaction() as conn:
            fresh = conn.execute(
                "SELECT * FROM blobs WHERE sha256 = ?", (sha,)
            ).fetchone()
            if fresh is None:
                pending_error = ArtifactQuarantined(f"原件 {sha} 未登记，无法供数")
            elif fresh["quarantined"]:
                pending_error = ArtifactQuarantined(
                    f"原件已隔离、暂停供数，须按原声明补传：{fresh['quarantine_reason']}"
                )
            else:
                # 取锁期间文件可能已被补传流程替换：以最新指纹复核一次
                try:
                    st2 = os.stat(target)
                except FileNotFoundError:
                    pending_error = ArtifactQuarantined(
                        f"原件文件缺失（sha={sha}），等待对账回退与补传"
                    )
                else:
                    if st2.st_mtime_ns != mtime_ns or st2.st_size != size:
                        # 已被替换：本轮不判定，交由最新一次核验/对账结论
                        pending_error = ArtifactQuarantined(
                            "原件在核验期间被替换，请重试下载以下载最新核验结论"
                        )
                    elif reason is not None:
                        _quarantine_blob(conn, sha, reason, size, mtime_ns)
                        pending_error = ArtifactQuarantined(
                            f"原件完整性核验未通过，已停止供数并等待补传：{reason}"
                        )
                    else:
                        conn.execute(
                            "UPDATE blobs SET last_size = ?, last_mtime_ns = ?, "
                            "last_verified_at = ? WHERE sha256 = ?",
                            (size, mtime_ns, _now_iso(), sha),
                        )
    # 事务已提交；隔离标记现在持久化，再向调用方抛错
    if pending_error is not None:
        raise pending_error
    healthy = db.query_one("SELECT * FROM blobs WHERE sha256 = ?", (sha,))
    return dict(healthy)


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
    """启动与周期性对账：在全局锁 + 写事务内修复崩溃残留与原件损坏。

    修复项：
    1. 暂存区里没有对应 UPLOADING 接收的文件（含其他实例崩溃残留）→ 删除；
    2. UPLOADING 接收缺失暂存文件（写盘/登记中断）→ 清零进度，接收可重传；
    3. 已发布接收缺成品文件 → 回退为 UPLOADING 并解除 blob 引用，
       绝不允许存在“成功记录但缺件”；
    4. 成品 blob 真实字节与登记长度/SHA-256 不符（含等长改坏）→ 隔离该 blob 及
       全部关联接收（QUARANTINED，不可供数、保留文件与额度、等待补传）；
    5. 已隔离 blob 若字节恢复正确 → 解除隔离并恢复全部关联接收供数；
    6. 无任何接收引用的 blob → 删除并注销；账本与磁盘不符 → 按磁盘真相修正。

    慢核验（SHA-256 重算）在全局锁之外、调用线程内执行，因此不会拖住其他站日
    接收的下载与上传；状态变更阶段短暂持锁并复核指纹，避免与补传/删除交错误判。
    """
    removed_staging = 0
    removed_blobs = 0
    reset_receipts = 0
    quarantined_blobs = 0
    restored_receipts = 0

    # ---- 阶段 A：持全局锁快照 blob 磁盘与账本，释放锁后做慢哈希 ----
    # 注意：暂存区不在此快照内做归属判定——暂存增删发生在接收级锁而非全局锁，
    # 阶段 B 必须重新盘点实时暂存与实时 UPLOADING 集合，避免用过快照旧快照
    # 误删另一实例/本实例正在发布的在途暂存。
    with global_lock(settings):
        _total, _staging_names, blob_names = _physical_usage(settings)
        snapshot = {
            "blob_rows": {
                r["sha256"]: dict(r)
                for r in db.query_all("SELECT * FROM blobs")
            },
        }

    # 锁外逐文件核验（可能很慢）：记录每个 blob 的最新指纹与结论
    # verdict: "healthy" | "corrupt:<reason>" | "missing"
    findings: dict[str, dict] = {}
    for sha, brow in snapshot["blob_rows"].items():
        target = blob_path(settings, sha)
        try:
            st = os.stat(target)
            size, mtime_ns = st.st_size, st.st_mtime_ns
            # 隔离中的原件也要重算：补传可能已把字节修好（文件原子替换）
            digest = hash_file(target)
        except FileNotFoundError:
            # 锁外哈希期间文件被删除流程回收：本轮记为缺失，阶段 B 以账本为准
            findings[sha] = {"verdict": "missing", "size": None, "mtime_ns": None}
            continue
        if size != brow["size"]:
            findings[sha] = {
                "verdict": "corrupt",
                "reason": f"磁盘长度 {size} 与登记长度 {brow['size']} 不符",
                "size": size, "mtime_ns": mtime_ns,
            }
        elif digest != sha:
            findings[sha] = {
                "verdict": "corrupt",
                "reason": (
                    f"磁盘字节 SHA-256={digest} 与登记摘要 {sha} 不符"
                    "（原件疑似被等长改坏）"
                ),
                "size": size, "mtime_ns": mtime_ns,
            }
        else:
            findings[sha] = {
                "verdict": "healthy", "size": size, "mtime_ns": mtime_ns,
            }

    # ---- 阶段 B：重新持锁，复核指纹后应用隔离/恢复/回收 ----
    with global_lock(settings):
        with db.transaction() as conn:
            uploading = {
                r["receipt_id"]: r
                for r in conn.execute(
                    "SELECT receipt_id, bytes_received, publishing FROM receipts "
                    "WHERE state = 'UPLOADING'"
                ).fetchall()
            }
            # 发布临界态接收（publishing=1，可能处于 UPLOADING 或 QUARANTINED）：
            # 其暂存正被本/他实例的上传-发布流程使用，对账必须保留，不得当孤儿清理。
            # 判据：能非阻塞拿到接收级 flock，说明没有活跃上传会话（进程已崩溃、
            # 内核释放锁），且暂存缺失 => 陈旧标记清除、暂存按孤儿处理；
            # 拿不到锁（活跃会话持有）=> 一律保留，哪怕暂存尚未落盘。
            flagged = [
                r["receipt_id"]
                for r in conn.execute(
                    "SELECT receipt_id FROM receipts WHERE publishing = 1"
                ).fetchall()
            ]
            publishing_ids: set[str] = set()
            stale_publishing: list[str] = []
            for rid in flagged:
                part_alive = os.path.isfile(staging_path(settings, rid))
                if part_alive:
                    publishing_ids.add(rid)
                    continue
                probe = FileLock(
                    os.path.join(settings.lock_dir, f"rcpt-{rid}.lock")
                )
                if probe.acquire(blocking=False):
                    # 无活跃会话：崩溃残留，释放探测锁并清陈旧标记
                    probe.release()
                    stale_publishing.append(rid)
                else:
                    # 活跃上传会话正在运行（可能尚未落盘）：保留
                    publishing_ids.add(rid)
            if stale_publishing:
                conn.execute(
                    "UPDATE receipts SET publishing = 0, "
                    "updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') "
                    "WHERE publishing = 1 AND receipt_id IN ({})".format(
                        ",".join("?" for _ in stale_publishing)
                    ),
                    stale_publishing,
                )
            published_missing: list[str] = []
            for r in conn.execute(
                "SELECT receipt_id, blob_sha256 FROM receipts WHERE state = 'PUBLISHED'"
            ).fetchall():
                if not r["blob_sha256"] or not os.path.isfile(
                    blob_path(settings, r["blob_sha256"])
                ):
                    published_missing.append(r["receipt_id"])

            # 1 & 2：暂存区对账——以“锁内实时磁盘暂存 + 实时接收登记”为准，
            # 不使用阶段 A 的旧快照，杜绝把刚建立的在途暂存误判为孤儿。
            current_staging: list[str] = []
            if os.path.isdir(settings.staging_dir):
                current_staging = [
                    n for n in os.listdir(settings.staging_dir)
                    if os.path.isfile(os.path.join(settings.staging_dir, n))
                ]
            live_staging = set()
            for name in current_staging:
                receipt_id = name[: -len(".part")] if name.endswith(".part") else None
                if receipt_id is not None and (
                    receipt_id in uploading or receipt_id in publishing_ids
                ):
                    # 属于在途接收，或正处于发布临界态：保留暂存
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

            # 3：缺件的“已发布”记录回退为可重传（隔离件文件仍在，不走这里）
            for receipt_id in published_missing:
                row = conn.execute(
                    "SELECT blob_sha256 FROM receipts WHERE receipt_id = ?",
                    (receipt_id,),
                ).fetchone()
                sha = row["blob_sha256"] if row else None
                if sha:
                    conn.execute(
                        "UPDATE blobs SET ref_count = MAX(ref_count - 1, 0) "
                        "WHERE sha256 = ?",
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

            # 4 & 5：原件字节核验结论应用（先复核指纹，避免与补传替换交错误判）
            for sha, finding in findings.items():
                brow = conn.execute(
                    "SELECT * FROM blobs WHERE sha256 = ?", (sha,)
                ).fetchone()
                if brow is None:
                    continue  # 已被删除流程注销
                target = blob_path(settings, sha)
                try:
                    st = os.stat(target)
                except FileNotFoundError:
                    continue  # 已被删除流程回收，缺件回退在上面处理
                # 指纹在慢哈希期间发生变化 => 文件被补传/替换，本轮跳过，下轮再核
                if (
                    st.st_mtime_ns != finding["mtime_ns"]
                    or st.st_size != finding["size"]
                ):
                    continue
                if finding["verdict"] == "corrupt":
                    if not brow["quarantined"]:
                        _quarantine_blob(
                            conn, sha, finding["reason"],
                            finding["size"], finding["mtime_ns"],
                        )
                        quarantined_blobs += 1
                elif finding["verdict"] == "healthy":
                    conn.execute(
                        "UPDATE blobs SET last_size = ?, last_mtime_ns = ?, "
                        "last_verified_at = ? WHERE sha256 = ?",
                        (st.st_size, st.st_mtime_ns, _now_iso(), sha),
                    )
                    if brow["quarantined"]:
                        restored_receipts += restore_blob_healthy(
                            conn, sha, size=st.st_size,
                            mtime_ns=st.st_mtime_ns, row_count=None,
                        )

            # 6：blob 账本对账（以磁盘为准；引用含 PUBLISHED 与 QUARANTINED）。
            # 锁内重新枚举磁盘：阶段 A 之后文件可能已被补传替换或被删除流程回收，
            # 所有磁盘访问都容忍并发消失，绝不因 FileNotFoundError 中断整轮对账。
            db_shas = {
                r["sha256"]: r for r in conn.execute("SELECT * FROM blobs").fetchall()
            }
            live_disk: dict[str, int] = {}
            if os.path.isdir(settings.blob_dir):
                for prefix in os.listdir(settings.blob_dir):
                    pdir = os.path.join(settings.blob_dir, prefix)
                    if not os.path.isdir(pdir):
                        continue
                    for name in os.listdir(pdir):
                        full = os.path.join(pdir, name)
                        if not os.path.isfile(full) or ".tmp." in name:
                            continue
                        try:
                            live_disk[name] = os.path.getsize(full)
                        except FileNotFoundError:
                            # 枚举期间被并发删除：本轮跳过，下轮对账收敛
                            continue
            disk_shas = set(live_disk)
            for sha in disk_shas - db_shas.keys():
                conn.execute(
                    "INSERT OR IGNORE INTO blobs(sha256, size, ref_count) "
                    "VALUES (?, ?, 0)",
                    (sha, live_disk[sha]),
                )
            placeholders = ",".join("?" for _ in _REF_STATES)
            refs: dict[str, int] = {}
            for r in conn.execute(
                f"SELECT blob_sha256, COUNT(*) AS n FROM receipts "
                f"WHERE state IN ({placeholders}) AND blob_sha256 IS NOT NULL "
                f"GROUP BY blob_sha256",
                _REF_STATES,
            ).fetchall():
                refs[r["blob_sha256"]] = r["n"]
            for sha, row in list(db_shas.items()):
                if sha not in disk_shas:
                    # 磁盘已无（被删除流程回收）：注销账本
                    conn.execute("DELETE FROM blobs WHERE sha256 = ?", (sha,))
                    continue
                real_refs = refs.get(sha, 0)
                real_size = live_disk[sha]
                if row["ref_count"] != real_refs or row["size"] != real_size:
                    conn.execute(
                        "UPDATE blobs SET ref_count = ?, size = ? WHERE sha256 = ?",
                        (real_refs, real_size, sha),
                    )
            for r in conn.execute(
                "SELECT sha256, ref_count, quarantined FROM blobs"
            ).fetchall():
                sha = r["sha256"]
                if sha not in disk_shas:
                    continue
                # 隔离原件仍被 QUARANTINED 接收引用，ref_count>0，绝不能当孤儿删除；
                # 只有真正零引用的健康/隔离 blob 才回收。
                if r["ref_count"] == 0:
                    safe_remove(blob_path(settings, sha))
                    conn.execute("DELETE FROM blobs WHERE sha256 = ?", (sha,))
                    removed_blobs += 1

    return {
        "removed_staging_files": removed_staging,
        "removed_orphan_blobs": removed_blobs,
        "reset_receipts": reset_receipts,
        "quarantined_blobs": quarantined_blobs,
        "restored_receipts": restored_receipts,
    }


def current_usage(settings: Settings, db: Database) -> dict[str, int]:
    """当前配额口径：成品 blob 实际字节 + 在途暂存实际字节。"""
    blob_row = db.query_one(
        "SELECT COALESCE(SUM(size), 0) AS n, "
        "COALESCE(SUM(CASE WHEN quarantined = 1 THEN size ELSE 0 END), 0) AS q "
        "FROM blobs"
    )
    blob_bytes = int(blob_row["n"])
    quarantined_bytes = int(blob_row["q"])
    staging_bytes = 0
    if os.path.isdir(settings.staging_dir):
        for name in os.listdir(settings.staging_dir):
            full = os.path.join(settings.staging_dir, name)
            if os.path.isfile(full):
                staging_bytes += os.path.getsize(full)
    return {
        "quota_bytes": settings.quota_bytes,
        "used_bytes": blob_bytes + staging_bytes,
        "blob_bytes": blob_bytes,
        "staging_bytes": staging_bytes,
        "available_bytes": settings.quota_bytes - blob_bytes - staging_bytes,
        "quarantined_bytes": quarantined_bytes,
    }


def assert_staging_fits(settings: Settings, db: Database, staging_bytes: int) -> None:
    """落盘暂存前的配额核算。

    口径：已发布/隔离成品 blob 占用 + 本次暂存占用。暂存文件按接收号命名、
    同一接收重传会截断重写，活动暂存互不叠加；声明长度本身已限制为
    不超过总额度，因此多个在途暂存的总和不会失控。这样“额度恰好等于
    包大小”的同内容重发不会被误拒（其成品去重、不产生新物理占用）。
    隔离坏件仍占额度，补传替换为同长度正确原件不新增物理占用。
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
