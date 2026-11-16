"""FastAPI 入口：路由、Swagger、后台对账与全局异常映射。

阻塞型 SQLite/文件/Parquet 操作统一在 service 的 I/O 线程池执行，
事件循环保持畅通，因此单个慢上传不会卡住其他包的接收与查询。
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator

from fastapi import BackgroundTasks, FastAPI, Query, Request, status
from fastapi.responses import FileResponse, JSONResponse
from pydantic import ValidationError

from . import __version__
from .config import Settings, load_settings
from .db import Database
from .errors import ObsError
from .schemas import (
    DeclarationIn,
    QuotaOut,
    ReceiptOut,
    ReconcileOut,
    UploadResult,
)
from .service import (
    _offload,
    abort_receipt,
    declare_receipt,
    delete_receipt,
    get_quota,
    get_receipt,
    list_receipts,
    shutdown_io_pool,
    upload_package,
)
from .storage import blob_path, ensure_dirs, reconcile

settings: Settings
db: Database
_reconcile_task: asyncio.Task | None = None


async def _reconcile_loop() -> None:
    while True:
        try:
            await asyncio.sleep(settings.reconcile_interval)
            await _offload(reconcile, settings, db)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - 后台任务不能因单次异常退出
            # 保持静默重试，不影响主流程
            pass


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    global settings, db, _reconcile_task
    settings = load_settings()
    ensure_dirs(settings)
    db = Database(settings.db_path)
    # 启动即对账：回收本实例/另一实例上次崩溃留下的暂存与孤儿成品
    await _offload(reconcile, settings, db)
    _reconcile_task = asyncio.create_task(_reconcile_loop())
    try:
        yield
    finally:
        if _reconcile_task is not None:
            _reconcile_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await _reconcile_task
        shutdown_io_pool()
        db.close()


app = FastAPI(
    title="海洋数仓 · 浮标观测包接收 API",
    description=(
        "浮标站每日 Parquet 观测包入湖接收服务。\n\n"
        "流程：按接收号 **声明**（站号、UTC 日期、长度、SHA-256）→ "
        "**上传原包** → 长度/摘要/Parquet 内容全部通过后发布。\n\n"
        "- 同号同声明重试沿用原记录，异声明拒绝（409）；\n"
        "- 相同内容跨接收只存一份（内容寻址），不重复占额；\n"
        "- 空间不足返回 507；校验失败无可下载成品；\n"
        "- 双实例共享存储，崩溃残留由启动与周期对账自动回收。"
    ),
    version=__version__,
    lifespan=lifespan,
)


@app.exception_handler(ObsError)
async def obs_error_handler(request: Request, exc: ObsError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"code": exc.code, "message": exc.message}},
    )


@app.exception_handler(ValidationError)
async def pydantic_error_handler(request: Request, exc: ValidationError) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"error": {"code": "validation_error", "message": "请求参数校验失败",
                           "details": exc.errors()}},
    )


@app.get("/health", tags=["运维"], summary="健康检查")
async def health() -> dict:
    return {"status": "ok", "version": __version__, "instance": settings.instance_id}


@app.put(
    "/receipts/{receipt_id}/declaration",
    response_model=ReceiptOut,
    status_code=status.HTTP_200_OK,
    tags=["接收"],
    summary="1. 声明接收（幂等）",
)
async def declare(receipt_id: str, decl: DeclarationIn) -> ReceiptOut:
    if decl.receipt_id != receipt_id:
        return JSONResponse(
            status_code=422,
            content={"error": {"code": "validation_error",
                               "message": "路径接收号与请求体接收号不一致"}},
        )
    row = await _offload(declare_receipt, settings, db, decl)
    return ReceiptOut(**row)


@app.post(
    "/receipts/{receipt_id}/content",
    response_model=UploadResult,
    tags=["接收"],
    summary="2. 上传原包（流式，body 为 Parquet 原始字节）",
    status_code=status.HTTP_200_OK,
)
async def upload(receipt_id: str, request: Request) -> UploadResult:
    async def byte_stream() -> AsyncIterator[bytes]:
        async for chunk in request.stream():
            yield chunk

    content_length = request.headers.get("content-length")
    cl = int(content_length) if content_length is not None else None
    row = await upload_package(settings, db, receipt_id, byte_stream(), cl)
    return UploadResult(
        receipt_id=row["receipt_id"],
        state=row["state"],
        bytes_received=row["bytes_received"],
        declared_size=row["declared_size"],
        actual_sha256=row["actual_sha256"],
        row_count=row["row_count"],
        message="原包已发布，可下载" if row["state"] == "PUBLISHED" else "原包已接收，等待完成",
    )


@app.get(
    "/receipts",
    response_model=list[ReceiptOut],
    tags=["接收"],
    summary="查询接收进度（可按状态/站号过滤）",
)
async def list_many(
    state_filter: str | None = Query(None, alias="state", pattern="^(UPLOADING|PUBLISHED)$"),
    station_id: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> list[ReceiptOut]:
    rows = await _offload(
        list_receipts, db, state=state_filter, station_id=station_id,
        limit=limit, offset=offset,
    )
    return [ReceiptOut(**r) for r in rows]


@app.get(
    "/receipts/{receipt_id}",
    response_model=ReceiptOut,
    tags=["接收"],
    summary="查询单个接收进度",
)
async def show(receipt_id: str) -> ReceiptOut:
    row = await _offload(get_receipt, db, receipt_id)
    return ReceiptOut(**row)


@app.get(
    "/receipts/{receipt_id}/download",
    tags=["接收"],
    summary="下载已发布原包（未发布返回 409）",
)
async def download(receipt_id: str):
    row = await _offload(get_receipt, db, receipt_id)
    if row["state"] != "PUBLISHED" or not row["blob_sha256"]:
        return JSONResponse(
            status_code=409,
            content={"error": {"code": "not_published",
                               "message": f"接收 {receipt_id} 尚未发布成功，无可下载成品",
                               "last_error": row["last_error"]}},
        )
    path = blob_path(settings, row["blob_sha256"])
    filename = f"{row['station_id']}_{row['obs_date']}_{receipt_id}.parquet"
    return FileResponse(
        path,
        media_type="application/vnd.apache.parquet",
        filename=filename,
        headers={"X-SHA256": row["blob_sha256"], "X-Bytes": str(row["declared_size"])},
    )


@app.post(
    "/receipts/{receipt_id}/abort",
    response_model=ReceiptOut,
    tags=["接收"],
    summary="中止未完成接收并释放暂存占额",
)
async def abort(receipt_id: str) -> ReceiptOut:
    row = await _offload(abort_receipt, settings, db, receipt_id)
    return ReceiptOut(**row)


@app.delete(
    "/receipts/{receipt_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=JSONResponse,
    tags=["接收"],
    summary="删除接收（唯一引用的成品一并回收）",
)
async def remove(receipt_id: str):
    await _offload(delete_receipt, settings, db, receipt_id)
    return JSONResponse(status_code=status.HTTP_204_NO_CONTENT, content=None)


@app.get("/quota", response_model=QuotaOut, tags=["运维"], summary="查询字节额度占用")
async def quota() -> QuotaOut:
    row = await _offload(get_quota, settings, db)
    return QuotaOut(**row)


@app.post(
    "/admin/reconcile",
    response_model=ReconcileOut,
    tags=["运维"],
    summary="手动触发崩溃回收对账",
)
async def trigger_reconcile(background: BackgroundTasks) -> ReconcileOut:
    result = await _offload(reconcile, settings, db)
    return ReconcileOut(**result)
