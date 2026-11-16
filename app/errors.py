"""领域异常与 HTTP 状态映射。"""

from __future__ import annotations


class ObsError(Exception):
    """所有可预期业务错误的基类。"""

    status_code = 400
    code = "bad_request"

    def __init__(self, message: str, *, code: str | None = None, status_code: int | None = None):
        super().__init__(message)
        self.message = message
        if code is not None:
            self.code = code
        if status_code is not None:
            self.status_code = status_code


class ReceiptNotFound(ObsError):
    status_code = 404
    code = "receipt_not_found"


class DeclarationConflict(ObsError):
    status_code = 409
    code = "declaration_conflict"


class ReceiptBusy(ObsError):
    status_code = 409
    code = "receipt_busy"


class QuotaExceeded(ObsError):
    status_code = 507
    code = "quota_exceeded"


class UploadNotComplete(ObsError):
    status_code = 409
    code = "upload_not_complete"


class ContentMismatch(ObsError):
    status_code = 422
    code = "content_mismatch"


class InvalidPackage(ObsError):
    status_code = 422
    code = "invalid_package"


class AlreadyFinalized(ObsError):
    status_code = 409
    code = "already_finalized"
