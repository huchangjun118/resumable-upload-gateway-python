"""网关统一错误类型。

所有可预期的业务错误都继承 GatewayError，携带稳定的错误码，
便于 API 层映射为确定性的响应。
"""


class GatewayError(Exception):
    """所有网关业务错误的基类。"""

    code = "internal_error"

    def __init__(self, message: str = "", **context):
        super().__init__(message or self.code)
        self.message = message or self.code
        self.context = context


class SessionNotFoundError(GatewayError):
    code = "session_not_found"


class SessionConflictError(GatewayError):
    """重复创建会话但参数不一致，或分片重复提交但摘要不同。"""

    code = "session_conflict"


class SessionStateError(GatewayError):
    """会话状态不允许当前操作（已完成/已取消/已过期）。"""

    code = "session_state_error"


class ChunkDigestMismatchError(GatewayError):
    code = "chunk_digest_mismatch"


class ChunkIndexError(GatewayError):
    code = "chunk_index_out_of_range"


class IncompleteUploadError(GatewayError):
    """complete 时仍有缺失分片。"""

    code = "incomplete_upload"


class QuotaExceededError(GatewayError):
    code = "quota_exceeded"


class OwnerMismatchError(GatewayError):
    code = "owner_mismatch"


class ObjectNotFoundError(GatewayError):
    code = "object_not_found"
