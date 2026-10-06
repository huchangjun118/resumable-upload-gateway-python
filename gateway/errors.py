"""Gateway error hierarchy.

All operational failures raised by the gateway derive from :class:`GatewayError`
so that callers (HTTP layer or embedded clients) can map them deterministically.
"""

from __future__ import annotations


class GatewayError(Exception):
    """Base class for all gateway errors."""

    code: str = "gateway_error"
    http_status: int = 500


class SessionNotFound(GatewayError):
    code = "session_not_found"
    http_status = 404


class SessionExpired(GatewayError):
    code = "session_expired"
    http_status = 410


class SessionCanceled(GatewayError):
    code = "session_canceled"
    http_status = 409


class SessionCompleted(GatewayError):
    code = "session_completed"
    http_status = 409


class SessionClosed(SessionCompleted):
    """Session is committing/committed and no longer accepts writes."""

    code = "session_closed"


class ObjectAlreadyExists(GatewayError):
    code = "object_already_exists"
    http_status = 409


class ChunkError(GatewayError):
    code = "chunk_error"
    http_status = 400


class ChunkOutOfRange(ChunkError):
    code = "chunk_out_of_range"


class ChunkChecksumMismatch(ChunkError):
    code = "chunk_checksum_mismatch"


class ChunkSizeMismatch(ChunkError):
    code = "chunk_size_mismatch"


class MissingChunks(GatewayError):
    code = "missing_chunks"
    http_status = 409

    def __init__(self, missing: list[int]) -> None:
        super().__init__(f"missing chunks: {missing}")
        self.missing = missing


class TotalSizeMismatch(ChunkError):
    code = "total_size_mismatch"


class WholeObjectChecksumMismatch(ChunkError):
    code = "whole_object_checksum_mismatch"


class Forbidden(GatewayError):
    code = "forbidden"
    http_status = 403


class QuotaExceeded(GatewayError):
    code = "quota_exceeded"
    http_status = 413


class InvalidRequest(GatewayError):
    code = "invalid_request"
    http_status = 400
