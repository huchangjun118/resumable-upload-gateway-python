"""HTTP API (FastAPI).

Endpoints
---------
    POST   /v1/sessions                      create (idempotent w/ key)
    GET    /v1/sessions/{sid}                status + progress
    DELETE /v1/sessions/{sid}                cancel
    PUT    /v1/sessions/{sid}/chunks/{n}     upload one chunk (raw bytes)
    POST   /v1/sessions/{sid}/commit         finish -> atomic publish
    GET    /v1/objects/{name:path}            stream current visible version
    HEAD   /v1/objects/{name:path}            metadata

Tenant identity comes from ``X-Tenant-Id`` (reference trust model; place a
real authenticator at the edge).

Blocking service calls run in a worker thread (``anyio.to_thread``) so large
streaming request bodies never block the event loop. Bounded per-request
buffering keeps memory flat regardless of object/chunk size.
"""

from __future__ import annotations

import io
from typing import Any

import anyio
from fastapi import FastAPI, Header, Request, Response
from fastapi.responses import StreamingResponse

from .errors import (
    ChunkError,
    Forbidden,
    GatewayError,
    InvalidRequest,
    MissingChunks,
    ObjectAlreadyExists,
    QuotaExceeded,
    SessionCanceled,
    SessionClosed,
    SessionCompleted,
    SessionExpired,
    SessionNotFound,
)
from .manager import GatewayService
from .models import SessionStatus


def _session_json(state) -> dict[str, Any]:
    return {
        "session_id": state.session_id,
        "tenant": state.tenant,
        "object_name": state.object_name,
        "total_size": state.total_size,
        "chunk_size": state.chunk_size,
        "status": state.status.value,
        "created_at": state.created_at,
        "updated_at": state.updated_at,
        "expires_at": state.expires_at,
        "received_chunks": len(state.chunks),
        "total_chunks": state.total_chunks,
        "received_bytes": state.received_bytes(),
        "missing_chunks": state.missing_chunks(),
    }


def create_app(service: GatewayService) -> FastAPI:
    app = FastAPI(title="Resumable Upload Gateway", version="0.1.0")
    app.state.service = service

    async def tenant(x_tenant_id: str | None) -> str:
        if not x_tenant_id:
            raise Forbidden("missing X-Tenant-Id header")
        return x_tenant_id.strip()

    @app.exception_handler(GatewayError)
    async def on_gateway_error(_request, exc: GatewayError):
        return _json_error(exc)

    @app.post("/v1/sessions")
    async def create_session(
        request: Request,
        x_tenant_id: str | None = Header(default=None),
    ):
        t = await tenant(x_tenant_id)
        body = await _json_body(request)
        state = await anyio.to_thread.run_sync(
            lambda: service.create_session(
                tenant=t,
                object_name=body["object_name"],
                total_size=int(body["total_size"]),
                chunk_size=body.get("chunk_size"),
                whole_sha256=body.get("whole_sha256"),
                session_id=body.get("session_id"),
                idempotency_key=body.get("idempotency_key"),
            )
        )
        return _session_json(state)

    @app.get("/v1/sessions/{session_id}")
    async def get_session(
        session_id: str, x_tenant_id: str | None = Header(default=None)
    ):
        t = await tenant(x_tenant_id)
        state = await anyio.to_thread.run_sync(
            lambda: service.get_session(t, session_id)
        )
        return _session_json(state)

    @app.delete("/v1/sessions/{session_id}", status_code=204)
    async def abort_session(
        session_id: str, x_tenant_id: str | None = Header(default=None)
    ):
        t = await tenant(x_tenant_id)
        await anyio.to_thread.run_sync(
            lambda: service.abort_session(t, session_id)
        )
        return Response(status_code=204)

    @app.put("/v1/sessions/{session_id}/chunks/{number}")
    async def put_chunk(
        session_id: str,
        number: int,
        request: Request,
        x_content_sha256: str = Header(...),
        x_chunk_size: str | None = Header(default=None),
        x_tenant_id: str | None = Header(default=None),
    ):
        t = await tenant(x_tenant_id)
        declared_size = int(x_chunk_size) if x_chunk_size is not None else None
        # Stream the body through a small async iterator into a file-like
        # adapter; the service reads it in fixed-size buffers.
        stream = _AioStream(request, service.config.read_buffer_size)
        state = await anyio.to_thread.run_sync(
            lambda: service.put_chunk(
                tenant=t,
                session_id=session_id,
                number=number,
                stream=stream,
                declared_sha256=x_content_sha256,
                declared_size=declared_size,
            )
        )
        return _session_json(state)

    @app.post("/v1/sessions/{session_id}/commit")
    async def commit_session(
        session_id: str, x_tenant_id: str | None = Header(default=None)
    ):
        t = await tenant(x_tenant_id)
        state = await anyio.to_thread.run_sync(
            lambda: service.commit_session(t, session_id)
        )
        return _session_json(state)

    @app.head("/v1/objects/{object_name:path}")
    async def head_object(
        object_name: str, x_tenant_id: str | None = Header(default=None)
    ):
        t = await tenant(x_tenant_id)
        obj = await anyio.to_thread.run_sync(
            lambda: service.head_object(t, object_name)
        )
        return Response(
            headers={
                "Content-Length": str(obj.size),
                "ETag": f'"{obj.sha256 or ""}"',
            }
        )

    @app.get("/v1/objects/{object_name:path}")
    async def get_object(
        object_name: str, x_tenant_id: str | None = Header(default=None)
    ):
        t = await tenant(x_tenant_id)
        obj = await anyio.to_thread.run_sync(
            lambda: service.head_object(t, object_name)
        )

        def iterfile():
            with service.open_object(t, object_name) as f:
                while True:
                    chunk = f.read(service.config.read_buffer_size)
                    if not chunk:
                        break
                    yield chunk

        return StreamingResponse(
            iterfile(),
            media_type="application/octet-stream",
            headers={"Content-Length": str(obj.size)},
        )

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    return app


class _AioStream:
    """Adapt an async request body to a bounded synchronous ``read(n)``.

    The async producer (running on the event loop) and the sync consumer
    (running in a worker thread) rendezvous through a *single-slot* buffer.
    Back-pressure is therefore intrinsic: at most one chunk (~read_buffer
    size) is queued, so memory is bounded independently of the body size.
    """

    def __init__(self, request: Request, bufsize: int) -> None:
        import asyncio
        import threading

        self._request = request
        self._bufsize = bufsize
        self._loop = asyncio.get_event_loop()
        self._slot: bytes | None = None
        self._eof = False
        self._error: BaseException | None = None
        self._cond = threading.Condition()
        self._task = asyncio.ensure_future(self._produce())

    async def _produce(self) -> None:
        try:
            async for raw in self._request.stream():
                if not raw:
                    continue
                # Hand bounded pieces to the slot to cap buffer size.
                view = memoryview(raw)
                while view:
                    piece = bytes(view[: self._bufsize])
                    view = view[self._bufsize :]
                    await anyio.to_thread.run_sync(self._put, piece)
        except BaseException as exc:
            await anyio.to_thread.run_sync(self._fail, exc)
        finally:
            await anyio.to_thread.run_sync(self._put_eof)

    def _put(self, data: bytes) -> None:
        with self._cond:
            while self._slot is not None and self._error is None:
                self._cond.wait()
            if self._error is not None:
                return
            self._slot = data
            self._cond.notify_all()

    def _put_eof(self) -> None:
        with self._cond:
            while self._slot is not None and self._error is None:
                self._cond.wait()
            self._eof = True
            self._cond.notify_all()

    def _fail(self, exc: BaseException) -> None:
        with self._cond:
            self._error = exc
            self._eof = True
            self._cond.notify_all()

    def read(self, size: int = -1) -> bytes:
        with self._cond:
            while self._slot is None and not self._eof:
                if self._error is not None:
                    raise self._error
                self._cond.wait(timeout=1.0)
            if self._error is not None:
                raise self._error
            if self._slot is None:
                return b""
            if size is None or size < 0 or len(self._slot) <= size:
                data = self._slot
                self._slot = None
                self._cond.notify_all()
                return data
            data = self._slot[:size]
            self._slot = self._slot[size:]
            return data


async def _json_body(request: Request) -> dict:
    try:
        body = await request.json()
    except Exception as exc:
        raise InvalidRequest("request body must be JSON") from exc
    if not isinstance(body, dict):
        raise InvalidRequest("request body must be a JSON object")
    return body


def _json_error(exc: GatewayError) -> Response:
    from fastapi.responses import JSONResponse

    status = getattr(exc, "http_status", 500)
    payload: dict[str, Any] = {"error": exc.code, "message": str(exc)}
    if isinstance(exc, MissingChunks):
        payload["missing_chunks"] = exc.missing
    return JSONResponse(status_code=status, content=payload)
