"""FastAPI HTTP 层：薄封装，业务逻辑全在 UploadGateway。

错误码映射稳定，客户端可据此得到确定且可解释的响应。
"""

from __future__ import annotations

import uuid

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from .errors import GatewayError
from .gateway import UploadGateway
from .models import SessionRecord

_STATUS_BY_CODE = {
    "session_not_found": 404,
    "object_not_found": 404,
    "session_conflict": 409,
    "session_state_error": 409,
    "incomplete_upload": 409,
    "chunk_digest_mismatch": 422,
    "chunk_index_out_of_range": 422,
    "quota_exceeded": 413,
    "owner_mismatch": 403,
}


def _session_view(record: SessionRecord, created: bool | None = None) -> dict:
    view = {
        "session_id": record.session_id,
        "object_name": record.object_name,
        "owner": record.owner,
        "total_size": record.total_size,
        "chunk_size": record.chunk_size,
        "state": record.state.value,
        "created_at": record.created_at,
        "expires_at": record.expires_at,
        "completed_at": record.completed_at,
        "confirmed_chunks": sorted(record.chunks.keys()),
    }
    if created is not None:
        view["created"] = created
    return view


class CreateSessionRequest(BaseModel):
    object_name: str
    owner: str
    total_size: int = Field(ge=0)
    chunk_size: int = Field(gt=0)
    session_id: str | None = None  # 缺省时服务端生成


class OwnerRequest(BaseModel):
    owner: str


def create_app(gateway: UploadGateway | None = None) -> FastAPI:
    app = FastAPI(title="resumable-upload-gateway")
    app.state.gateway = gateway or UploadGateway()

    @app.exception_handler(GatewayError)
    async def _gateway_error(_: Request, exc: GatewayError) -> JSONResponse:
        return JSONResponse(
            status_code=_STATUS_BY_CODE.get(exc.code, 500),
            content={"error": exc.code, "message": exc.message, **exc.context},
        )

    def gw() -> UploadGateway:
        return app.state.gateway

    @app.post("/sessions", status_code=201)
    def create_session(req: CreateSessionRequest) -> dict:
        record, created = gw().create_session(
            session_id=req.session_id or uuid.uuid4().hex,
            object_name=req.object_name,
            owner=req.owner,
            total_size=req.total_size,
            chunk_size=req.chunk_size,
        )
        return _session_view(record, created=created)

    @app.get("/sessions/{session_id}")
    def get_session(session_id: str, x_owner: str | None = Header(None)) -> dict:
        return _session_view(gw().get_session(session_id, owner=x_owner))

    @app.put("/sessions/{session_id}/chunks/{index}")
    async def put_chunk(
        session_id: str,
        index: int,
        request: Request,
        x_chunk_sha256: str = Header(...),
        x_owner: str = Header(...),
    ) -> dict:
        body = await request.body()
        import io

        stored, size = gw().put_chunk(
            session_id, index, x_chunk_sha256, io.BytesIO(body), owner=x_owner
        )
        return {"stored": stored, "size": size, "index": index}

    @app.post("/sessions/{session_id}/complete")
    def complete(session_id: str, req: OwnerRequest) -> dict:
        pointer = gw().complete_session(session_id, owner=req.owner)
        return {
            "object_name": pointer.object_name,
            "size": pointer.size,
            "sha256": pointer.sha256,
            "committed_at": pointer.committed_at,
        }

    @app.post("/sessions/{session_id}/cancel")
    def cancel(session_id: str, req: OwnerRequest) -> dict:
        return _session_view(gw().cancel_session(session_id, owner=req.owner))

    @app.get("/objects/{object_name:path}")
    def get_object(object_name: str) -> StreamingResponse:
        pointer, stream = gw().read_object_iter(object_name)
        return StreamingResponse(
            stream,
            media_type="application/octet-stream",
            headers={
                "Content-Length": str(pointer.size),
                "X-Object-SHA256": pointer.sha256,
            },
        )

    @app.head("/objects/{object_name:path}")
    def head_object(object_name: str) -> JSONResponse:
        pointer = gw().stat_object(object_name)
        return JSONResponse(
            content=None,
            status_code=200,
            headers={
                "Content-Length": str(pointer.size),
                "X-Object-SHA256": pointer.sha256,
            },
        )

    @app.post("/admin/sweep")
    def sweep() -> dict:
        return {"expired": gw().sweep_expired()}

    return app
