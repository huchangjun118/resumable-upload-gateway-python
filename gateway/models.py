"""核心数据模型：会话状态机、会话记录、分片记录、对象指针。"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class SessionState(str, enum.Enum):
    """上传会话状态机。

    合法迁移：
        ACTIVE -> COMPLETED   （complete 成功，对象原子可见）
        ACTIVE -> CANCELLED   （客户端主动取消）
        ACTIVE -> EXPIRED     （超过 TTL 被回收）
    终态：COMPLETED / CANCELLED / EXPIRED，终态后一切写操作被拒绝。
    """

    ACTIVE = "active"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"

    @property
    def is_terminal(self) -> bool:
        return self in (SessionState.COMPLETED, SessionState.CANCELLED, SessionState.EXPIRED)


@dataclass
class ChunkRecord:
    index: int
    sha256: str
    size: int


@dataclass
class SessionRecord:
    """会话的持久化表示（序列化为 session.json）。"""

    session_id: str
    object_name: str
    owner: str
    total_size: int
    chunk_size: int
    state: SessionState = SessionState.ACTIVE
    created_at: float = 0.0
    expires_at: float = 0.0
    completed_at: float | None = None
    # index -> ChunkRecord；仅保存“已确认”的分片
    chunks: dict[int, ChunkRecord] = field(default_factory=dict)

    @property
    def chunk_count(self) -> int:
        return (self.total_size + self.chunk_size - 1) // self.chunk_size

    def expected_chunk_size(self, index: int) -> int:
        last = self.chunk_count - 1
        if index < last:
            return self.chunk_size
        return self.total_size - self.chunk_size * last


@dataclass
class ObjectPointer:
    """对象的可见性指针（CURRENT 文件内容）。

    读取方先读指针、再按 token 打开对应版本的数据文件，
    指针的原子替换即对象的可见性切换。
    """

    object_name: str
    token: str
    size: int
    sha256: str
    owner: str
    session_id: str
    committed_at: float


# ---------- 序列化 ----------

def chunk_record_to_dict(c: ChunkRecord) -> dict:
    return {"index": c.index, "sha256": c.sha256, "size": c.size}


def chunk_record_from_dict(d: dict) -> ChunkRecord:
    return ChunkRecord(index=int(d["index"]), sha256=str(d["sha256"]), size=int(d["size"]))


def session_to_dict(s: SessionRecord) -> dict:
    return {
        "session_id": s.session_id,
        "object_name": s.object_name,
        "owner": s.owner,
        "total_size": s.total_size,
        "chunk_size": s.chunk_size,
        "state": s.state.value,
        "created_at": s.created_at,
        "expires_at": s.expires_at,
        "completed_at": s.completed_at,
        "chunks": {str(i): chunk_record_to_dict(c) for i, c in s.chunks.items()},
    }


def session_from_dict(d: dict) -> SessionRecord:
    return SessionRecord(
        session_id=str(d["session_id"]),
        object_name=str(d["object_name"]),
        owner=str(d["owner"]),
        total_size=int(d["total_size"]),
        chunk_size=int(d["chunk_size"]),
        state=SessionState(d["state"]),
        created_at=float(d["created_at"]),
        expires_at=float(d["expires_at"]),
        completed_at=d.get("completed_at"),
        chunks={int(i): chunk_record_from_dict(c) for i, c in d.get("chunks", {}).items()},
    )


def pointer_to_dict(p: ObjectPointer) -> dict:
    return {
        "object_name": p.object_name,
        "token": p.token,
        "size": p.size,
        "sha256": p.sha256,
        "owner": p.owner,
        "session_id": p.session_id,
        "committed_at": p.committed_at,
    }


def pointer_from_dict(d: dict) -> ObjectPointer:
    return ObjectPointer(
        object_name=str(d["object_name"]),
        token=str(d["token"]),
        size=int(d["size"]),
        sha256=str(d["sha256"]),
        owner=str(d["owner"]),
        session_id=str(d["session_id"]),
        committed_at=float(d["committed_at"]),
    )
