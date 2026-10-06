"""Core domain models: session state, status and persistence records."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class SessionStatus(str, enum.Enum):
    INIT = "init"                # created, no chunks yet
    IN_PROGRESS = "in_progress"  # at least one chunk acknowledged
    COMMITTING = "committing"    # commit in progress; writes are rejected
    COMPLETED = "completed"      # object committed and visible
    CANCELED = "canceled"        # aborted by client
    EXPIRED = "expired"          # garbage-collected after TTL


# Statuses that never accept further writes.
CLOSED_STATUSES = frozenset(
    {
        SessionStatus.COMMITTING,
        SessionStatus.COMPLETED,
        SessionStatus.CANCELED,
        SessionStatus.EXPIRED,
    }
)
TERMINAL_STATUSES = frozenset(
    {SessionStatus.COMPLETED, SessionStatus.CANCELED, SessionStatus.EXPIRED}
)


@dataclass
class ChunkInfo:
    """A single acknowledged chunk."""

    number: int
    offset: int
    size: int
    sha256: str
    filename: str

    def to_dict(self) -> dict:
        return {
            "number": self.number,
            "offset": self.offset,
            "size": self.size,
            "sha256": self.sha256,
            "filename": self.filename,
        }

    @staticmethod
    def from_dict(d: dict) -> "ChunkInfo":
        return ChunkInfo(
            number=int(d["number"]),
            offset=int(d["offset"]),
            size=int(d["size"]),
            sha256=d["sha256"],
            filename=d["filename"],
        )


@dataclass
class SessionState:
    """Full session metadata, persisted as JSON."""

    session_id: str
    tenant: str
    object_name: str
    total_size: int
    chunk_size: int
    status: SessionStatus = SessionStatus.INIT
    created_at: float = 0.0
    updated_at: float = 0.0
    expires_at: float = 0.0
    chunks: dict[int, ChunkInfo] = field(default_factory=dict)
    # Optional client-supplied whole-object checksum verified at commit.
    whole_sha256: str | None = None
    # Set once the object is committed.
    committed_path: str | None = None
    # Monotonic creation sequence; drives deterministic "last writer" rule.
    create_seq: int = 0
    # Client-provided idempotency key for create dedup.
    idempotency_key: str | None = None

    @property
    def total_chunks(self) -> int:
        if self.total_size == 0:
            return 0
        return (self.total_size + self.chunk_size - 1) // self.chunk_size

    def missing_chunks(self) -> list[int]:
        return [i for i in range(self.total_chunks) if i not in self.chunks]

    def received_bytes(self) -> int:
        return sum(c.size for c in self.chunks.values())

    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    def is_closed(self) -> bool:
        return self.status in CLOSED_STATUSES

    def touch(self, now: float, ttl: float) -> None:
        self.updated_at = now
        self.expires_at = now + ttl

    def to_dict(self) -> dict:
        return {
            "session_id": self.session_id,
            "tenant": self.tenant,
            "object_name": self.object_name,
            "total_size": self.total_size,
            "chunk_size": self.chunk_size,
            "status": self.status.value,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "expires_at": self.expires_at,
            "chunks": {str(k): v.to_dict() for k, v in self.chunks.items()},
            "whole_sha256": self.whole_sha256,
            "committed_path": self.committed_path,
            "create_seq": self.create_seq,
            "idempotency_key": self.idempotency_key,
        }

    @staticmethod
    def from_dict(d: dict) -> "SessionState":
        return SessionState(
            session_id=d["session_id"],
            tenant=d["tenant"],
            object_name=d["object_name"],
            total_size=int(d["total_size"]),
            chunk_size=int(d["chunk_size"]),
            status=SessionStatus(d["status"]),
            created_at=float(d["created_at"]),
            updated_at=float(d["updated_at"]),
            expires_at=float(d.get("expires_at", 0.0)),
            chunks={
                int(k): ChunkInfo.from_dict(v)
                for k, v in d.get("chunks", {}).items()
            },
            whole_sha256=d.get("whole_sha256"),
            committed_path=d.get("committed_path"),
            create_seq=int(d.get("create_seq", 0)),
            idempotency_key=d.get("idempotency_key"),
        )
