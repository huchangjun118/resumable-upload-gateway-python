"""会话管理：生命周期、分片提交（乱序/重复/迟到）、完成与取消。

并发约定：每个会话一把锁，所有会改变会话状态或写分片的操作都在锁内完成；
不同会话之间互不阻塞。所有持久化先落盘（含 fsync）再更新内存视图。
"""

from __future__ import annotations

import io
import re
import threading
import time
from typing import IO

from .errors import (
    ChunkDigestMismatchError,
    ChunkIndexError,
    IncompleteUploadError,
    SessionConflictError,
    SessionNotFoundError,
    SessionStateError,
)
from .models import ChunkRecord, SessionRecord, SessionState
from .storage import Storage

_SESSION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class SessionManager:
    def __init__(self, storage: Storage):
        self.storage = storage
        self._locks: dict[str, threading.RLock] = {}
        self._locks_guard = threading.Lock()

    # ---------- 内部工具 ----------

    def _lock_for(self, session_id: str) -> threading.RLock:
        with self._locks_guard:
            lock = self._locks.get(session_id)
            if lock is None:
                lock = threading.RLock()
                self._locks[session_id] = lock
            return lock

    def _load_or_raise(self, session_id: str) -> SessionRecord:
        record = self.storage.load_session(session_id)
        if record is None:
            raise SessionNotFoundError(f"session {session_id!r} not found",
                                       session_id=session_id)
        return record

    def _maybe_expire(self, record: SessionRecord, now: float | None = None) -> SessionRecord:
        """惰性过期：访问时发现 ACTIVE 会话已超过 TTL，则转为 EXPIRED 并回收分片。"""
        now = time.time() if now is None else now
        if record.state == SessionState.ACTIVE and now >= record.expires_at:
            self._expire_locked(record, now)
        return record

    def _expire_locked(self, record: SessionRecord, now: float) -> None:
        record.state = SessionState.EXPIRED
        self.storage.save_session(record)
        # 过期即回收分片占用的空间；元数据保留以便客户端得到可解释的状态
        import shutil

        shutil.rmtree(self.storage.chunks_dir(record.session_id), ignore_errors=True)
        record.chunks.clear()

    def _require_active(self, record: SessionRecord) -> None:
        if record.state != SessionState.ACTIVE:
            raise SessionStateError(
                f"session {record.session_id!r} is {record.state.value}, "
                f"operation requires active",
                session_id=record.session_id,
                state=record.state.value,
            )

    # ---------- 生命周期 ----------

    def create_session(
        self,
        session_id: str,
        object_name: str,
        owner: str,
        total_size: int,
        chunk_size: int,
        ttl_seconds: float,
        now: float | None = None,
    ) -> tuple[SessionRecord, bool]:
        """创建会话；幂等：同 id 同参数返回已有会话，同 id 不同参数报冲突。

        返回 (record, created)。created=False 表示命中已有会话。
        """
        now = time.time() if now is None else now
        if not _SESSION_ID_RE.match(session_id):
            raise SessionConflictError(
                f"invalid session id {session_id!r}", session_id=session_id
            )
        if total_size < 0:
            raise SessionConflictError("total_size must be >= 0")
        max_chunk = self.storage.config.max_chunk_size
        if not (0 < chunk_size <= max_chunk):
            raise SessionConflictError(
                f"chunk_size must be in (0, {max_chunk}]", chunk_size=chunk_size
            )
        if total_size > 0 and chunk_size > total_size:
            # 允许，但归一化为 total_size，保持 expected_chunk_size 语义简单
            chunk_size = total_size

        with self._lock_for(session_id):
            existing = self.storage.load_session(session_id)
            if existing is not None:
                same = (
                    existing.object_name == object_name
                    and existing.owner == owner
                    and existing.total_size == total_size
                    and existing.chunk_size == chunk_size
                )
                if not same:
                    raise SessionConflictError(
                        f"session {session_id!r} already exists with different "
                        f"parameters",
                        session_id=session_id,
                        existing={
                            "object_name": existing.object_name,
                            "owner": existing.owner,
                            "total_size": existing.total_size,
                            "chunk_size": existing.chunk_size,
                        },
                    )
                return self._maybe_expire(existing, now), False

            record = SessionRecord(
                session_id=session_id,
                object_name=object_name,
                owner=owner,
                total_size=total_size,
                chunk_size=chunk_size,
                state=SessionState.ACTIVE,
                created_at=now,
                expires_at=now + ttl_seconds,
            )
            self.storage.save_session(record)
            return record, True

    def get_session(self, session_id: str) -> SessionRecord:
        with self._lock_for(session_id):
            return self._maybe_expire(self._load_or_raise(session_id))

    # ---------- 分片提交 ----------

    def put_chunk(
        self,
        session_id: str,
        index: int,
        sha256: str,
        reader: IO[bytes],
        now: float | None = None,
    ) -> tuple[ChunkRecord, bool]:
        """提交一个分片。返回 (record, stored)。

        * 乱序：任意顺序接受。
        * 重复：同 index 同摘要 -> stored=False 的幂等成功，不重写数据；
                同 index 不同摘要 -> SessionConflictError。
        * 迟到：会话已到终态 -> SessionStateError。
        * 校验：尺寸必须等于该 index 的期望尺寸，sha256 必须匹配，
                失败删除临时文件，不留下任何已确认状态。
        """
        with self._lock_for(session_id):
            record = self._maybe_expire(self._load_or_raise(session_id), now)
            self._require_active(record)
            if not (0 <= index < record.chunk_count):
                raise ChunkIndexError(
                    f"chunk index {index} out of range [0, {record.chunk_count})",
                    session_id=session_id,
                    index=index,
                    chunk_count=record.chunk_count,
                )

            existing = record.chunks.get(index)
            if existing is not None:
                if existing.sha256 == sha256:
                    return existing, False
                raise SessionConflictError(
                    f"chunk {index} already confirmed with different digest",
                    session_id=session_id,
                    index=index,
                )

            final = self.storage.chunk_path(session_id, index)
            size, hasher = self.storage.atomic_write_stream(final, reader)
            expected_size = record.expected_chunk_size(index)
            if size != expected_size:
                final.unlink(missing_ok=True)
                raise ChunkDigestMismatchError(
                    f"chunk {index} size {size} != expected {expected_size}",
                    session_id=session_id,
                    index=index,
                )
            actual = hasher.hexdigest()
            if actual != sha256.lower():
                final.unlink(missing_ok=True)
                raise ChunkDigestMismatchError(
                    f"chunk {index} digest mismatch",
                    session_id=session_id,
                    index=index,
                    expected=sha256,
                    actual=actual,
                )

            chunk = ChunkRecord(index=index, sha256=sha256.lower(), size=size)
            record.chunks[index] = chunk
            self.storage.save_session(record)
            return chunk, True

    # ---------- 完成与取消 ----------

    def missing_chunks(self, record: SessionRecord) -> list[int]:
        return [i for i in range(record.chunk_count) if i not in record.chunks]

    def begin_complete(self, session_id: str) -> SessionRecord:
        """校验完整性并返回快照；真正的对象发布由门面层在对象锁内完成。"""
        with self._lock_for(session_id):
            record = self._maybe_expire(self._load_or_raise(session_id))
            self._require_active(record)
            missing = self.missing_chunks(record)
            if missing:
                raise IncompleteUploadError(
                    f"session {session_id!r} has {len(missing)} missing chunks",
                    session_id=session_id,
                    missing=missing,
                )
            return record

    def finish_complete(self, record: SessionRecord, now: float | None = None) -> None:
        """对象发布成功后，把会话置为 COMPLETED 并清理分片数据。"""
        now = time.time() if now is None else now
        with self._lock_for(record.session_id):
            record.state = SessionState.COMPLETED
            record.completed_at = now
            self.storage.save_session(record)
            import shutil

            shutil.rmtree(
                self.storage.chunks_dir(record.session_id), ignore_errors=True
            )

    def cancel(self, session_id: str) -> SessionRecord:
        with self._lock_for(session_id):
            record = self._maybe_expire(self._load_or_raise(session_id))
            self._require_active(record)
            record.state = SessionState.CANCELLED
            self.storage.save_session(record)
            import shutil

            shutil.rmtree(
                self.storage.chunks_dir(record.session_id), ignore_errors=True
            )
            record.chunks.clear()
            return record

    # ---------- 保留策略回收 ----------

    def sweep_expired(self, now: float | None = None) -> list[str]:
        """把超过 TTL 的 ACTIVE 会话转为 EXPIRED 并回收其分片空间。

        只触碰 ACTIVE 且超时的会话；进行中（未超时）与已提交的不受影响。
        """
        now = time.time() if now is None else now
        expired: list[str] = []
        for summary in self.storage.list_sessions():
            if summary.state != SessionState.ACTIVE or summary.expires_at > now:
                continue
            with self._lock_for(summary.session_id):
                record = self.storage.load_session(summary.session_id)
                if (
                    record is not None
                    and record.state == SessionState.ACTIVE
                    and record.expires_at <= now
                ):
                    self._expire_locked(record, now)
                    expired.append(record.session_id)
        return expired
