"""门面层：组合存储、会话、配额与并发控制，对外提供完整上传网关。

一致性语义：
    * 对象可见性由 CURRENT 指针的原子替换决定；提交要么完整发生，要么不发生。
    * 同一对象名的并发提交在对象锁内串行化，后完成者覆盖先完成者
      （last-committer-wins），未胜出/失败的会话不在 objects/ 下留任何残留。
    * 读取方先取指针快照再读数据文件：提交进行中看到的是旧版本（或不存在），
      绝不看到不完整对象或混合版本；已打开的旧版本文件描述符仍然可读。
    * 配额与归属校验发生在任何持久写入之前；被拒绝的请求不产生孤儿数据。
"""

from __future__ import annotations

import hashlib
import os
import threading
import time
import uuid
from typing import IO, Iterator

from .config import GatewayConfig
from .errors import ObjectNotFoundError, OwnerMismatchError, QuotaExceededError
from .models import ObjectPointer, SessionRecord, SessionState
from .sessions import SessionManager
from .storage import Storage


class UploadGateway:
    def __init__(self, config: GatewayConfig | None = None):
        self.config = config or GatewayConfig()
        self.storage = Storage(self.config)
        self.storage.ensure_layout()
        self.storage.recover()
        self.sessions = SessionManager(self.storage)
        self._object_locks: dict[str, threading.Lock] = {}
        self._object_locks_guard = threading.Lock()
        self._usage_lock = threading.Lock()
        self._usage: dict[str, int] = {}
        self._rebuild_usage()

    # ---------- 配额记账 ----------

    def _rebuild_usage(self) -> None:
        """从磁盘扫描重建内存配额视图（启动与回收后调用）。"""
        usage: dict[str, int] = {}
        # 已提交对象
        if self.storage.objects_dir.exists():
            for f in self.storage.objects_dir.iterdir():
                if not f.name.endswith(".CURRENT"):
                    continue
                from urllib.parse import unquote

                name = unquote(f.name[: -len(".CURRENT")])
                ptr = self.storage.read_pointer(name)
                if ptr is not None:
                    usage[ptr.owner] = usage.get(ptr.owner, 0) + ptr.size
        # 进行中会话的已确认分片
        for record in self.storage.list_sessions():
            if record.state == SessionState.ACTIVE:
                confirmed = sum(c.size for c in record.chunks.values())
                if confirmed:
                    usage[record.owner] = usage.get(record.owner, 0) + confirmed
        with self._usage_lock:
            self._usage = usage

    def used_bytes(self, owner: str) -> int:
        with self._usage_lock:
            return self._usage.get(owner, 0)

    def _adjust_usage(self, owner: str, delta: int) -> None:
        with self._usage_lock:
            new = self._usage.get(owner, 0) + delta
            if new <= 0:
                self._usage.pop(owner, None)
            else:
                self._usage[owner] = new

    def _check_quota(self, owner: str, incoming: int) -> None:
        """在任何持久写入之前调用；超配额直接拒绝。"""
        quota = self.config.quota_for(owner)
        with self._usage_lock:
            used = self._usage.get(owner, 0)
            if used + incoming > quota:
                raise QuotaExceededError(
                    f"owner {owner!r} quota exceeded: used={used} "
                    f"incoming={incoming} quota={quota}",
                    owner=owner,
                    used=used,
                    incoming=incoming,
                    quota=quota,
                )

    # ---------- 归属校验 ----------

    @staticmethod
    def _require_owner(record: SessionRecord, owner: str) -> None:
        if record.owner != owner:
            raise OwnerMismatchError(
                f"session {record.session_id!r} belongs to {record.owner!r}, "
                f"not {owner!r}",
                session_id=record.session_id,
                owner=record.owner,
            )

    def _object_lock(self, object_name: str) -> threading.Lock:
        with self._object_locks_guard:
            lock = self._object_locks.get(object_name)
            if lock is None:
                lock = threading.Lock()
                self._object_locks[object_name] = lock
            return lock

    # ---------- 会话操作（带归属与配额） ----------

    def create_session(
        self,
        session_id: str,
        object_name: str,
        owner: str,
        total_size: int,
        chunk_size: int,
    ) -> tuple[SessionRecord, bool]:
        # 准入校验：声明的总大小就不得超配额，且发生在任何持久写入之前
        self._check_quota(owner, total_size)
        return self.sessions.create_session(
            session_id=session_id,
            object_name=object_name,
            owner=owner,
            total_size=total_size,
            chunk_size=chunk_size,
            ttl_seconds=self.config.session_ttl_seconds,
        )

    def get_session(self, session_id: str, owner: str | None = None) -> SessionRecord:
        record = self.sessions.get_session(session_id)
        if owner is not None:
            self._require_owner(record, owner)
        return record

    def put_chunk(
        self,
        session_id: str,
        index: int,
        sha256: str,
        reader: IO[bytes],
        owner: str,
    ) -> tuple[bool, int]:
        """提交分片，返回 (stored, confirmed_bytes)。

        配额在写入前按该分片的期望尺寸预检；确认后按实际字节记账。
        """
        record = self.get_session(session_id, owner=owner)
        self._require_owner(record, owner)
        expected = (
            record.expected_chunk_size(index)
            if 0 <= index < record.chunk_count
            else 0
        )
        self._check_quota(owner, expected)
        chunk, stored = self.sessions.put_chunk(session_id, index, sha256, reader)
        if stored:
            self._adjust_usage(owner, chunk.size)
        return stored, chunk.size

    def cancel_session(self, session_id: str, owner: str) -> SessionRecord:
        record = self.get_session(session_id, owner=owner)
        confirmed = sum(c.size for c in record.chunks.values())
        result = self.sessions.cancel(session_id)
        self._adjust_usage(owner, -confirmed)
        return result

    # ---------- 提交：装配 + 原子发布 ----------

    def complete_session(self, session_id: str, owner: str) -> ObjectPointer:
        record = self.get_session(session_id, owner=owner)
        confirmed = sum(c.size for c in record.chunks.values())
        snapshot = self.sessions.begin_complete(session_id)
        token = uuid.uuid4().hex
        # 对象锁串行化同一对象名的并发提交：结果由获得锁的顺序确定
        with self._object_lock(snapshot.object_name):
            pointer, old = self._assemble_and_publish(snapshot, token)
        # 记账：分片空间释放，对象空间入账；被覆盖对象的属主释放旧空间
        self._adjust_usage(snapshot.owner, pointer.size - confirmed)
        if old is not None:
            self._adjust_usage(old.owner, -old.size)
        self.sessions.finish_complete(snapshot)
        return pointer

    def _assemble_and_publish(
        self, snapshot: SessionRecord, token: str
    ) -> tuple[ObjectPointer, ObjectPointer | None]:
        """流式装配分片 -> staging，整体校验后原子切换指针。

        内存占用以 copy_buffer_size 为界，与对象大小无关。
        任何失败都只留下 staging 临时文件，由 recover() 清理，对外不可见。
        """
        buf_size = self.config.copy_buffer_size
        staged = self.storage.staging_path(token)
        hasher = hashlib.sha256()
        total = 0
        try:
            with open(staged, "wb") as out:
                for index in range(snapshot.chunk_count):
                    chunk_file = self.storage.chunk_path(
                        snapshot.session_id, index
                    )
                    with open(chunk_file, "rb") as src:
                        while True:
                            buf = src.read(buf_size)
                            if not buf:
                                break
                            hasher.update(buf)
                            out.write(buf)
                            total += len(buf)
                out.flush()
                os.fsync(out.fileno())
            if total != snapshot.total_size:
                raise IOError(
                    f"assembled size {total} != declared {snapshot.total_size}"
                )
            pointer = ObjectPointer(
                object_name=snapshot.object_name,
                token=token,
                size=total,
                sha256=hasher.hexdigest(),
                owner=snapshot.owner,
                session_id=snapshot.session_id,
                committed_at=time.time(),
            )
            old = self.storage.commit_object(
                snapshot.object_name, token, pointer
            )
        except BaseException:
            staged.unlink(missing_ok=True)
            raise
        if old is not None:
            # 旧版本数据文件在指针切换后清理；崩溃残留由 recover() 兜底
            self.storage.remove_data_file(old.object_name, old.token)
        return pointer, old

    # ---------- 读取 ----------

    def stat_object(self, object_name: str) -> ObjectPointer:
        pointer = self.storage.read_pointer(object_name)
        if pointer is None:
            raise ObjectNotFoundError(f"object {object_name!r} not found",
                                      object_name=object_name)
        return pointer

    def open_object(self, object_name: str) -> tuple[ObjectPointer, IO[bytes]]:
        """返回 (指针快照, 数据流)。调用方负责关闭流。"""
        pointer = self.stat_object(object_name)
        return pointer, self.storage.open_object_data(pointer)

    def read_object_iter(
        self, object_name: str, chunk: int | None = None
    ) -> tuple[ObjectPointer, Iterator[bytes]]:
        pointer, fp = self.open_object(object_name)
        size = chunk or self.config.copy_buffer_size

        def _iter() -> Iterator[bytes]:
            with fp:
                while True:
                    buf = fp.read(size)
                    if not buf:
                        break
                    yield buf

        return pointer, _iter()

    # ---------- 保留策略 ----------

    def sweep_expired(self) -> list[str]:
        expired = self.sessions.sweep_expired()
        if expired:
            self._rebuild_usage()
        return expired
