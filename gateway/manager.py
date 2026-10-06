"""GatewayService: session state machine + upload/object operations.

Concurrency model
-----------------
* One process-wide ``threading.RLock`` guards session state transitions and
  the in-memory indexes; metadata writes happen while holding it (they are
  small and fast).
* Per-object locks serialize the *commit* (assemble + checksum + atomic
  rename) for a given object name, while still allowing different objects to
  commit concurrently. Large file assembly/IO happens outside the global
  lock, with sessions flipped to ``COMMITTING`` first so late writes/reads
  have a defined answer.
* Chunk bytes are streamed straight to disk in fixed-size buffers; memory
  use is independent of object size.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import uuid

from .config import Config
from .errors import (
    ChunkChecksumMismatch,
    ChunkOutOfRange,
    ChunkSizeMismatch,
    Forbidden,
    InvalidRequest,
    MissingChunks,
    ObjectAlreadyExists,
    QuotaExceeded,
    SessionCanceled,
    SessionClosed,
    SessionCompleted,
    SessionExpired,
    SessionNotFound,
    TotalSizeMismatch,
    WholeObjectChecksumMismatch,
)
from .models import ChunkInfo, SessionState, SessionStatus
from .quota import QuotaManager
from .session_store import SessionStore
from .storage import LocalStorage

DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024


class GatewayService:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.storage = LocalStorage(config.data_dir, config.read_buffer_size)
        self.session_store = SessionStore(self.storage)
        self.quota = QuotaManager(
            self.storage, default_limit=config.default_quota_bytes
        )
        self._lock = threading.RLock()
        self._commit_locks: dict[tuple[str, str], threading.Lock] = {}
        # Per-object high-water mark: create_seq of the session that last
        # published the object. Any older/equal session attempting to commit
        # afterward deterministically loses, even if the winner is no longer
        # in the in-memory index (defensive against stale/committing peers).
        self._published_seq: dict[tuple[str, str], int] = {}
        # session_id -> SessionState cache; also authority for TTL decisions.
        self._sessions: dict[tuple[str, str], SessionState] = {}
        # (tenant, object_name) -> session_id of session currently committing
        # or completed-visible tracking (index only; disk remains truth).
        self._seq = 0
        self._reaper_thread: threading.Thread | None = None
        self._stopping = threading.Event()

    # ==================================================================
    # Lifecycle
    # ==================================================================
    def start(self) -> None:
        self.storage.ensure_layout()
        self._recover()
        if self.config.reap_interval_seconds > 0:
            t = threading.Thread(
                target=self._reap_loop, name="ugw-reaper", daemon=True
            )
            self._reaper_thread = t
            t.start()

    def shutdown(self) -> None:
        self._stopping.set()
        t = self._reaper_thread
        if t is not None:
            t.join(timeout=2.0)

    # ==================================================================
    # Recovery
    # ==================================================================
    def _recover(self) -> None:
        """Reload all sessions from disk and reconcile half-finished commits."""
        with self._lock:
            self._sessions.clear()
            max_seq = 0
            committing: list[SessionState] = []
            for state in self.session_store.iter_all():
                max_seq = max(max_seq, state.create_seq)
                self._sessions[(state.tenant, state.session_id)] = state
                if state.status == SessionStatus.COMMITTING:
                    committing.append(state)
            self._seq = max_seq

            # 1) Finish or roll back any commit interrupted by a crash. Do
            #    this BEFORE rebuilding quota so the on-disk outcome is final.
            for state in committing:
                self._resume_commit_locked(state)

            # 2) Rebuild quota accounting from the resulting on-disk reality.
            #    This is authoritative and immune to intermediate commit
            #    bookkeeping that happened before the crash.
            tenants = {t for (t, _sid) in self._sessions}
            tenant_reserved: dict[str, int] = {}
            for (_tenant, _sid), st in self._sessions.items():
                if not st.is_terminal():
                    tenant_reserved[_tenant] = (
                        tenant_reserved.get(_tenant, 0) + st.total_size
                    )
            for tenant in tenants:
                committed = sum(
                    size for _name, size in self.storage.list_objects(tenant)
                )
                self.quota.rebuild(
                    tenant, committed, tenant_reserved.get(tenant, 0)
                )
                # Any pre-crash object outranks recovered unfinished sessions:
                # new sessions are minted with seq > self._seq, so seeding the
                # watermark at self._seq makes stale sessions lose while still
                # allowing genuine newer uploads to overwrite.
                for name, _size in self.storage.list_objects(tenant):
                    self._published_seq[(tenant, name)] = self._seq

    def _resume_commit_locked(self, state: SessionState) -> None:
        """Finish or roll back a commit interrupted by a crash."""
        jpath = self.storage.journal_path(state.tenant, state.session_id)
        journal = None
        if os.path.isfile(jpath):
            try:
                with open(jpath, "rb") as f:
                    journal = json.loads(f.read())
            except (OSError, json.JSONDecodeError):
                journal = None
        target = state.committed_path or self.storage.object_path(
            state.tenant, state.object_name
        )
        # If the target already exists with the expected size, the rename may
        # have completed just before the crash.
        if (
            journal is not None
            and os.path.isfile(target)
            and os.path.getsize(target) == state.total_size
        ):
            self._finalize_commit_locked(state, journal)
            return
        try:
            replaced = 0
            if self.storage.object_exists(state.tenant, state.object_name):
                replaced = self.storage.stat_object(
                    state.tenant, state.object_name
                ).size
            self._assemble_and_publish(state, replaced, journal_hint=journal)
        except Exception:
            # Roll back: chunks stay on disk, client may retry commit.
            state.status = SessionStatus.IN_PROGRESS
            state.touch(time.time(), self.config.session_ttl_seconds)
            self.session_store.save(state)
            self.storage.remove_staging(state.tenant, state.session_id)
            self._silent_remove(jpath)
            return

        # Staging file was assembled, verified and atomically renamed; finish
        # bookkeeping exactly like a normal commit (quota, journal cleanup,
        # session removal). journal may be None if the journal file was lost;
        # reconstruct the minimal record from session metadata.
        if journal is None:
            journal = {"replaced_size": replaced}
        self._finalize_commit_locked(state, journal)

    # ==================================================================
    # Session lifecycle
    # ==================================================================
    def create_session(
        self,
        tenant: str,
        object_name: str,
        total_size: int,
        chunk_size: int | None = None,
        whole_sha256: str | None = None,
        session_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> SessionState:
        from .storage import validate_object_name

        try:
            validate_object_name(object_name)
        except ValueError as exc:
            raise InvalidRequest(str(exc)) from exc
        if not isinstance(total_size, int) or total_size < 0:
            raise InvalidRequest("total_size must be a non-negative integer")
        cs = chunk_size or DEFAULT_CHUNK_SIZE
        if not isinstance(cs, int) or cs <= 0:
            raise InvalidRequest("chunk_size must be a positive integer")
        if whole_sha256 is not None and (
            not isinstance(whole_sha256, str) or len(whole_sha256) != 64
        ):
            raise InvalidRequest("whole_sha256 must be a 64-char hex string")

        with self._lock:
            # Idempotent create: same key returns the original session.
            if idempotency_key is not None:
                existing = self._find_idempotent_locked(tenant, idempotency_key)
                if existing is not None:
                    if (
                        existing.object_name != object_name
                        or existing.total_size != total_size
                    ):
                        raise InvalidRequest(
                            "idempotency key already used with different "
                            "object_name/total_size"
                        )
                    self._expire_if_due_locked(existing)
                    return existing

            active = [
                s
                for (t, _sid), s in self._sessions.items()
                if t == tenant and not s.is_terminal()
            ]
            cap = self.config.max_sessions_per_tenant
            if cap is not None and len(active) >= cap:
                raise QuotaExceeded(
                    f"tenant {tenant!r} exceeded max sessions {cap}"
                )

            # Quota is checked/reserved BEFORE any directory or metadata is
            # written, so rejection leaves no orphan bytes or metadata.
            self.quota.reserve(tenant, total_size)

            sid = session_id or uuid.uuid4().hex
            if (tenant, sid) in self._sessions:
                raise InvalidRequest(f"session {sid!r} already exists")
            now = time.time()
            self._seq += 1
            state = SessionState(
                session_id=sid,
                tenant=tenant,
                object_name=object_name,
                total_size=total_size,
                chunk_size=cs,
                status=SessionStatus.INIT,
                created_at=now,
                updated_at=now,
                expires_at=now + self.config.session_ttl_seconds,
                whole_sha256=whole_sha256,
                create_seq=self._seq,
                idempotency_key=idempotency_key,
            )
            try:
                self.storage.ensure_session_dirs(tenant, sid)
                self.session_store.save(state)
            except BaseException:
                # Roll back reservation if metadata could not be persisted.
                self.quota.release(tenant, total_size)
                raise
            self._sessions[(tenant, sid)] = state
            return state

    def get_session(self, tenant: str, session_id: str) -> SessionState:
        with self._lock:
            state = self._require_session_locked(tenant, session_id)
            self._expire_if_due_locked(state)
            return state

    def abort_session(self, tenant: str, session_id: str) -> None:
        with self._lock:
            state = self._sessions.get((tenant, session_id))
            # Already discarded (canceled/expired/committed-and-cleaned):
            # abort is idempotent when there is nothing left to do.
            if state is None:
                return
            if state.status is SessionStatus.COMPLETED:
                raise SessionCompleted("cannot abort a completed session")
            if state.status in (
                SessionStatus.CANCELED,
                SessionStatus.EXPIRED,
            ):
                return
            if state.status is SessionStatus.COMMITTING:
                raise SessionClosed("session is committing; try again shortly")
            self._discard_session_locked(state, SessionStatus.CANCELED)

    # ---- internal session helpers ------------------------------------
    def _require_session_locked(
        self, tenant: str, session_id: str
    ) -> SessionState:
        state = self._sessions.get((tenant, session_id))
        if state is None:
            raise SessionNotFound(session_id)
        return state

    def _find_idempotent_locked(
        self, tenant: str, key: str
    ) -> SessionState | None:
        for (t, _sid), s in self._sessions.items():
            if t == tenant and s.idempotency_key == key:
                return s
        return None

    def _expire_if_due_locked(self, state: SessionState) -> None:
        if state.is_terminal() or state.status is SessionStatus.COMMITTING:
            return
        if time.time() >= state.expires_at:
            self._discard_session_locked(state, SessionStatus.EXPIRED)
            raise SessionExpired(state.session_id)

    def _discard_session_locked(
        self, state: SessionState, status: SessionStatus
    ) -> None:
        """Mark a session CANCELED/EXPIRED, free its reservation and files."""
        state.status = status
        state.updated_at = time.time()
        # Metadata tombstone is written first, then data is removed.
        self.session_store.save(state)
        self.quota.release(state.tenant, state.total_size)
        self.session_store.delete(state.tenant, state.session_id)
        self.storage.remove_staging(state.tenant, state.session_id)
        self._sessions.pop((state.tenant, state.session_id), None)

    def _object_lock(self, tenant: str, object_name: str) -> threading.Lock:
        with self._lock:
            lock = self._commit_locks.get((tenant, object_name))
            if lock is None:
                lock = threading.Lock()
                self._commit_locks[(tenant, object_name)] = lock
            return lock

    @staticmethod
    def _silent_remove(path: str) -> None:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass

    # ==================================================================
    # Chunk submission
    # ==================================================================
    def _chunk_offset(self, state: SessionState, number: int) -> int:
        return number * state.chunk_size

    def _expected_chunk_size(self, state: SessionState, number: int) -> int:
        offset = self._chunk_offset(state, number)
        if state.total_chunks == 0:
            return 0
        if number == state.total_chunks - 1:
            return state.total_size - offset
        return state.chunk_size

    def put_chunk(
        self,
        tenant: str,
        session_id: str,
        number: int,
        stream,
        declared_sha256: str,
        declared_size: int | None = None,
    ) -> SessionState:
        if not isinstance(number, int) or number < 0:
            raise InvalidRequest("chunk number must be a non-negative integer")
        if not declared_sha256 or len(declared_sha256) != 64:
            raise InvalidRequest("declared_sha256 must be 64 hex chars")

        with self._lock:
            state = self._require_session_locked(tenant, session_id)
            self._expire_if_due_locked(state)

            # Late writes against a closed session are rejected explicitly.
            if state.status is SessionStatus.COMMITTING:
                raise SessionClosed("session is committing")
            if state.status is SessionStatus.COMPLETED:
                raise SessionCompleted("session already completed")
            if state.status is SessionStatus.CANCELED:
                raise SessionCanceled("session was canceled")
            if state.status is SessionStatus.EXPIRED:
                raise SessionExpired("session expired")

            if number >= state.total_chunks:
                raise ChunkOutOfRange(
                    f"chunk {number} exceeds total {state.total_chunks}"
                )

            expected_size = self._expected_chunk_size(state, number)
            if declared_size is not None and declared_size != expected_size:
                raise ChunkSizeMismatch(
                    f"declared size {declared_size} != expected {expected_size}"
                )

            # Duplicate / retry: a chunk already acknowledged is immutable.
            if number in state.chunks:
                known = state.chunks[number]
                if known.sha256 == declared_sha256:
                    # Idempotent ack: still drain/validate the resend bytes so
                    # a corrupt retry cannot be silently treated as success.
                    self._verify_resend(state, number, stream, declared_sha256)
                    state.touch(time.time(), self.config.session_ttl_seconds)
                    self.session_store.save(state)
                    return state
                raise InvalidRequest(
                    f"chunk {number} already acknowledged with a different "
                    "checksum; acknowledged chunks are immutable"
                )

            # Stream to a temp file with bounded memory, hashing as we go.
            final_path, written = self._stream_and_hash(
                state, number, stream, declared_sha256, expected_size
            )

            info = ChunkInfo(
                number=number,
                offset=self._chunk_offset(state, number),
                size=written,
                sha256=declared_sha256,
                filename=os.path.basename(final_path),
            )
            state.chunks[number] = info
            state.status = SessionStatus.IN_PROGRESS
            state.touch(time.time(), self.config.session_ttl_seconds)
            # Metadata durable before the chunk is considered acknowledged.
            self.session_store.save(state)
            return state

    def _stream_and_hash(
        self, state, number, stream, declared_sha256, expected_size
    ) -> tuple[str, int]:
        final = self.storage.chunk_path(state.tenant, state.session_id, number)
        os.makedirs(os.path.dirname(final), exist_ok=True)
        tmp = final + f".recv.{os.getpid()}"
        hasher = hashlib.sha256()
        total = 0
        try:
            with open(tmp, "wb") as out:
                while True:
                    buf = stream.read(self.config.read_buffer_size)
                    if not buf:
                        break
                    total += len(buf)
                    if total > expected_size:
                        raise ChunkSizeMismatch(
                            f"chunk {number} larger than expected "
                            f"{expected_size} bytes"
                        )
                    hasher.update(buf)
                    out.write(buf)
                out.flush()
                os.fsync(out.fileno())
            if total != expected_size:
                raise ChunkSizeMismatch(
                    f"chunk {number}: got {total} bytes, expected {expected_size}"
                )
            actual = hasher.hexdigest()
            if actual != declared_sha256:
                raise ChunkChecksumMismatch(
                    f"chunk {number}: declared {declared_sha256} actual {actual}"
                )
            # Only after full validation do we publish the temp chunk, and
            # even then it lands under a unique-new number slot (duplicates
            # were handled earlier), so prior data can never be clobbered by
            # a failed/resend attempt.
            os.replace(tmp, final)
            self.storage.fsync_parent(final)
        except BaseException:
            self._silent_remove(tmp)
            raise
        return final, total

    def _verify_resend(self, state, number, stream, declared_sha256) -> None:
        """Validate a duplicate resend's bytes without touching stored data."""
        hasher = hashlib.sha256()
        total = 0
        expected = self._expected_chunk_size(state, number)
        while True:
            buf = stream.read(self.config.read_buffer_size)
            if not buf:
                break
            total += len(buf)
            if total > expected:
                raise ChunkSizeMismatch("duplicate chunk exceeds expected size")
            hasher.update(buf)
        if total != expected:
            raise ChunkSizeMismatch(
                f"duplicate chunk {number}: got {total}, expected {expected}"
            )
        if hasher.hexdigest() != declared_sha256:
            raise ChunkChecksumMismatch(
                f"duplicate chunk {number} checksum differs from its own upload"
            )

    # ==================================================================
    # Commit
    # ==================================================================
    def commit_session(self, tenant: str, session_id: str) -> SessionState:
        # The per-object lock is acquired BEFORE flipping status, so the whole
        # "decide winner -> assemble -> publish" sequence for one object name
        # is serialized. Two racing commits therefore observe stable state:
        # the loser is still present when its turn comes and is rejected with
        # a deterministic 409 rather than a racy 404.
        with self._lock:
            state = self._require_session_locked(tenant, session_id)
            object_lock = self._object_lock(tenant, state.object_name)

        with object_lock:
            with self._lock:
                state = self._require_session_locked(tenant, session_id)
                self._expire_if_due_locked(state)
                if state.status is SessionStatus.COMPLETED:
                    return state
                if state.status in (
                    SessionStatus.CANCELED,
                    SessionStatus.EXPIRED,
                ):
                    raise SessionClosed("session is no longer committable")
                if state.status is SessionStatus.COMMITTING:
                    raise SessionClosed("commit already in progress")

                missing = state.missing_chunks()
                if missing:
                    raise MissingChunks(missing)
                if state.received_bytes() != state.total_size:
                    raise TotalSizeMismatch(
                        f"received {state.received_bytes()} != "
                        f"declared total {state.total_size}"
                    )

                state.status = SessionStatus.COMMITTING
                state.updated_at = time.time()
                self.session_store.save(state)

            return self._commit_under_object_lock(tenant, session_id)

    def _commit_under_object_lock(
        self, tenant: str, session_id: str
    ) -> SessionState:
        with self._lock:
            state = self._require_session_locked(tenant, session_id)
            if state.status is not SessionStatus.COMMITTING:
                if state.status is SessionStatus.COMPLETED:
                    return state
                raise SessionClosed("session no longer committing")

            exists = self.storage.object_exists(tenant, state.object_name)
            # Defensive high-water mark: if an equally-or-younger session has
            # already published this object, this stale session must lose,
            # regardless of current in-memory peer state.
            published = self._published_seq.get(
                (tenant, state.object_name)
            )
            if published is not None and state.create_seq <= published:
                self._discard_session_locked(state, SessionStatus.CANCELED)
                raise ObjectAlreadyExists(
                    f"object {state.object_name!r}: session {session_id} is "
                    f"older than the already-published version (seq "
                    f"{state.create_seq} <= {published})"
                )
            # Policy check: on conflict the losing session is deterministically
            # discarded so it leaves no files or quota reservation behind.
            if exists and self.config.overwrite_policy == "never":
                self._discard_session_locked(state, SessionStatus.CANCELED)
                raise ObjectAlreadyExists(
                    f"object {state.object_name!r} already exists and "
                    "overwrite_policy='never'"
                )
            # Find competing live sessions for the same object. Under
            # "always", the session with the greatest create_seq wins; a
            # younger live session makes this session a defined loser.
            winner = self._select_winner_locked(state)
            if winner is not state:
                winner_id = winner.session_id
                self._discard_session_locked(state, SessionStatus.CANCELED)
                raise ObjectAlreadyExists(
                    f"object {state.object_name!r}: session {session_id} lost "
                    f"the commit race to newer session {winner_id}"
                )

            replaced = (
                self.storage.stat_object(tenant, state.object_name).size
                if exists
                else 0
            )
            journal = {
                "session_id": session_id,
                "tenant": tenant,
                "object_name": state.object_name,
                "total_size": state.total_size,
                "whole_sha256": state.whole_sha256,
                "replaced_size": replaced,
                "started_at": time.time(),
            }
            self.storage.write_atomic(
                self.storage.journal_path(tenant, session_id),
                json.dumps(journal, sort_keys=True).encode("utf-8"),
            )
            state.committed_path = self.storage.object_path(
                tenant, state.object_name
            )
            self.session_store.save(state)

        # Heavy IO outside the global lock; other objects remain fully
        # serviceable while this object is assembled. A checksum/IO failure
        # rolls the session back to in_progress (chunks intact) and a crash
        # here is reconciled identically by startup recovery.
        try:
            self._assemble_and_publish(state, replaced, journal)
        except BaseException:
            with self._lock:
                self._rollback_commit_locked(state)
            raise
        with self._lock:
            self._finalize_commit_locked(state, journal)
            return state

    def _select_winner_locked(self, state: SessionState) -> SessionState:
        """Return the session that should win the object.

        Candidates are non-terminal sessions (incl. committing/completed for
        the same object). Greatest ``create_seq`` wins deterministically.
        """
        candidates = [
            s
            for (t, _sid), s in self._sessions.items()
            if t == state.tenant
            and s.object_name == state.object_name
            and s.status
            in (
                SessionStatus.INIT,
                SessionStatus.IN_PROGRESS,
                SessionStatus.COMMITTING,
            )
            and s.session_id != state.session_id
        ]
        for other in candidates:
            if other.create_seq > state.create_seq:
                return other
        return state

    def _assemble_and_publish(
        self, state: SessionState, replaced_size: int, journal_hint=None
    ) -> None:
        """Stream chunks -> staging file, verify, atomically rename."""
        tenant = state.tenant
        staging = self.storage.staging_path(tenant, state.session_id)
        os.makedirs(os.path.dirname(staging), exist_ok=True)
        whole = hashlib.sha256()
        size = 0
        with open(staging, "wb") as out:
            for i in range(state.total_chunks):
                info = state.chunks.get(i)
                if info is None:
                    raise MissingChunks(state.missing_chunks())
                cpath = self.storage.chunk_path(
                    tenant, state.session_id, i
                )
                with open(cpath, "rb") as part:
                    while True:
                        buf = part.read(self.config.read_buffer_size)
                        if not buf:
                            break
                        whole.update(buf)
                        out.write(buf)
                        size += len(buf)
            out.flush()
            os.fsync(out.fileno())

        if size != state.total_size:
            self._silent_remove(staging)
            raise TotalSizeMismatch(
                f"assembled {size} != total {state.total_size}"
            )
        if state.whole_sha256 and whole.hexdigest() != state.whole_sha256:
            self._silent_remove(staging)
            raise WholeObjectChecksumMismatch(
                "assembled object checksum does not match whole_sha256"
            )

        target = self.storage.object_path(tenant, state.object_name)
        # The single atomic visibility switch. Readers either see the old
        # version (opened before rename) or the new one, never a mixture.
        self.storage.atomic_replace(staging, target)

    def _finalize_commit_locked(self, state: SessionState, journal: dict) -> None:
        state.status = SessionStatus.COMPLETED
        state.updated_at = time.time()
        state.committed_path = self.storage.object_path(
            state.tenant, state.object_name
        )
        self.session_store.save(state)
        self.quota.commit(state.tenant, state.total_size, journal.get(
            "replaced_size", 0
        ))
        self._published_seq[(state.tenant, state.object_name)] = state.create_seq
        # Session data removed only after the object is durably visible.
        self._silent_remove(
            self.storage.journal_path(state.tenant, state.session_id)
        )
        self.session_store.delete(state.tenant, state.session_id)
        self._sessions.pop((state.tenant, state.session_id), None)
        # Discard still-open (init/in_progress) competing sessions for this
        # object: once a newer/dominant session publishes, they can never
        # win. Sessions already COMMITTING are serialized on this same object
        # lock and handled deterministically on their own turn (left alone
        # here so they get a proper conflict decision instead of a 404).
        losers = [
            s
            for (t, sid), s in list(self._sessions.items())
            if t == state.tenant
            and s.object_name == state.object_name
            and sid != state.session_id
            and s.status in (SessionStatus.INIT, SessionStatus.IN_PROGRESS)
        ]
        for loser in losers:
            self._discard_session_locked(
                loser,
                SessionStatus.CANCELED,
            )

    def _rollback_commit_locked(self, state: SessionState) -> None:
        state.status = SessionStatus.IN_PROGRESS
        state.touch(time.time(), self.config.session_ttl_seconds)
        self.session_store.save(state)
        self.storage.remove_staging(state.tenant, state.session_id)
        self._silent_remove(
            self.storage.journal_path(state.tenant, state.session_id)
        )

    # ==================================================================
    # Object reads
    # ==================================================================
    def head_object(self, tenant: str, object_name: str):
        """Return object metadata.

        While a commit is in progress the *previously committed* version is
        reported (the staging file is never visible). If no version exists
        yet, :class:`SessionNotFound` (404) is raised rather than waiting.
        """
        from .storage import validate_object_name

        try:
            validate_object_name(object_name)
        except ValueError as exc:
            raise InvalidRequest(str(exc)) from exc
        if not self.storage.object_exists(tenant, object_name):
            raise SessionNotFound(f"object {object_name!r} not found")
        return self.storage.stat_object(tenant, object_name)

    def open_object(self, tenant: str, object_name: str):
        """Open the current visible object for streaming read.

        The file handle is opened before the caller starts reading; because
        visibility changes via ``os.replace`` (never in-place writes), a
        reader always observes exactly one complete version.
        """
        self.head_object(tenant, object_name)  # validates + 404 semantics
        return self.storage.open_object(tenant, object_name)

    def read_object(self, tenant: str, object_name: str) -> bytes:
        with self.open_object(tenant, object_name) as f:
            return f.read()

    # ==================================================================
    # Expiration / reaping
    # ==================================================================
    def reap_expired(self, now: float | None = None) -> list[str]:
        """Cancel all unfinished sessions past their expiry.

        Committed/in-progress (non-expired) sessions are never touched.
        Returns the list of reaped session IDs.
        """
        now = time.time() if now is None else now
        reaped: list[str] = []
        with self._lock:
            for (_t, sid), state in list(self._sessions.items()):
                if state.is_terminal() or state.status is SessionStatus.COMMITTING:
                    continue
                if now >= state.expires_at:
                    self._discard_session_locked(state, SessionStatus.EXPIRED)
                    reaped.append(sid)
        return reaped

    def _reap_loop(self) -> None:
        interval = self.config.reap_interval_seconds
        while not self._stopping.wait(interval):
            try:
                self.reap_expired()
            except Exception:  # pragma: no cover - reaper must never die
                pass

    def recover(self) -> dict:
        """Public recovery entry point (also invoked by :meth:`start`)."""
        self.storage.ensure_layout()
        self._recover()
        with self._lock:
            return {
                "sessions_loaded": len(self._sessions),
                "tenants": sorted({t for (t, _s) in self._sessions}),
            }
