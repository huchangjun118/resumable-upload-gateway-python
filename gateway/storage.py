"""On-disk storage layout and low-level primitives.

Layout (all under ``Config.data_dir``)::

    sessions/<tenant>/<session_id>/meta.json
    sessions/<tenant>/<session_id>/journal
    sessions/<tenant>/<session_id>/chunks/NNNNNN
    objects/<tenant>/<object-path>
    objects/<tenant>/.staging/<session_id>.tmp
    quota/<tenant>.json

Object visibility switches with a single ``os.replace`` of a fully fsynced
staging file; readers therefore never observe a partially written object.
"""

from __future__ import annotations

import os
import re
import shutil
from dataclasses import dataclass

_SAFE_NAME = re.compile(r"^[A-Za-z0-9._-]+$")


def _validate_path_segment(value: str, what: str) -> str:
    if not value or value in (".", ".."):
        raise ValueError(f"invalid {what}: {value!r}")
    if not _SAFE_NAME.match(value):
        raise ValueError(f"invalid {what}: {value!r}")
    return value


def validate_object_name(name: str) -> str:
    """Object names may be nested paths, but must stay inside the tenant root."""
    if not name or name.startswith("/"):
        raise ValueError(f"invalid object name: {name!r}")
    parts = name.split("/")
    for part in parts:
        if not part or part in (".", ".."):
            raise ValueError(f"invalid object name: {name!r}")
    return name


@dataclass(frozen=True)
class StoredObject:
    tenant: str
    object_name: str
    path: str
    size: int
    sha256: str | None = None


class LocalStorage:
    """Low-level, policy-free filesystem primitives."""

    def __init__(self, data_dir: str, read_buffer_size: int = 64 * 1024) -> None:
        self.data_dir = os.path.abspath(data_dir)
        self.read_buffer_size = read_buffer_size

    # ---- path helpers -------------------------------------------------
    def _tenant(self, tenant: str) -> str:
        return _validate_path_segment(tenant, "tenant")

    def sessions_root(self) -> str:
        return os.path.join(self.data_dir, "sessions")

    def objects_root(self) -> str:
        return os.path.join(self.data_dir, "objects")

    def quota_root(self) -> str:
        return os.path.join(self.data_dir, "quota")

    def session_dir(self, tenant: str, session_id: str) -> str:
        t = self._tenant(tenant)
        sid = _validate_path_segment(session_id, "session_id")
        return os.path.join(self.sessions_root(), t, sid)

    def meta_path(self, tenant: str, session_id: str) -> str:
        return os.path.join(self.session_dir(tenant, session_id), "meta.json")

    def journal_path(self, tenant: str, session_id: str) -> str:
        return os.path.join(self.session_dir(tenant, session_id), "journal")

    def chunks_dir(self, tenant: str, session_id: str) -> str:
        return os.path.join(self.session_dir(tenant, session_id), "chunks")

    def chunk_path(self, tenant: str, session_id: str, number: int) -> str:
        if number < 0:
            raise ValueError("chunk number must be >= 0")
        return os.path.join(
            self.chunks_dir(tenant, session_id), f"{number:08d}"
        )

    def object_path(self, tenant: str, object_name: str) -> str:
        t = self._tenant(tenant)
        validate_object_name(object_name)
        return os.path.join(self.objects_root(), t, object_name)

    def staging_dir(self, tenant: str) -> str:
        return os.path.join(self.objects_root(), self._tenant(tenant), ".staging")

    def staging_path(self, tenant: str, session_id: str) -> str:
        sid = _validate_path_segment(session_id, "session_id")
        return os.path.join(self.staging_dir(tenant), f"{sid}.tmp")

    def quota_path(self, tenant: str) -> str:
        return os.path.join(self.quota_root(), f"{self._tenant(tenant)}.json")

    # ---- fs helpers ----------------------------------------------------
    def ensure_layout(self) -> None:
        for d in (
            self.sessions_root(),
            self.objects_root(),
            self.quota_root(),
        ):
            os.makedirs(d, exist_ok=True)

    def ensure_session_dirs(self, tenant: str, session_id: str) -> None:
        os.makedirs(self.chunks_dir(tenant, session_id), exist_ok=True)

    def fsync_parent(self, path: str) -> None:
        parent = os.path.dirname(os.path.abspath(path)) or "."
        fd = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def write_atomic(self, path: str, data: bytes) -> None:
        """Durably write a small file (metadata/journal) via temp + rename."""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        self.fsync_parent(path)

    def write_chunk(self, tenant, session_id, number, src_fileobj) -> tuple[str, int]:
        """Stream ``src_fileobj`` to a temp chunk file, then atomically rename.

        Returns (final_path, bytes_written). The caller verifies checksum and
        size *after* this returns; on failure the temp file is discarded so a
        bad resend can never corrupt a previously acknowledged chunk.
        """
        final = self.chunk_path(tenant, session_id, number)
        os.makedirs(os.path.dirname(final), exist_ok=True)
        tmp = final + f".tmp.{os.getpid()}"
        total = 0
        try:
            with open(tmp, "wb") as out:
                while True:
                    buf = src_fileobj.read(self.read_buffer_size)
                    if not buf:
                        break
                    out.write(buf)
                    total += len(buf)
                out.flush()
                os.fsync(out.fileno())
            os.replace(tmp, final)
            self.fsync_parent(final)
        except BaseException:
            self._silent_unlink(tmp)
            raise
        return final, total

    def atomic_replace(self, src: str, dst: str) -> None:
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        os.replace(src, dst)
        self.fsync_parent(dst)

    def remove_session_tree(self, tenant: str, session_id: str) -> None:
        path = self.session_dir(tenant, session_id)
        shutil.rmtree(path, ignore_errors=True)
        # Best-effort cleanup of now-empty tenant dir.
        try:
            os.rmdir(os.path.dirname(path))
        except OSError:
            pass

    def remove_staging(self, tenant: str, session_id: str) -> None:
        self._silent_unlink(self.staging_path(tenant, session_id))

    def object_exists(self, tenant: str, object_name: str) -> bool:
        return os.path.isfile(self.object_path(tenant, object_name))

    def open_object(self, tenant: str, object_name: str):
        return open(self.object_path(tenant, object_name), "rb")

    def stat_object(self, tenant: str, object_name: str) -> StoredObject:
        st = os.stat(self.object_path(tenant, object_name))
        return StoredObject(
            tenant=tenant,
            object_name=object_name,
            path=self.object_path(tenant, object_name),
            size=st.st_size,
        )

    def list_sessions(self) -> list[tuple[str, str]]:
        root = self.sessions_root()
        found: list[tuple[str, str]] = []
        if not os.path.isdir(root):
            return found
        for tenant in os.listdir(root):
            tdir = os.path.join(root, tenant)
            if not os.path.isdir(tdir):
                continue
            for sid in os.listdir(tdir):
                if os.path.isfile(os.path.join(tdir, sid, "meta.json")):
                    found.append((tenant, sid))
        return found

    def list_objects(self, tenant: str) -> list[tuple[str, int]]:
        base = os.path.join(self.objects_root(), self._tenant(tenant))
        out: list[tuple[str, int]] = []
        if not os.path.isdir(base):
            return out
        for dirpath, dirnames, filenames in os.walk(base):
            if os.path.basename(dirpath) == ".staging":
                dirnames[:] = []
                continue
            for fn in filenames:
                full = os.path.join(dirpath, fn)
                rel = os.path.relpath(full, base)
                out.append((rel.replace(os.sep, "/"), os.path.getsize(full)))
        return out

    @staticmethod
    def _silent_unlink(path: str) -> None:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
