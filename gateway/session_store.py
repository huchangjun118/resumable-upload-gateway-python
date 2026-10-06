"""Durable session metadata store (JSON files + atomic writes)."""

from __future__ import annotations

import json
import os

from .errors import SessionNotFound
from .models import SessionState
from .storage import LocalStorage


class SessionStore:
    def __init__(self, storage: LocalStorage) -> None:
        self.storage = storage

    def path(self, tenant: str, session_id: str) -> str:
        return self.storage.meta_path(tenant, session_id)

    def exists(self, tenant: str, session_id: str) -> bool:
        return os.path.isfile(self.path(tenant, session_id))

    def save(self, state: SessionState) -> None:
        data = json.dumps(state.to_dict(), sort_keys=True).encode("utf-8")
        self.storage.write_atomic(self.path(state.tenant, state.session_id), data)

    def load(self, tenant: str, session_id: str) -> SessionState:
        state = self.try_load(tenant, session_id)
        if state is None:
            raise SessionNotFound(session_id)
        return state

    def try_load(self, tenant: str, session_id: str) -> SessionState | None:
        path = self.path(tenant, session_id)
        try:
            with open(path, "rb") as f:
                raw = f.read()
        except FileNotFoundError:
            return None
        return SessionState.from_dict(json.loads(raw.decode("utf-8")))

    def delete(self, tenant: str, session_id: str) -> None:
        self.storage.remove_session_tree(tenant, session_id)

    def iter_all(self):
        for tenant, sid in self.storage.list_sessions():
            try:
                yield self.load(tenant, sid)
            except SessionNotFound:
                continue
