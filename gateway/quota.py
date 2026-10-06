"""Per-tenant quota accounting.

Usage = committed object bytes + bytes reserved by live sessions.
Accounting state is persisted per tenant so a crash cannot leak reservations;
:meth:`QuotaManager.rebuild` reconciles it from on-disk reality at startup.
"""

from __future__ import annotations

import json
import os
import threading

from .errors import QuotaExceeded
from .storage import LocalStorage


class QuotaManager:
    def __init__(
        self,
        storage: LocalStorage,
        default_limit: int | None = None,
        limits: dict[str, int | None] | None = None,
    ) -> None:
        self.storage = storage
        self.default_limit = default_limit
        self.limits = dict(limits or {})
        self._lock = threading.RLock()
        self._cache: dict[str, dict[str, int]] = {}

    def limit_for(self, tenant: str) -> int | None:
        return self.limits.get(tenant, self.default_limit)

    def _load(self, tenant: str) -> dict[str, int]:
        if tenant in self._cache:
            return self._cache[tenant]
        entry = {"committed": 0, "reserved": 0}
        path = self.storage.quota_path(tenant)
        if os.path.isfile(path):
            with open(path, "rb") as f:
                raw = json.loads(f.read().decode("utf-8"))
            entry["committed"] = int(raw.get("committed", 0))
            entry["reserved"] = int(raw.get("reserved", 0))
        self._cache[tenant] = entry
        return entry

    def _persist(self, tenant: str, entry: dict[str, int]) -> None:
        data = json.dumps(entry, sort_keys=True).encode("utf-8")
        self.storage.write_atomic(self.storage.quota_path(tenant), data)

    def usage(self, tenant: str) -> int:
        with self._lock:
            e = self._load(tenant)
            return e["committed"] + e["reserved"]

    def breakdown(self, tenant: str) -> tuple[int, int]:
        with self._lock:
            e = self._load(tenant)
            return e["committed"], e["reserved"]

    def reserve(self, tenant: str, amount: int) -> None:
        """Reserve ``amount`` bytes.

        Raises :class:`QuotaExceeded` *before* any state is mutated, so a
        rejected upload never consumes space or metadata.
        """
        if amount < 0:
            raise ValueError("amount must be non-negative")
        with self._lock:
            e = self._load(tenant)
            limit = self.limit_for(tenant)
            current = e["committed"] + e["reserved"]
            if limit is not None and current + amount > limit:
                raise QuotaExceeded(
                    f"tenant {tenant!r} quota {limit} exceeded: "
                    f"used={current}, requested={amount}"
                )
            e["reserved"] += amount
            self._persist(tenant, e)

    def release(self, tenant: str, amount: int) -> None:
        with self._lock:
            e = self._load(tenant)
            e["reserved"] = max(0, e["reserved"] - amount)
            self._persist(tenant, e)

    def commit(self, tenant: str, amount: int, replaced_size: int = 0) -> None:
        """Convert a reservation into committed usage.

        ``reserved`` drops by ``amount``; ``committed`` grows by
        ``amount - replaced_size`` (overwritten object bytes are reclaimed).
        """
        with self._lock:
            e = self._load(tenant)
            e["reserved"] = max(0, e["reserved"] - amount)
            e["committed"] = max(0, e["committed"] + amount - replaced_size)
            self._persist(tenant, e)

    def remove_committed(self, tenant: str, amount: int) -> None:
        with self._lock:
            e = self._load(tenant)
            e["committed"] = max(0, e["committed"] - amount)
            self._persist(tenant, e)

    def rebuild(self, tenant: str, committed: int, reserved: int) -> None:
        with self._lock:
            entry = {"committed": int(committed), "reserved": int(reserved)}
            self._cache[tenant] = entry
            self._persist(tenant, entry)
