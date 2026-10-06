"""Gateway configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass

_VALID_POLICIES = ("never", "always")


@dataclass(frozen=True)
class Config:
    # Root directory for all on-disk state (sessions + committed objects).
    data_dir: str = "./data"
    # Session retention: seconds of inactivity before an unfinished session
    # becomes eligible for reaping.
    session_ttl_seconds: float = 24 * 3600.0
    # Reaper sweep interval in seconds; 0 disables the background reaper.
    reap_interval_seconds: float = 300.0
    # Per-tenant storage quota in bytes (sum of committed objects + live
    # sessions). None means unlimited.
    default_quota_bytes: int | None = None
    # Overwrite policy for concurrent sessions targeting the same object:
    #   "never"  - first commit wins, later commits get ObjectAlreadyExists
    #   "always" - session created last wins; older sessions lose at commit
    overwrite_policy: str = "always"
    # Read buffer size for streaming chunks; bounds per-request memory.
    read_buffer_size: int = 64 * 1024
    # Per-tenant cap on concurrently active upload sessions.
    max_sessions_per_tenant: int | None = None

    def __post_init__(self) -> None:
        if self.overwrite_policy not in _VALID_POLICIES:
            raise ValueError(
                f"invalid overwrite_policy {self.overwrite_policy!r}; "
                f"expected one of {_VALID_POLICIES}"
            )
        if self.session_ttl_seconds <= 0:
            raise ValueError("session_ttl_seconds must be positive")
        if self.read_buffer_size <= 0:
            raise ValueError("read_buffer_size must be positive")

    @staticmethod
    def from_env(prefix: str = "UGW_") -> "Config":
        def opt(name: str) -> str | None:
            return os.environ.get(prefix + name)

        def as_int(value: str | None, default: int | None) -> int | None:
            if value is None or value == "":
                return default
            return int(value)

        def as_float(value: str | None, default: float) -> float:
            return float(value) if value is not None else default

        return Config(
            data_dir=opt("DATA_DIR") or "./data",
            session_ttl_seconds=as_float(
                opt("SESSION_TTL_SECONDS"), 24 * 3600.0
            ),
            reap_interval_seconds=as_float(
                opt("REAP_INTERVAL_SECONDS"), 300.0
            ),
            default_quota_bytes=as_int(opt("DEFAULT_QUOTA_BYTES"), None),
            overwrite_policy=opt("OVERWRITE_POLICY") or "always",
            read_buffer_size=as_int(opt("READ_BUFFER_SIZE"), 64 * 1024) or 64 * 1024,
            max_sessions_per_tenant=as_int(
                opt("MAX_SESSIONS_PER_TENANT"), None
            ),
        )


def load_config() -> Config:
    return Config.from_env()
