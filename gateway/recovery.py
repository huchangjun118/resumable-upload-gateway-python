"""Crash recovery entry point.

On startup the gateway scans persisted sessions and reconciles state. The
actual logic lives on :class:`~gateway.manager.GatewayService` (it needs the
service's locks and indexes); this module keeps the documented recovery
workflow importable and testable on its own.
"""

from __future__ import annotations

from .manager import GatewayService


def recover(service: GatewayService) -> dict:
    """Run recovery against the on-disk state behind ``service``."""
    return service.recover()
