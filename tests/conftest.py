"""Shared fixtures and helpers for gateway tests."""

from __future__ import annotations

import hashlib
import io
import os

import pytest

from gateway.config import Config
from gateway.manager import GatewayService


@pytest.fixture
def config(tmp_path):
    return Config(
        data_dir=str(tmp_path / "data"),
        session_ttl_seconds=3600,
        reap_interval_seconds=0,  # deterministic tests; no background reaper
    )


@pytest.fixture
def service(config):
    svc = GatewayService(config)
    svc.start()
    try:
        yield svc
    finally:
        svc.shutdown()


def make_data(total: int, seed: int = 0) -> bytes:
    """Deterministic, non-repeating-ish bytes for a payload."""
    out = bytearray(total)
    x = 0x9E3779B97F4A7C15 ^ seed
    for i in range(total):
        x ^= (x << 13) & 0xFFFFFFFFFFFFFFFF
        x ^= x >> 7
        x ^= (x << 17) & 0xFFFFFFFFFFFFFFFF
        x &= 0xFFFFFFFFFFFFFFFF
        out[i] = (x ^ (x >> 32) ^ i) & 0xFF
    return bytes(out)


def split_chunks(data: bytes, chunk_size: int):
    return [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def upload_session(svc, tenant, object_name, data, chunk_size, order=None):
    """Create session + upload chunks (optionally out of order) + return it."""
    pieces = split_chunks(data, chunk_size)
    st = svc.create_session(
        tenant, object_name, len(data), chunk_size=chunk_size,
        whole_sha256=sha256_hex(data),
    )
    indices = list(range(len(pieces))) if order is None else order
    for n in indices:
        svc.put_chunk(
            tenant, st.session_id, n,
            io.BytesIO(pieces[n]), sha256_hex(pieces[n]),
        )
    return st
