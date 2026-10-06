"""TTL expiration and retention reaping (requirement 6)."""

from __future__ import annotations

import os
import time

import pytest

from gateway.config import Config
from gateway.errors import SessionExpired
from gateway.manager import GatewayService
from gateway.models import SessionStatus

from .conftest import make_data, sha256_hex, split_chunks, upload_session


def test_reaper_only_removes_expired_sessions(tmp_path):
    cfg = Config(
        data_dir=str(tmp_path / "data"),
        session_ttl_seconds=100,
        reap_interval_seconds=0,
    )
    svc = GatewayService(cfg)
    svc.start()

    fresh = svc.create_session("t", "fresh", 100)
    old = svc.create_session("t", "old", 200)
    # Force the old session past expiry.
    old.expires_at = time.time() - 1

    reaped = svc.reap_expired()
    assert set(reaped) == {old.session_id}

    # Fresh session still usable; committed objects untouched.
    assert svc.get_session("t", fresh.session_id).total_size == 100
    data = make_data(500, seed=7)
    st = upload_session(svc, "t", "kept", data, 200)
    svc.commit_session("t", st.session_id)
    assert svc.read_object("t", "kept") == data
    assert svc.quota.usage("t") == 600  # 100 reserved fresh + 500 committed


def test_expired_session_rejects_late_chunk_and_get(tmp_path):
    cfg = Config(data_dir=":memory:" if False else str(tmp_path / "d"),
                 session_ttl_seconds=10, reap_interval_seconds=0)
    svc = GatewayService(cfg)
    svc.start()
    st = svc.create_session("t", "o", 100)
    st.expires_at = time.time() - 1
    import io
    with pytest.raises(SessionExpired):
        svc.get_session("t", st.session_id)
    # get_session already expired+reaped it; a late chunk is a defined 404.
    from gateway.errors import SessionNotFound
    with pytest.raises(SessionNotFound):
        svc.put_chunk("t", st.session_id, 0, io.BytesIO(b"x"),
                      sha256_hex(b"x"))
    # Nothing reserved afterward.
    assert svc.quota.usage("t") == 0
    svc.shutdown()


def test_activity_slides_expiry(tmp_path):
    import io
    cfg = Config(data_dir=str(tmp_path / "d"),
                 session_ttl_seconds=100, reap_interval_seconds=0)
    svc = GatewayService(cfg)
    svc.start()
    data = make_data(400)
    pieces = split_chunks(data, 200)
    st = svc.create_session("t", "o", 400, chunk_size=200)
    # Simulate most of the TTL elapsing; an active upload slides expiry.
    st.expires_at = time.time() + 1
    svc.put_chunk("t", st.session_id, 0, io.BytesIO(pieces[0]),
                  sha256_hex(pieces[0]))
    refreshed = svc.get_session("t", st.session_id)
    assert refreshed.expires_at > time.time() + 50
    svc.shutdown()
