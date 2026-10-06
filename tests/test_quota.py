"""Quota and ownership enforcement (requirement 5)."""

from __future__ import annotations

import io
import os

import pytest

from gateway.config import Config
from gateway.errors import Forbidden, QuotaExceeded
from gateway.manager import GatewayService

from .conftest import make_data, sha256_hex, split_chunks, upload_session


@pytest.fixture
def quota_service(tmp_path):
    cfg = Config(
        data_dir=str(tmp_path / "data"),
        session_ttl_seconds=3600,
        reap_interval_seconds=0,
        default_quota_bytes=10_000,
        max_sessions_per_tenant=3,
    )
    svc = GatewayService(cfg)
    svc.start()
    yield svc
    svc.shutdown()


def test_quota_rejected_before_any_persistent_data(quota_service, tmp_path):
    svc = quota_service
    with pytest.raises(QuotaExceeded):
        svc.create_session("t", "too-big", 10_001)
    # No session directory and no usage recorded at all.
    sroot = svc.storage.sessions_root()
    assert os.listdir(sroot) == []
    assert svc.quota.usage("t") == 0


def test_reservation_release_on_abort(quota_service):
    svc = quota_service
    st = svc.create_session("t", "o1", 6_000, chunk_size=2_000)
    assert svc.quota.usage("t") == 6_000
    # Second 6k session would exceed 10k.
    with pytest.raises(QuotaExceeded):
        svc.create_session("t", "o2", 6_000)
    svc.abort_session("t", st.session_id)
    assert svc.quota.usage("t") == 0
    # Now it fits.
    st2 = svc.create_session("t", "o2", 6_000)
    assert st2.total_size == 6_000


def test_quota_converts_on_commit_and_reclaims_overwrite(quota_service):
    svc = quota_service
    d1 = make_data(4_000, seed=1)
    st1 = upload_session(svc, "t", "o", d1, 2_000)
    svc.commit_session("t", st1.session_id)
    committed, reserved = svc.quota.breakdown("t")
    assert (committed, reserved) == (4_000, 0)

    # Overwrite with a 3k object: committed usage must drop to 3k, old
    # version's bytes reclaimed.
    d2 = make_data(3_000, seed=2)
    st2 = upload_session(svc, "t", "o", d2, 2_000)
    assert svc.quota.usage("t") == 4_000 + 3_000
    svc.commit_session("t", st2.session_id)
    committed, reserved = svc.quota.breakdown("t")
    assert (committed, reserved) == (3_000, 0)
    assert svc.read_object("t", "o") == d2


def test_max_sessions_cap(quota_service):
    svc = quota_service
    svc.create_session("t", "a", 1)
    svc.create_session("t", "b", 1)
    svc.create_session("t", "c", 1)
    with pytest.raises(QuotaExceeded):
        svc.create_session("t", "d", 1)


def test_tenants_are_isolated(quota_service):
    svc = quota_service
    svc.create_session("alice", "a", 8_000)
    # Bob has his own quota bucket.
    svc.create_session("bob", "b", 8_000)
    assert svc.quota.usage("alice") == 8_000
    assert svc.quota.usage("bob") == 8_000
    # Bob cannot read or commit Alice's session.
    from gateway.errors import SessionNotFound
    with pytest.raises(SessionNotFound):
        svc.get_session("bob", svc._sessions[("alice", "a")].session_id
                        if False else [
                            s for (t, s) in svc._sessions if t == "alice"
                        ][0])


def test_idempotent_create_returns_same_session(quota_service):
    svc = quota_service
    s1 = svc.create_session("t", "o", 100, idempotency_key="key-1")
    s2 = svc.create_session("t", "o", 100, idempotency_key="key-1")
    assert s1.session_id == s2.session_id
    # Same key but different parameters is a deterministic conflict.
    from gateway.errors import InvalidRequest
    with pytest.raises(InvalidRequest):
        svc.create_session("t", "other", 100, idempotency_key="key-1")
