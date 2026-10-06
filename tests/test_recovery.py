"""Crash/restart recovery (requirement 6)."""

from __future__ import annotations

import io
import json
import os

from gateway.config import Config
from gateway.manager import GatewayService
from gateway.models import SessionStatus

from .conftest import make_data, sha256_hex, split_chunks


def _svc(tmp_path, quota=None):
    cfg = Config(
        data_dir=str(tmp_path / "data"),
        session_ttl_seconds=3600,
        reap_interval_seconds=0,
        default_quota_bytes=quota,
    )
    return GatewayService(cfg)


def test_committed_objects_and_chunks_survive_restart(tmp_path):
    svc = _svc(tmp_path)
    svc.start()
    data = make_data(9_000, seed=4)
    cs = 2_500
    pieces = split_chunks(data, cs)
    st = svc.create_session("t", "survive", len(data), chunk_size=cs,
                            whole_sha256=sha256_hex(data))
    # Upload only 2 of 4 chunks, then "crash" (no graceful shutdown of state).
    for i in [0, 2]:
        svc.put_chunk("t", st.session_id, i, io.BytesIO(pieces[i]),
                      sha256_hex(pieces[i]))
    sid = st.session_id
    svc.shutdown()

    # New process, same data dir.
    svc2 = _svc(tmp_path)
    svc2.start()
    resumed = svc2.get_session("t", sid)
    assert resumed.status is SessionStatus.IN_PROGRESS
    assert resumed.missing_chunks() == [1, 3]

    # Continue the upload after restart.
    for i in [1, 3]:
        svc2.put_chunk("t", sid, i, io.BytesIO(pieces[i]),
                       sha256_hex(pieces[i]))
    svc2.commit_session("t", sid)
    assert svc2.read_object("t", "survive") == data
    svc2.shutdown()


def test_interrupted_commit_is_resumed_on_restart(tmp_path):
    svc = _svc(tmp_path)
    svc.start()
    data = make_data(7_777, seed=11)
    cs = 2_048
    pieces = split_chunks(data, cs)
    st = svc.create_session("t", "halfcommit", len(data), chunk_size=cs,
                            whole_sha256=sha256_hex(data))
    for i, p in enumerate(pieces):
        svc.put_chunk("t", st.session_id, i, io.BytesIO(p), sha256_hex(p))
    sid = st.session_id

    # Simulate a crash after COMMITTING + journal persisted but before/around
    # the atomic rename: force status and write journal, leave no target.
    with svc._lock:
        st.status = SessionStatus.COMMITTING
        svc.session_store.save(st)
    journal = {
        "session_id": sid,
        "tenant": "t",
        "object_name": "halfcommit",
        "total_size": len(data),
        "whole_sha256": sha256_hex(data),
        "replaced_size": 0,
        "started_at": 0,
    }
    svc.storage.write_atomic(
        svc.storage.journal_path("t", sid),
        json.dumps(journal).encode(),
    )
    svc.shutdown()

    svc2 = _svc(tmp_path)
    svc2.start()  # recovery must finish the commit
    assert svc2.read_object("t", "halfcommit") == data
    # Session record cleaned, quota reflects committed object.
    assert svc2.quota.breakdown("t") == (len(data), 0)
    svc2.shutdown()


def test_commit_interrupted_after_rename_completes_cleanly(tmp_path):
    svc = _svc(tmp_path)
    svc.start()
    data = make_data(5_000, seed=5)
    cs = 2_000
    pieces = split_chunks(data, cs)
    st = svc.create_session("t", "renamed", len(data), chunk_size=cs,
                            whole_sha256=sha256_hex(data))
    for i, p in enumerate(pieces):
        svc.put_chunk("t", st.session_id, i, io.BytesIO(p), sha256_hex(p))
    sid = st.session_id

    # Crash window: object file already renamed into place, journal still
    # present, session still COMMITTING.
    with svc._lock:
        st.status = SessionStatus.COMMITTING
        st.committed_path = svc.storage.object_path("t", "renamed")
        svc.session_store.save(st)
    svc._assemble_and_publish(st, 0)  # performs the atomic rename
    journal = {
        "session_id": sid, "tenant": "t", "object_name": "renamed",
        "total_size": len(data), "whole_sha256": sha256_hex(data),
        "replaced_size": 0, "started_at": 0,
    }
    svc.storage.write_atomic(
        svc.storage.journal_path("t", sid), json.dumps(journal).encode()
    )
    svc.shutdown()

    svc2 = _svc(tmp_path)
    svc2.start()
    assert svc2.read_object("t", "renamed") == data
    assert svc2.quota.breakdown("t") == (len(data), 0)
    svc2.shutdown()


def test_quota_rebuilt_from_disk_after_crash(tmp_path):
    svc = _svc(tmp_path, quota=100_000)
    svc.start()
    # One committed object (4_000 bytes) and one unfinished session (6_000).
    d = make_data(4_000, seed=1)
    st = svc.create_session("t", "done", 4_000, chunk_size=2_000,
                            whole_sha256=sha256_hex(d))
    for i, p in enumerate(split_chunks(d, 2_000)):
        svc.put_chunk("t", st.session_id, i, io.BytesIO(p), sha256_hex(p))
    svc.commit_session("t", st.session_id)

    up = svc.create_session("t", "pending", 6_000, chunk_size=2_000)
    svc.shutdown()

    # Corrupt the quota file to prove recovery rebuilds from on-disk reality.
    qp = svc.storage.quota_path("t")
    os.unlink(qp)

    svc2 = _svc(tmp_path, quota=100_000)
    svc2.start()
    committed, reserved = svc2.quota.breakdown("t")
    assert committed == 4_000
    assert reserved == 6_000
    svc2.shutdown()
