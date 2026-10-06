"""Concurrent uploads to the same object name (requirement 4).

Deterministic rule under ``overwrite_policy='always'``: among live sessions
targeting an object, the session created LAST (greatest create_seq) wins the
commit. Earlier sessions are rejected at commit with ObjectAlreadyExists and
leave no files or quota residue. Under ``'never'`` the first commit wins and
overwriting attempts are rejected.
"""

from __future__ import annotations

import io
import os
import threading

import pytest

from gateway.config import Config
from gateway.errors import ObjectAlreadyExists, SessionClosed
from gateway.manager import GatewayService
from gateway.models import SessionStatus

from .conftest import make_data, sha256_hex, split_chunks


def _fully_upload(svc, tenant, name, data, cs):
    pieces = split_chunks(data, cs)
    st = svc.create_session(
        tenant, name, len(data), chunk_size=cs,
        whole_sha256=sha256_hex(data),
    )
    for i, p in enumerate(pieces):
        svc.put_chunk(tenant, st.session_id, i, io.BytesIO(p),
                      sha256_hex(p))
    return st


def test_always_policy_last_created_wins(tmp_path):
    cfg = Config(data_dir=str(tmp_path / "d"), session_ttl_seconds=3600,
                 reap_interval_seconds=0, overwrite_policy="always")
    svc = GatewayService(cfg)
    svc.start()

    d1 = make_data(5_000, seed=1)
    d2 = make_data(5_000, seed=2)
    s1 = _fully_upload(svc, "t", "o", d1, 2_000)  # created first
    s2 = _fully_upload(svc, "t", "o", d2, 2_000)  # created second -> wins

    # Commit the older first; it must lose to the newer live session.
    with pytest.raises(ObjectAlreadyExists):
        svc.commit_session("t", s1.session_id)

    # Loser leaves no files and no reservation.
    assert not os.path.exists(svc.storage.session_dir("t", s1.session_id))
    assert svc.quota.breakdown("t") == (0, 5_000)

    svc.commit_session("t", s2.session_id)
    assert svc.read_object("t", "o") == d2
    assert svc.quota.breakdown("t") == (5_000, 0)
    svc.shutdown()


def test_never_policy_first_commit_wins(tmp_path):
    cfg = Config(data_dir=str(tmp_path / "d"), session_ttl_seconds=3600,
                 reap_interval_seconds=0, overwrite_policy="never")
    svc = GatewayService(cfg)
    svc.start()
    d1 = make_data(3_000, seed=1)
    d2 = make_data(3_000, seed=2)
    s1 = _fully_upload(svc, "t", "o", d1, 2_000)
    svc.commit_session("t", s1.session_id)

    s2 = _fully_upload(svc, "t", "o", d2, 2_000)
    with pytest.raises(ObjectAlreadyExists):
        svc.commit_session("t", s2.session_id)
    # Old content remains visible; loser fully cleaned up.
    assert svc.read_object("t", "o") == d1
    assert not os.path.exists(svc.storage.session_dir("t", s2.session_id))
    assert svc.quota.breakdown("t") == (3_000, 0)
    svc.shutdown()


def test_parallel_commits_only_one_wins_no_residue(tmp_path):
    cfg = Config(data_dir=str(tmp_path / "d"), session_ttl_seconds=3600,
                 reap_interval_seconds=0, overwrite_policy="always",
                 default_quota_bytes=10_000_000)
    svc = GatewayService(cfg)
    svc.start()

    N = 8
    sessions = []
    for k in range(N):
        d = make_data(20_000, seed=100 + k)
        sessions.append((d, _fully_upload(svc, "t", "race", d, 4_096)))

    results: list[BaseException | None] = []
    lock = threading.Lock()

    def worker(d, st):
        try:
            svc.commit_session("t", st.session_id)
            err = None
        except BaseException as exc:  # noqa: BLE001
            err = exc
        with lock:
            results.append(err)

    threads = [
        threading.Thread(target=worker, args=(d, st))
        for d, st in sessions
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    successes = [e for e in results if e is None]
    conflicts = [e for e in results if isinstance(e, ObjectAlreadyExists)]
    assert len(successes) == 1
    assert len(conflicts) == N - 1

    # Exactly one object, exactly the winner's content; no session residue.
    objs = {n: sz for n, sz in svc.storage.list_objects("t")}
    assert list(objs) == ["race"]
    winner_data = sessions[-1][0]  # last created wins deterministically
    assert svc.read_object("t", "race") == winner_data
    sroot = svc.storage.session_dir("t", "x")
    tenant_root = os.path.dirname(sroot)
    # Tenant dir may itself be removed when empty; either way no session dirs.
    leftovers = (
        os.listdir(tenant_root) if os.path.isdir(tenant_root) else []
    )
    assert leftovers == []
    # No staging files, no leaked reservation.
    staging = svc.storage.staging_dir("t")
    assert not os.path.exists(staging) or os.listdir(staging) == []
    assert svc.quota.breakdown("t") == (20_000, 0)
    svc.shutdown()


def test_concurrent_chunk_uploads_assemble_correctly(tmp_path):
    cfg = Config(data_dir=str(tmp_path / "d"), session_ttl_seconds=3600,
                 reap_interval_seconds=0, default_quota_bytes=10_000_000)
    svc = GatewayService(cfg)
    svc.start()
    data = make_big = make_data(60_000, seed=9)
    cs = 4_096
    pieces = split_chunks(data, cs)
    st = svc.create_session("t", "parallel-chunks", len(data), chunk_size=cs,
                            whole_sha256=sha256_hex(data))

    def worker(i):
        svc.put_chunk("t", st.session_id, i, io.BytesIO(pieces[i]),
                      sha256_hex(pieces[i]))

    order = list(range(len(pieces)))
    import random
    random.Random(42).shuffle(order)
    threads = [threading.Thread(target=worker, args=(i,)) for i in order]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    svc.commit_session("t", st.session_id)
    assert svc.read_object("t", "parallel-chunks") == data
    svc.shutdown()


def test_reads_during_commit_see_one_complete_version(tmp_path):
    cfg = Config(data_dir=str(tmp_path / "d"), session_ttl_seconds=3600,
                 reap_interval_seconds=0, overwrite_policy="always")
    svc = GatewayService(cfg)
    svc.start()
    d1 = make_data(10_000, seed=1)
    s1 = _fully_upload(svc, "t", "o", d1, 4_096)
    svc.commit_session("t", s1.session_id)

    d2 = make_data(10_000, seed=2)
    s2 = _fully_upload(svc, "t", "o", d2, 4_096)

    observed = []
    errors = []

    def reader():
        try:
            observed.append(svc.read_object("t", "o"))
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    # Start a reader concurrently with the commit.
    tr = threading.Thread(target=reader)
    tc = threading.Thread(target=lambda: svc.commit_session("t", s2.session_id))
    tr.start(); tc.start()
    tr.join(); tc.join()

    assert errors == []
    assert observed[0] in (d1, d2)  # one complete version, never a mixture
    assert svc.read_object("t", "o") == d2
    svc.shutdown()


def test_queued_older_committing_session_cannot_overwrite_winner(tmp_path):
    """Both sessions flip COMMITTING and serialize on the object lock.

    The older session must lose even though, by the time it acquires the
    lock, the winner has already completed and been removed from memory.
    """
    import time as _time
    cfg = Config(data_dir=str(tmp_path / "d"), session_ttl_seconds=3600,
                 reap_interval_seconds=0, overwrite_policy="always")
    svc = GatewayService(cfg)
    svc.start()
    d1 = make_data(5_000, seed=1)
    d2 = make_data(5_000, seed=2)
    s_old = _fully_upload(svc, "t", "o", d1, 2_000)
    s_new = _fully_upload(svc, "t", "o", d2, 2_000)

    errors = []

    def older():
        try:
            svc.commit_session("t", s_old.session_id)
        except ObjectAlreadyExists as e:
            errors.append(e)

    def newer():
        _time.sleep(0.05)  # ensure the older one enters the lock first
        svc.commit_session("t", s_new.session_id)

    t1 = threading.Thread(target=older)
    t2 = threading.Thread(target=newer)
    t1.start(); t2.start()
    t1.join(); t2.join()

    assert len(errors) == 1
    # The newer session's content must remain visible, never overwritten.
    assert svc.read_object("t", "o") == d2
    assert svc.quota.breakdown("t") == (5_000, 0)
    svc.shutdown()
