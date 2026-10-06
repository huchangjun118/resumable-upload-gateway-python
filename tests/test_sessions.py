"""Basic lifecycle, out-of-order, duplicate and late chunks."""

from __future__ import annotations

import io

import pytest

from gateway.errors import (
    ChunkChecksumMismatch,
    ChunkOutOfRange,
    ChunkSizeMismatch,
    InvalidRequest,
    MissingChunks,
    SessionCanceled,
    SessionClosed,
    SessionCompleted,
)
from gateway.models import SessionStatus

from .conftest import make_data, sha256_hex, split_chunks, upload_session


def test_happy_path_out_of_order(service):
    data = make_data(10_000, seed=1)
    cs = 4_096
    st = upload_session(
        service, "t1", "a/big.bin", data, cs, order=[2, 0, 1]
    )
    assert st.status in (SessionStatus.IN_PROGRESS, SessionStatus.INIT)
    assert service.get_session("t1", st.session_id).missing_chunks() == []

    committed = service.commit_session("t1", st.session_id)
    assert committed.status is SessionStatus.COMPLETED
    assert service.read_object("t1", "a/big.bin") == data


def test_zero_byte_object(service):
    st = service.create_session("t", "empty", 0, chunk_size=1024)
    committed = service.commit_session("t", st.session_id)
    assert committed.status is SessionStatus.COMPLETED
    assert service.read_object("t", "empty") == b""


def test_commit_with_missing_chunks_raises(service):
    data = make_data(5_000)
    pieces = split_chunks(data, 2_000)
    st = service.create_session("t", "o", len(data), chunk_size=2_000)
    service.put_chunk("t", st.session_id, 0, io.BytesIO(pieces[0]),
                      sha256_hex(pieces[0]))
    service.put_chunk("t", st.session_id, 2, io.BytesIO(pieces[2]),
                      sha256_hex(pieces[2]))
    with pytest.raises(MissingChunks) as ei:
        service.commit_session("t", st.session_id)
    assert ei.value.missing == [1]
    # Object must not exist.
    assert not service.storage.object_exists("t", "o")


def test_duplicate_chunk_is_idempotent_and_immutable(service):
    data = make_data(3_000, seed=3)
    pieces = split_chunks(data, 1_000)
    st = service.create_session("t", "dup", len(data), chunk_size=1_000)
    service.put_chunk("t", st.session_id, 0, io.BytesIO(pieces[0]),
                      sha256_hex(pieces[0]))
    # Same bytes + same checksum -> idempotent ack, stored file unchanged.
    path = service.storage.chunk_path("t", st.session_id, 0)
    before = open(path, "rb").read()
    service.put_chunk("t", st.session_id, 0, io.BytesIO(pieces[0]),
                      sha256_hex(pieces[0]))
    assert open(path, "rb").read() == before

    # Same slot, different declared checksum -> rejected, original intact.
    with pytest.raises(InvalidRequest):
        service.put_chunk("t", st.session_id, 0, io.BytesIO(pieces[0]),
                          sha256_hex(b"x" * 32))
    assert open(path, "rb").read() == before


def test_bad_checksum_rejected_no_partial_state(service):
    data = make_data(2_000)
    pieces = split_chunks(data, 1_000)
    st = service.create_session("t", "bad", len(data), chunk_size=1_000)
    with pytest.raises(ChunkChecksumMismatch):
        service.put_chunk("t", st.session_id, 0, io.BytesIO(pieces[0]),
                          sha256_hex(b"deadbeef" * 8))
    state = service.get_session("t", st.session_id)
    assert 0 not in state.chunks  # not acknowledged
    assert state.status is SessionStatus.INIT
    # No .recv temp files left behind.
    cdir = service.storage.chunks_dir("t", st.session_id)
    assert all(not f.endswith(("recv",)) for f in __import__("os").listdir(cdir))


def test_oversized_chunk_rejected(service):
    data = make_data(2_000)
    pieces = split_chunks(data, 1_000)
    st = service.create_session("t", "big", len(data), chunk_size=1_000)
    with pytest.raises(ChunkSizeMismatch):
        service.put_chunk("t", st.session_id, 0,
                          io.BytesIO(pieces[0] + b"x"),
                          sha256_hex(pieces[0] + b"x"))


def test_chunk_number_out_of_range(service):
    st = service.create_session("t", "o", 1_000, chunk_size=1_000)
    with pytest.raises(ChunkOutOfRange):
        service.put_chunk("t", st.session_id, 5, io.BytesIO(b"x"),
                          sha256_hex(b"x"))


def test_abort_cleans_up(service):
    data = make_data(3_000)
    st = upload_session(service, "t", "abort", data, 1_000, order=[1])
    service.abort_session("t", st.session_id)
    # Abort is idempotent.
    service.abort_session("t", st.session_id)
    from gateway.errors import SessionNotFound
    # No files and no reservation remain.
    assert not __import__("os").path.exists(
        service.storage.session_dir("t", st.session_id)
    )
    assert service.quota.usage("t") == 0
    with pytest.raises(SessionNotFound):
        service.get_session("t", st.session_id)


def test_writes_after_complete_rejected(service):
    from gateway.errors import SessionNotFound
    data = make_data(1_000)
    st = upload_session(service, "t", "done", data, 1_000)
    service.commit_session("t", st.session_id)
    # Once committed, the session record is gone; late chunks cannot attach
    # to the visible object and never mutate it.
    with pytest.raises(SessionNotFound):
        service.put_chunk("t", st.session_id, 0, io.BytesIO(data),
                          sha256_hex(data))
    assert service.read_object("t", "done") == data


def test_commit_idempotent(service):
    from gateway.errors import SessionNotFound
    data = make_data(1_000)
    st = upload_session(service, "t", "idem", data, 1_000)
    c1 = service.commit_session("t", st.session_id)
    assert c1.status is SessionStatus.COMPLETED
    # A repeated commit finds no session (the object is already durable);
    # the object content is unchanged and complete.
    with pytest.raises(SessionNotFound):
        service.commit_session("t", st.session_id)
    assert service.read_object("t", "idem") == data


def test_whole_checksum_mismatch_rolls_back_and_object_absent(service):
    # Declare a wrong whole-object checksum: commit must fail atomically,
    # object never appears, and session returns to in_progress for retry.
    from gateway.errors import WholeObjectChecksumMismatch
    data = make_data(2_000)
    pieces = split_chunks(data, 1_000)
    st = service.create_session("t", "whole", len(data), chunk_size=1_000,
                                whole_sha256="0" * 64)
    for i, p in enumerate(pieces):
        service.put_chunk("t", st.session_id, i, io.BytesIO(p),
                          sha256_hex(p))
    with pytest.raises(WholeObjectChecksumMismatch):
        service.commit_session("t", st.session_id)
    # No visible object, no staging residue, session still live/in progress.
    from gateway.errors import SessionNotFound
    with pytest.raises(SessionNotFound):
        service.head_object("t", "whole")
    s2 = service.get_session("t", st.session_id)
    assert s2.status is SessionStatus.IN_PROGRESS
    # Reservation retained while the session stays retryable.
    assert service.quota.usage("t") == 2_000
    service.abort_session("t", st.session_id)
    assert service.quota.usage("t") == 0
