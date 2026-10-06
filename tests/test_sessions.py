"""会话生命周期与分片语义：乱序、重复、迟到、校验失败、缺失识别。"""

import hashlib
import io

import pytest

from gateway.errors import (
    ChunkDigestMismatchError,
    ChunkIndexError,
    IncompleteUploadError,
    SessionConflictError,
    SessionNotFoundError,
    SessionStateError,
)
from gateway.models import SessionState

from .conftest import sha256


def upload_all(gw, session_id, owner, chunks: dict[int, bytes]):
    for index, data in chunks.items():
        gw.put_chunk(session_id, index, sha256(data), io.BytesIO(data), owner=owner)


def test_create_session_is_idempotent_with_same_params(gateway):
    r1, created1 = gateway.create_session("s", "obj", "alice", 10, 4)
    r2, created2 = gateway.create_session("s", "obj", "alice", 10, 4)
    assert created1 is True
    assert created2 is False
    assert r1.session_id == r2.session_id == "s"


def test_create_session_conflicts_on_different_params(gateway):
    gateway.create_session("s", "obj", "alice", 10, 4)
    with pytest.raises(SessionConflictError):
        gateway.create_session("s", "obj", "alice", 11, 4)
    with pytest.raises(SessionConflictError):
        gateway.create_session("s", "other-obj", "alice", 10, 4)
    with pytest.raises(SessionConflictError):
        gateway.create_session("s", "obj", "bob", 10, 4)


def test_out_of_order_chunks_complete_successfully(gateway):
    gateway.create_session("s", "obj", "alice", 10, 4)
    upload_all(gateway, "s", "alice", {2: b"ij", 0: b"abcd", 1: b"efgh"})
    pointer = gateway.complete_session("s", owner="alice")
    assert pointer.size == 10
    _, stream = gateway.read_object_iter("obj")
    assert b"".join(stream) == b"abcdefghij"


def test_duplicate_chunk_same_digest_is_noop(gateway):
    gateway.create_session("s", "obj", "alice", 4, 4)
    stored1, _ = gateway.put_chunk("s", 0, sha256(b"abcd"), io.BytesIO(b"abcd"),
                                   owner="alice")
    stored2, _ = gateway.put_chunk("s", 0, sha256(b"abcd"), io.BytesIO(b"abcd"),
                                   owner="alice")
    assert stored1 is True
    assert stored2 is False  # 幂等成功，不重写
    pointer = gateway.complete_session("s", owner="alice")
    assert pointer.sha256 == sha256(b"abcd")


def test_duplicate_chunk_different_digest_conflicts(gateway):
    gateway.create_session("s", "obj", "alice", 4, 4)
    gateway.put_chunk("s", 0, sha256(b"abcd"), io.BytesIO(b"abcd"), owner="alice")
    with pytest.raises(SessionConflictError):
        gateway.put_chunk("s", 0, sha256(b"WXYZ"), io.BytesIO(b"WXYZ"),
                          owner="alice")
    # 已确认数据未被破坏
    pointer = gateway.complete_session("s", owner="alice")
    assert pointer.sha256 == sha256(b"abcd")


def test_digest_mismatch_rejected_and_leaves_no_residue(gateway):
    gateway.create_session("s", "obj", "alice", 8, 4)
    with pytest.raises(ChunkDigestMismatchError):
        gateway.put_chunk("s", 0, sha256(b"zzzz"), io.BytesIO(b"abcd"),
                          owner="alice")
    assert not gateway.storage.chunk_path("s", 0).exists()
    assert not gateway.storage.chunk_tmp_path("s", 0).exists()
    record = gateway.get_session("s")
    assert record.chunks == {}


def test_wrong_size_rejected(gateway):
    gateway.create_session("s", "obj", "alice", 8, 4)
    data = b"abcde"
    with pytest.raises(ChunkDigestMismatchError):
        gateway.put_chunk("s", 0, sha256(data), io.BytesIO(data), owner="alice")
    assert not gateway.storage.chunk_path("s", 0).exists()


def test_chunk_index_out_of_range(gateway):
    gateway.create_session("s", "obj", "alice", 8, 4)
    with pytest.raises(ChunkIndexError):
        gateway.put_chunk("s", 2, sha256(b"abcd"), io.BytesIO(b"abcd"),
                          owner="alice")


def test_complete_with_missing_chunks_reports_them(gateway):
    gateway.create_session("s", "obj", "alice", 12, 4)
    upload_all(gateway, "s", "alice", {0: b"aaaa", 2: b"cccc"})
    with pytest.raises(IncompleteUploadError) as exc_info:
        gateway.complete_session("s", owner="alice")
    assert exc_info.value.context["missing"] == [1]
    # 会话仍然 ACTIVE，可继续补传
    assert gateway.get_session("s").state == SessionState.ACTIVE


def test_late_chunk_after_complete_rejected(gateway):
    gateway.create_session("s", "obj", "alice", 4, 4)
    upload_all(gateway, "s", "alice", {0: b"abcd"})
    gateway.complete_session("s", owner="alice")
    with pytest.raises(SessionStateError):
        gateway.put_chunk("s", 0, sha256(b"abcd"), io.BytesIO(b"abcd"),
                          owner="alice")


def test_late_chunk_after_cancel_rejected(gateway):
    gateway.create_session("s", "obj", "alice", 4, 4)
    gateway.cancel_session("s", owner="alice")
    assert gateway.get_session("s").state == SessionState.CANCELLED
    with pytest.raises(SessionStateError):
        gateway.put_chunk("s", 0, sha256(b"abcd"), io.BytesIO(b"abcd"),
                          owner="alice")


def test_cancel_reclaims_chunk_space(gateway):
    gateway.create_session("s", "obj", "alice", 8, 4)
    upload_all(gateway, "s", "alice", {0: b"aaaa"})
    used_before = gateway.used_bytes("alice")
    assert used_before == 4
    gateway.cancel_session("s", owner="alice")
    assert gateway.used_bytes("alice") == 0
    assert not gateway.storage.chunks_dir("s").exists()


def test_get_unknown_session(gateway):
    with pytest.raises(SessionNotFoundError):
        gateway.get_session("nope")


def test_empty_object(gateway):
    gateway.create_session("s", "empty", "alice", 0, 4)
    pointer = gateway.complete_session("s", owner="alice")
    assert pointer.size == 0
    _, stream = gateway.read_object_iter("empty")
    assert b"".join(stream) == b""
