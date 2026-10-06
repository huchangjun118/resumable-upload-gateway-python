"""配额与归属：越权/超配额在任何持久写入之前被拒绝，不留孤儿。"""

import io

import pytest

from gateway import GatewayConfig, UploadGateway
from gateway.errors import OwnerMismatchError, QuotaExceededError

from .conftest import sha256


@pytest.fixture()
def gw(tmp_path):
    return UploadGateway(
        GatewayConfig(root_dir=tmp_path / "data", default_quota_bytes=64)
    )


def test_create_session_rejected_when_declared_size_exceeds_quota(gw):
    with pytest.raises(QuotaExceededError):
        gw.create_session("s", "obj", "alice", 65, 4)
    # 拒绝发生在任何持久写入之前：无会话元数据、无目录
    assert gw.storage.load_session("s") is None
    assert not gw.storage.session_dir("s").exists()


def test_chunk_rejected_when_quota_would_be_exceeded(gw):
    gw.create_session("s", "obj", "alice", 64, 32)
    gw.put_chunk("s", 0, sha256(b"a" * 32), io.BytesIO(b"a" * 32), owner="alice")
    assert gw.used_bytes("alice") == 32
    # 准入时配额尚够（32+32<=64），但 s 先占满后，s2 的分片在写入前被拒
    gw.create_session("s2", "obj2", "alice", 32, 32)
    gw.put_chunk("s", 1, sha256(b"c" * 32), io.BytesIO(b"c" * 32), owner="alice")
    assert gw.used_bytes("alice") == 64
    with pytest.raises(QuotaExceededError):
        gw.put_chunk("s2", 0, sha256(b"b" * 32), io.BytesIO(b"b" * 32),
                     owner="alice")
    # 被拒绝的分片不留下任何数据
    assert not gw.storage.chunk_path("s2", 0).exists()
    assert gw.used_bytes("alice") == 64


def test_quota_accounts_committed_objects(gw):
    gw.create_session("s", "obj", "alice", 64, 64)
    gw.put_chunk("s", 0, sha256(b"a" * 64), io.BytesIO(b"a" * 64), owner="alice")
    gw.complete_session("s", owner="alice")
    assert gw.used_bytes("alice") == 64
    with pytest.raises(QuotaExceededError):
        gw.create_session("s2", "obj2", "alice", 1, 1)


def test_quota_is_per_owner(gw):
    gw.create_session("s", "obj", "alice", 64, 64)
    gw.put_chunk("s", 0, sha256(b"a" * 64), io.BytesIO(b"a" * 64), owner="alice")
    gw.complete_session("s", owner="alice")
    # bob 有独立配额
    gw.create_session("s2", "obj2", "bob", 64, 64)
    gw.put_chunk("s2", 0, sha256(b"b" * 64), io.BytesIO(b"b" * 64), owner="bob")
    assert gw.used_bytes("bob") == 64


def test_per_owner_quota_override(tmp_path):
    gw = UploadGateway(
        GatewayConfig(root_dir=tmp_path / "data", default_quota_bytes=8,
                      per_owner_quota={"vip": 100})
    )
    with pytest.raises(QuotaExceededError):
        gw.create_session("s", "o", "alice", 9, 4)
    gw.create_session("s2", "o", "vip", 100, 4)


def test_cancel_releases_quota(gw):
    gw.create_session("s", "obj", "alice", 64, 32)
    gw.put_chunk("s", 0, sha256(b"a" * 32), io.BytesIO(b"a" * 32), owner="alice")
    gw.cancel_session("s", owner="alice")
    assert gw.used_bytes("alice") == 0
    # 释放后可以继续上传
    gw.create_session("s2", "obj2", "alice", 64, 64)


def test_owner_mismatch_rejected_before_any_write(gw):
    gw.create_session("s", "obj", "alice", 16, 4)
    with pytest.raises(OwnerMismatchError):
        gw.put_chunk("s", 0, sha256(b"aaaa"), io.BytesIO(b"aaaa"), owner="bob")
    with pytest.raises(OwnerMismatchError):
        gw.complete_session("s", owner="bob")
    with pytest.raises(OwnerMismatchError):
        gw.cancel_session("s", owner="bob")
    with pytest.raises(OwnerMismatchError):
        gw.get_session("s", owner="bob")
    assert not gw.storage.chunk_path("s", 0).exists()
    assert gw.used_bytes("bob") == 0


def test_failed_upload_leaves_no_orphans(gw):
    """各类失败后，磁盘上不应残留占用空间或元数据的孤儿。"""
    with pytest.raises(QuotaExceededError):
        gw.create_session("q", "o", "alice", 1000, 4)
    gw.create_session("s", "o", "alice", 16, 4)
    from gateway.errors import ChunkDigestMismatchError

    with pytest.raises(ChunkDigestMismatchError):
        gw.put_chunk("s", 0, sha256(b"zzzz"), io.BytesIO(b"aaaa"), owner="alice")

    chunks_dir = gw.storage.chunks_dir("s")
    leftovers = list(chunks_dir.iterdir()) if chunks_dir.exists() else []
    assert leftovers == []
    assert gw.storage.load_session("q") is None
