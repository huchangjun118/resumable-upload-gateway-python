"""崩溃恢复与保留策略：重启一致性、断点续传、过期回收。"""

import io
import time

from gateway import GatewayConfig, UploadGateway
from gateway.models import SessionState

from .conftest import sha256


def _restart(tmp_path, **overrides):
    return UploadGateway(GatewayConfig(root_dir=tmp_path / "data", **overrides))


def test_committed_object_survives_restart(tmp_path):
    gw = _restart(tmp_path)
    gw.create_session("s", "obj", "alice", 4, 4)
    gw.put_chunk("s", 0, sha256(b"DATA"), io.BytesIO(b"DATA"), owner="alice")
    gw.complete_session("s", owner="alice")

    gw2 = _restart(tmp_path)
    _, stream = gw2.read_object_iter("obj")
    assert b"".join(stream) == b"DATA"
    assert gw2.get_session("s").state == SessionState.COMPLETED


def test_confirmed_chunks_survive_and_upload_resumes(tmp_path):
    gw = _restart(tmp_path)
    gw.create_session("s", "obj", "alice", 8, 4)
    gw.put_chunk("s", 0, sha256(b"aaaa"), io.BytesIO(b"aaaa"), owner="alice")

    gw2 = _restart(tmp_path)  # 模拟崩溃后重启
    record = gw2.get_session("s")
    assert record.state == SessionState.ACTIVE
    assert sorted(record.chunks) == [0]  # 已确认分片不丢
    # 从断点继续，只需补传缺失分片
    gw2.put_chunk("s", 1, sha256(b"bbbb"), io.BytesIO(b"bbbb"), owner="alice")
    gw2.complete_session("s", owner="alice")
    _, stream = gw2.read_object_iter("obj")
    assert b"".join(stream) == b"aaaabbbb"


def test_unfinished_chunk_tmp_file_discarded_on_restart(tmp_path):
    gw = _restart(tmp_path)
    gw.create_session("s", "obj", "alice", 8, 4)
    # 模拟写到一半崩溃的临时分片
    gw.storage.chunks_dir("s").mkdir(parents=True, exist_ok=True)
    gw.storage.chunk_tmp_path("s", 0).write_bytes(b"partial")

    gw2 = _restart(tmp_path)
    assert not gw2.storage.chunk_tmp_path("s", 0).exists()
    assert gw2.get_session("s").chunks == {}


def test_unfinished_commit_staging_discarded_on_restart(tmp_path):
    gw = _restart(tmp_path)
    gw.create_session("s", "obj", "alice", 4, 4)
    gw.put_chunk("s", 0, sha256(b"aaaa"), io.BytesIO(b"aaaa"), owner="alice")
    # 模拟装配到一半崩溃：staging 有残留但指针未切换
    gw.storage.staging_path("deadbeef").write_bytes(b"half-assembled")

    gw2 = _restart(tmp_path)
    assert list(gw2.storage.staging_dir.iterdir()) == []
    import pytest

    with pytest.raises(Exception):  # 对象仍不可见
        gw2.stat_object("obj")
    # 会话未受影响，可重新完成
    gw2.complete_session("s", owner="alice")
    _, stream = gw2.read_object_iter("obj")
    assert b"".join(stream) == b"aaaa"


def test_orphan_data_file_reclaimed_on_restart(tmp_path):
    gw = _restart(tmp_path)
    gw.create_session("s", "obj", "alice", 4, 4)
    gw.put_chunk("s", 0, sha256(b"aaaa"), io.BytesIO(b"aaaa"), owner="alice")
    gw.complete_session("s", owner="alice")
    # 模拟旧版本清理中断留下的孤儿数据文件
    gw.storage.data_path("obj", "orphan-token").write_bytes(b"junk")

    gw2 = _restart(tmp_path)
    assert not gw2.storage.data_path("obj", "orphan-token").exists()
    _, stream = gw2.read_object_iter("obj")
    assert b"".join(stream) == b"aaaa"


def test_usage_rebuilt_after_restart(tmp_path):
    gw = _restart(tmp_path)
    gw.create_session("s1", "o1", "alice", 8, 4)
    gw.put_chunk("s1", 0, sha256(b"aaaa"), io.BytesIO(b"aaaa"), owner="alice")
    gw.create_session("s2", "o2", "alice", 4, 4)
    gw.put_chunk("s2", 0, sha256(b"bbbb"), io.BytesIO(b"bbbb"), owner="alice")
    gw.complete_session("s2", owner="alice")

    gw2 = _restart(tmp_path)
    assert gw2.used_bytes("alice") == 4 + 4  # 已确认分片 + 已提交对象


def test_expired_session_swept_by_retention_policy(tmp_path):
    gw = _restart(tmp_path, session_ttl_seconds=0.05)
    gw.create_session("old", "obj", "alice", 8, 4)
    gw.put_chunk("old", 0, sha256(b"aaaa"), io.BytesIO(b"aaaa"), owner="alice")
    time.sleep(0.06)

    expired = gw.sweep_expired()
    assert expired == ["old"]
    record = gw.get_session("old")
    assert record.state == SessionState.EXPIRED
    assert gw.used_bytes("alice") == 0  # 空间已回收
    assert not gw.storage.chunks_dir("old").exists()
    # 过期后迟到分片被拒绝
    import pytest
    from gateway.errors import SessionStateError

    with pytest.raises(SessionStateError):
        gw.put_chunk("old", 1, sha256(b"bbbb"), io.BytesIO(b"bbbb"),
                     owner="alice")


def test_sweep_does_not_touch_active_or_completed(tmp_path):
    gw = _restart(tmp_path, session_ttl_seconds=0.05)
    gw.create_session("done", "o1", "alice", 4, 4)
    gw.put_chunk("done", 0, sha256(b"aaaa"), io.BytesIO(b"aaaa"), owner="alice")
    gw.complete_session("done", owner="alice")
    gw.create_session("wip", "o2", "alice", 8, 4, )
    gw.put_chunk("wip", 0, sha256(b"cccc"), io.BytesIO(b"cccc"), owner="alice")

    assert gw.sweep_expired() == []  # 未超时，什么都不动
    time.sleep(0.06)
    assert gw.sweep_expired() == ["wip"]  # 只回收超时的进行中会话
    assert gw.get_session("done").state == SessionState.COMPLETED
    _, stream = gw.read_object_iter("o1")
    assert b"".join(stream) == b"aaaa"


def test_lazy_expiry_on_access(tmp_path):
    gw = _restart(tmp_path, session_ttl_seconds=0.05)
    gw.create_session("s", "obj", "alice", 4, 4)
    time.sleep(0.06)
    assert gw.get_session("s").state == SessionState.EXPIRED
