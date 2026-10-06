"""并发语义：同一对象的竞争提交、并行上传、提交期间的读取行为。"""

import io
import threading

from gateway.errors import ObjectNotFoundError

from .conftest import sha256


def _fill(gw, sid, owner, data: bytes):
    gw.create_session(sid, "obj", owner, len(data), max(1, len(data)))
    gw.put_chunk(sid, 0, sha256(data), io.BytesIO(data), owner=owner)


def test_concurrent_commits_deterministic_single_visible_version(gateway):
    """两个会话竞争提交同一对象：最终可见版本必是两者之一且完整，
    未胜出者不留任何可见残留。"""
    _fill(gateway, "r1", "alice", b"A" * 100)
    _fill(gateway, "r2", "alice", b"B" * 100)

    barrier = threading.Barrier(2)
    results = {}

    def commit(sid):
        barrier.wait()
        results[sid] = gateway.complete_session(sid, owner="alice").sha256

    threads = [threading.Thread(target=commit, args=(s,)) for s in ("r1", "r2")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    final = gateway.stat_object("obj")
    assert final.sha256 in (sha256(b"A" * 100), sha256(b"B" * 100))
    _, stream = gateway.read_object_iter("obj")
    data = b"".join(stream)
    assert data in (b"A" * 100, b"B" * 100)
    assert sha256(data) == final.sha256  # 可见版本自洽，非混合
    # 只剩一个版本的数据文件
    leftovers = [
        f for f in gateway.storage.objects_dir.iterdir()
        if f.name.startswith("obj.data.")
    ]
    assert len(leftovers) == 1


def test_repeated_overwrite_last_committer_wins(gateway):
    for i in range(5):
        data = bytes([ord("a") + i]) * 50
        _fill(gateway, f"s{i}", "alice", data)
        gateway.complete_session(f"s{i}", owner="alice")
    _, stream = gateway.read_object_iter("obj")
    assert b"".join(stream) == b"e" * 50
    leftovers = [
        f for f in gateway.storage.objects_dir.iterdir()
        if f.name.startswith("obj.data.")
    ]
    assert len(leftovers) == 1


def test_parallel_uploads_to_different_sessions(gateway):
    """多会话并行上传分片互不阻塞、互不污染。"""
    owners_data = {f"s{i}": bytes([i]) * 64 for i in range(8)}
    for sid, data in owners_data.items():
        gateway.create_session(sid, f"obj-{sid}", "alice", len(data), 16)

    errors = []

    def worker(sid, data):
        try:
            for i in range(4):
                piece = data[i * 16:(i + 1) * 16]
                gateway.put_chunk(sid, i, sha256(piece), io.BytesIO(piece),
                                  owner="alice")
            gateway.complete_session(sid, owner="alice")
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(s, d))
               for s, d in owners_data.items()]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    for sid, data in owners_data.items():
        _, stream = gateway.read_object_iter(f"obj-{sid}")
        assert b"".join(stream) == data


def test_read_during_commit_sees_old_or_new_never_partial(gateway):
    """提交进行中到达的读取：要么看到旧版本，要么看到新版本，
    绝不看到部分数据或混合版本。"""
    _fill(gateway, "v1", "alice", b"1" * 4096)
    gateway.complete_session("v1", owner="alice")
    _fill(gateway, "v2", "alice", b"2" * 4096)

    stop = threading.Event()
    violations = []

    def reader():
        while not stop.is_set():
            _, stream = gateway.read_object_iter("obj")
            data = b"".join(stream)
            if data not in (b"1" * 4096, b"2" * 4096):
                violations.append(data[:16])

    r = threading.Thread(target=reader, daemon=True)
    r.start()
    try:
        for i in range(20):
            _fill(gateway, f"vx-{i}", "alice", b"2" * 4096)
            gateway.complete_session(f"vx-{i}", owner="alice")
    finally:
        stop.set()
        r.join(timeout=10)
    assert violations == []


def test_read_missing_object(gateway):
    import pytest

    with pytest.raises(ObjectNotFoundError):
        gateway.stat_object("missing")
