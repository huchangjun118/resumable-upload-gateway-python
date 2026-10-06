"""HTTP 层端到端：完整上传流程、错误码映射、对象下载。"""

import pytest
from fastapi.testclient import TestClient

from gateway import GatewayConfig, UploadGateway
from gateway.api import create_app

from .conftest import sha256


@pytest.fixture()
def client(tmp_path):
    gw = UploadGateway(GatewayConfig(root_dir=tmp_path / "data"))
    return TestClient(create_app(gw))


def test_full_upload_download_flow(client):
    r = client.post("/sessions", json={
        "object_name": "dir/file.bin", "owner": "alice",
        "total_size": 10, "chunk_size": 4, "session_id": "s1",
    })
    assert r.status_code == 201
    assert r.json()["created"] is True
    assert r.json()["state"] == "active"

    # 幂等重建
    r = client.post("/sessions", json={
        "object_name": "dir/file.bin", "owner": "alice",
        "total_size": 10, "chunk_size": 4, "session_id": "s1",
    })
    assert r.json()["created"] is False

    # 乱序分片
    for index, data in [(2, b"ij"), (0, b"abcd"), (1, b"efgh")]:
        r = client.put(
            f"/sessions/s1/chunks/{index}",
            content=data,
            headers={"X-Chunk-SHA256": sha256(data), "X-Owner": "alice"},
        )
        assert r.status_code == 200, r.text
        assert r.json()["stored"] is True

    # 重复分片 -> stored=false
    r = client.put(
        "/sessions/s1/chunks/0",
        content=b"abcd",
        headers={"X-Chunk-SHA256": sha256(b"abcd"), "X-Owner": "alice"},
    )
    assert r.json()["stored"] is False

    r = client.post("/sessions/s1/complete", json={"owner": "alice"})
    assert r.status_code == 200
    assert r.json()["sha256"] == sha256(b"abcdefghij")

    r = client.get("/objects/dir/file.bin")
    assert r.status_code == 200
    assert r.content == b"abcdefghij"
    assert r.headers["X-Object-SHA256"] == sha256(b"abcdefghij")


def test_error_status_mapping(client):
    client.post("/sessions", json={
        "object_name": "o", "owner": "alice",
        "total_size": 4, "chunk_size": 4, "session_id": "s1",
    })
    # 摘要错误 -> 422
    r = client.put("/sessions/s1/chunks/0", content=b"abcd",
                   headers={"X-Chunk-SHA256": sha256(b"zzzz"),
                            "X-Owner": "alice"})
    assert r.status_code == 422
    assert r.json()["error"] == "chunk_digest_mismatch"
    # 越权 -> 403
    r = client.put("/sessions/s1/chunks/0", content=b"abcd",
                   headers={"X-Chunk-SHA256": sha256(b"abcd"),
                            "X-Owner": "mallory"})
    assert r.status_code == 403
    # 会话不存在 -> 404
    r = client.get("/sessions/nope")
    assert r.status_code == 404
    # 对象不存在 -> 404
    r = client.get("/objects/missing")
    assert r.status_code == 404
    # 参数冲突 -> 409
    r = client.post("/sessions", json={
        "object_name": "different", "owner": "alice",
        "total_size": 4, "chunk_size": 4, "session_id": "s1",
    })
    assert r.status_code == 409


def test_incomplete_complete_returns_missing(client):
    client.post("/sessions", json={
        "object_name": "o", "owner": "alice",
        "total_size": 8, "chunk_size": 4, "session_id": "s1",
    })
    client.put("/sessions/s1/chunks/0", content=b"aaaa",
               headers={"X-Chunk-SHA256": sha256(b"aaaa"), "X-Owner": "alice"})
    r = client.post("/sessions/s1/complete", json={"owner": "alice"})
    assert r.status_code == 409
    assert r.json()["error"] == "incomplete_upload"
    assert r.json()["missing"] == [1]


def test_quota_exceeded_returns_413(tmp_path):
    gw = UploadGateway(
        GatewayConfig(root_dir=tmp_path / "data", default_quota_bytes=4)
    )
    client = TestClient(create_app(gw))
    r = client.post("/sessions", json={
        "object_name": "o", "owner": "alice",
        "total_size": 100, "chunk_size": 4, "session_id": "s1",
    })
    assert r.status_code == 413
    assert r.json()["error"] == "quota_exceeded"
