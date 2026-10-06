"""End-to-end HTTP API tests using FastAPI TestClient."""

from __future__ import annotations

import io
import threading

from fastapi.testclient import TestClient

from gateway.api import create_app
from gateway.config import Config
from gateway.manager import GatewayService

from .conftest import make_data, sha256_hex, split_chunks


def make_client(tmp_path, **over):
    cfg = Config(
        data_dir=str(tmp_path / "data"),
        session_ttl_seconds=3600,
        reap_interval_seconds=0,
        **over,
    )
    svc = GatewayService(cfg)
    svc.start()
    return TestClient(create_app(svc)), svc


def create(client, tenant, name, size, **kw):
    r = client.post(
        "/v1/sessions", json={"object_name": name, "total_size": size, **kw},
        headers={"X-Tenant-Id": tenant},
    )
    assert r.status_code == 200, r.text
    return r.json()


def put(client, tenant, sid, n, blob):
    return client.put(
        f"/v1/sessions/{sid}/chunks/{n}",
        content=blob,
        headers={
            "X-Tenant-Id": tenant,
            "X-Content-Sha256": sha256_hex(blob),
            "X-Chunk-Size": str(len(blob)),
        },
    )


def test_full_flow_out_of_order(tmp_path):
    client, svc = make_client(tmp_path)
    data = make_data(12_000, seed=3)
    cs = 4_096
    pieces = split_chunks(data, cs)
    sid = create(client, "t", "dir/file.bin", len(data),
                 chunk_size=cs, whole_sha256=sha256_hex(data))["session_id"]

    for n in [2, 0, 1]:
        r = put(client, "t", sid, n, pieces[n])
        assert r.status_code == 200, r.text
    # Duplicate chunk returns 200 (idempotent) and does not corrupt data.
    assert put(client, "t", sid, 1, pieces[1]).status_code == 200

    r = client.post(f"/v1/sessions/{sid}/commit",
                    headers={"X-Tenant-Id": "t"})
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "completed"

    r = client.get("/v1/objects/dir/file.bin",
                   headers={"X-Tenant-Id": "t"})
    assert r.status_code == 200
    assert r.content == data


def test_missing_tenant_header_rejected(tmp_path):
    client, _ = make_client(tmp_path)
    r = client.post("/v1/sessions",
                    json={"object_name": "x", "total_size": 1})
    assert r.status_code == 403


def test_bad_checksum_rejected(tmp_path):
    client, _ = make_client(tmp_path)
    sid = create(client, "t", "o", 100, chunk_size=100)["session_id"]
    r = client.put(
        f"/v1/sessions/{sid}/chunks/0", content=b"x" * 100,
        headers={"X-Tenant-Id": "t",
                 "X-Content-Sha256": "0" * 64},
    )
    assert r.status_code == 400
    assert r.json()["error"] == "chunk_checksum_mismatch"


def test_commit_with_missing_chunks(tmp_path):
    client, _ = make_client(tmp_path)
    data = make_data(300)
    pieces = split_chunks(data, 100)
    sid = create(client, "t", "o", 300, chunk_size=100)["session_id"]
    assert put(client, "t", sid, 0, pieces[0]).status_code == 200
    r = client.post(f"/v1/sessions/{sid}/commit",
                    headers={"X-Tenant-Id": "t"})
    assert r.status_code == 409
    body = r.json()
    assert body["error"] == "missing_chunks"
    assert body["missing_chunks"] == [1, 2]


def test_quota_rejection_no_data(tmp_path):
    client, svc = make_client(tmp_path, default_quota_bytes=10)
    r = client.post("/v1/sessions", json={"object_name": "o", "total_size": 11},
                    headers={"X-Tenant-Id": "t"})
    assert r.status_code == 413
    assert svc.quota.usage("t") == 0


def test_cancel_and_head(tmp_path):
    client, _ = make_client(tmp_path)
    sid = create(client, "t", "o", 10)["session_id"]
    r = client.delete(f"/v1/sessions/{sid}", headers={"X-Tenant-Id": "t"})
    assert r.status_code == 204
    r = client.get(f"/v1/sessions/{sid}", headers={"X-Tenant-Id": "t"})
    assert r.status_code == 404
    r = client.head("/v1/objects/o", headers={"X-Tenant-Id": "t"})
    assert r.status_code == 404


def test_idempotent_session_create(tmp_path):
    client, _ = make_client(tmp_path)
    a = create(client, "t", "o", 10, idempotency_key="K")
    b = create(client, "t", "o", 10, idempotency_key="K")
    assert a["session_id"] == b["session_id"]


def test_overwrite_competition_http(tmp_path):
    client, svc = make_client(tmp_path, overwrite_policy="always")
    d1, d2 = make_data(5_000, 1), make_data(5_000, 2)
    cs = 2_000
    sids = []
    for d in (d1, d2):
        sid = create(client, "t", "race", len(d), chunk_size=cs)["session_id"]
        for n, p in enumerate(split_chunks(d, cs)):
            assert put(client, "t", sid, n, p).status_code == 200
        sids.append(sid)

    results = {}

    def commit(idx, sid):
        r = client.post(f"/v1/sessions/{sid}/commit",
                        headers={"X-Tenant-Id": "t"})
        results[idx] = r.status_code

    threads = [threading.Thread(target=commit, args=(i, s))
               for i, s in enumerate(sids)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Newest session wins. Depending on interleaving the older commit is
    # either rejected while still racing (409 conflict) or finds itself
    # already cleaned up by the winner's finalization (404); both are
    # deterministic "you lost" outcomes with no residue. The newest wins.
    assert results[1] == 200
    assert results[0] in (409, 404)
    r = client.get("/v1/objects/race", headers={"X-Tenant-Id": "t"})
    assert r.content == d2
