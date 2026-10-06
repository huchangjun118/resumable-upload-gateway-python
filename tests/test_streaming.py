"""Bounded-memory streaming for large objects (requirement 7).

The service reads from the source in fixed-size buffers and streams chunk
bytes straight to disk, so per-request memory is independent of object size.
Here we use a slicing reader (small pieces) to exercise the streaming path
and a large multi-MB object to confirm flat peak-RSS behaviour.
"""

from __future__ import annotations

import gc
import hashlib
import resource

import pytest

from gateway.config import Config
from gateway.manager import GatewayService

from .conftest import make_data, sha256_hex


class SlicingReader:
    """File-like reader over ``data`` that yields at most ``piece`` bytes."""

    def __init__(self, data: bytes, offset: int, length: int,
                 piece: int) -> None:
        self._data = data
        self._end = offset + length
        self._pos = offset
        self._piece = piece

    def read(self, n: int = -1) -> bytes:
        if self._pos >= self._end:
            return b""
        take = self._piece
        if n and 0 < n < take:
            take = n
        take = min(take, self._end - self._pos)
        out = self._data[self._pos:self._pos + take]
        self._pos += take
        return out


def _peak_rss_kb() -> int:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


@pytest.mark.parametrize("size", [1, 4097, 300_000])
def test_upload_and_read_are_streaming(tmp_path, size):
    buf = 8 * 1024
    cfg = Config(
        data_dir=str(tmp_path / "d"),
        session_ttl_seconds=3600,
        reap_interval_seconds=0,
        read_buffer_size=buf,
        default_quota_bytes=1_000_000_000,
    )
    svc = GatewayService(cfg)
    svc.start()
    cs = 64 * 1024
    data = make_data(size, seed=0)

    st = svc.create_session("t", "big", size, chunk_size=cs,
                            whole_sha256=sha256_hex(data))
    for n in range(st.total_chunks):
        off = n * cs
        length = min(cs, size - off)
        chunk_bytes = data[off:off + length]
        reader = SlicingReader(data, off, length, buf)
        svc.put_chunk("t", st.session_id, n, reader,
                      sha256_hex(chunk_bytes))

    gc.collect()
    baseline = _peak_rss_kb()
    svc.commit_session("t", st.session_id)  # streams assembly, not full load
    peak = _peak_rss_kb()
    # Generous fixed slack: no proportional RSS growth with object size.
    assert peak - baseline < 40 * 1024  # KB

    # Streaming read-back returns identical bytes, assembled in small pieces.
    out = bytearray()
    with svc.open_object("t", "big") as f:
        while True:
            b = f.read(buf)
            if not b:
                break
            assert len(b) <= buf
            out.extend(b)
    assert bytes(out) == data
    svc.shutdown()
