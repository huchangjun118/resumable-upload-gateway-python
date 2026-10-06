"""大对象流式处理：装配缓冲区远小于对象尺寸时结果仍正确，
内存开销以 copy_buffer_size 为界而非随对象大小增长。"""

import hashlib
import io
import os

from gateway import GatewayConfig, UploadGateway

from .conftest import sha256


def test_large_object_with_tiny_buffer(tmp_path):
    gw = UploadGateway(
        GatewayConfig(root_dir=tmp_path / "data", copy_buffer_size=1024)
    )
    chunk_size = 256 * 1024
    chunk_count = 8  # 共 2 MiB，缓冲区仅 1 KiB
    total = chunk_size * chunk_count
    chunks = [os.urandom(chunk_size) for _ in range(chunk_count)]

    gw.create_session("big", "big.bin", "alice", total, chunk_size)
    for i in reversed(range(chunk_count)):  # 逆序提交
        gw.put_chunk("big", i, sha256(chunks[i]), io.BytesIO(chunks[i]),
                     owner="alice")
    pointer = gw.complete_session("big", owner="alice")

    expected = b"".join(chunks)
    assert pointer.size == total
    assert pointer.sha256 == hashlib.sha256(expected).hexdigest()

    # 流式读取校验，不一次性载入内存
    _, stream = gw.read_object_iter("big.bin", chunk=4096)
    hasher = hashlib.sha256()
    read = 0
    for piece in stream:
        assert len(piece) <= 4096
        hasher.update(piece)
        read += len(piece)
    assert read == total
    assert hasher.hexdigest() == pointer.sha256


def test_chunk_upload_streams_from_reader(tmp_path):
    """put_chunk 接受任意 file-like reader，按块流式落盘。"""
    gw = UploadGateway(
        GatewayConfig(root_dir=tmp_path / "data", copy_buffer_size=8)
    )
    data = os.urandom(1000)

    class SlowReader:
        def __init__(self, payload: bytes):
            self._buf = io.BytesIO(payload)

        def read(self, n: int = -1) -> bytes:
            return self._buf.read(min(n, 7))  # 每次最多吐 7 字节

    gw.create_session("s", "o", "alice", len(data), len(data))
    stored, size = gw.put_chunk("s", 0, sha256(data), SlowReader(data),
                                owner="alice")
    assert stored and size == len(data)
    pointer = gw.complete_session("s", owner="alice")
    assert pointer.sha256 == sha256(data)
