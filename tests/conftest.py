import hashlib

import pytest

from gateway import GatewayConfig, UploadGateway


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.fixture()
def gateway(tmp_path):
    return UploadGateway(GatewayConfig(root_dir=tmp_path / "data"))


@pytest.fixture()
def make_gateway(tmp_path):
    def _make(**overrides):
        return UploadGateway(GatewayConfig(root_dir=tmp_path / "data", **overrides))

    return _make
