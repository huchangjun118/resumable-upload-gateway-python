"""Application bootstrap: build a service+API from configuration."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from .api import create_app
from .config import Config, load_config
from .manager import GatewayService


def build_service(config: Config | None = None) -> GatewayService:
    cfg = config or load_config()
    service = GatewayService(cfg)
    service.start()
    return service


def build_app(config: Config | None = None) -> FastAPI:
    service = build_service(config)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        service.shutdown()

    app = create_app(service)
    app.router.lifespan_context = lifespan
    return app
