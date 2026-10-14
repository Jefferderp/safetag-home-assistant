"""Read-only authenticated cache gateway; never exposes provider identifiers."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import importlib.util
import os
import sys
import time
from contextlib import asynccontextmanager, suppress
from pathlib import Path

import aiohttp
from fastapi import Depends, FastAPI, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

# Load the ONE shared implementation without executing HA integration __init__.
_MODULE_NAME = "_safetag_shared_api"
if _MODULE_NAME not in sys.modules:
    _path = Path(__file__).resolve().parents[1] / "custom_components" / "safetag" / "api.py"
    _spec = importlib.util.spec_from_file_location(_MODULE_NAME, _path)
    _api = importlib.util.module_from_spec(_spec)
    sys.modules[_MODULE_NAME] = _api
    _spec.loader.exec_module(_api)
else:
    _api = sys.modules[_MODULE_NAME]
TrackerStore = _api.TrackerStore
SafetagClient = _api.SafetagClient


class Tracker(BaseModel):
    device_id: str
    latitude: float | None
    longitude: float | None
    speed_mph: float | None
    course: float | None
    battery_level: float | None
    signal_percent: float | None
    external_voltage: float | None
    charging: bool | None
    relay: bool | None
    last_updated: str | None
    status_last_updated: str | None
    model: float | None
    product: float | None


class GatewayStatus(BaseModel):
    connected: bool
    last_error: str | None
    age_seconds: float | None
    stale: bool


async def run_worker(app, client, *, sleep=asyncio.sleep):
    """Reconcile before each stream connection; retry with bounded backoff."""
    delay = 3
    while True:
        retry_after = 0
        try:
            app.state.store.replace(await client.get_trackers())
            app.state.last_received = time.monotonic()
            async for records in client.stream():
                app.state.store.update(records)
                app.state.connected = True
                app.state.last_error = None
                app.state.last_received = time.monotonic()
                delay = 3
        except _api.SafetagAuthError:
            app.state.last_error = "authentication"
            return
        except _api.SafetagRateLimitError as exc:
            app.state.last_error = "rate_limit"
            retry_after = exc.retry_after
        except (_api.SafetagConnectionError, _api.SafetagResponseError):
            app.state.last_error = "upstream"
        finally:
            app.state.connected = False
        await sleep(max(delay, retry_after))
        delay = min(120, delay * 2)


def create_app(*, token=None, store=None, start_worker=True):
    token = token if token is not None else os.environ.get("SAFETAG_GATEWAY_TOKEN", "")
    if not isinstance(token, str) or len(token) < 32:
        raise ValueError("SAFETAG_GATEWAY_TOKEN must contain at least 32 characters")
    username, api_key = os.environ.get("SAFETAG_USERNAME"), os.environ.get("SAFETAG_API_KEY")
    if start_worker and (not username or not api_key):
        raise ValueError("Safetag upstream credentials are required")
    if start_worker and hmac.compare_digest(token.encode(), api_key.encode()):
        raise ValueError("Gateway token must be different from upstream API key")
    store = store if store is not None else TrackerStore()

    @asynccontextmanager
    async def lifespan(app):
        if not start_worker:
            yield
            return
        async with aiohttp.ClientSession(trust_env=False) as session:
            task = asyncio.create_task(run_worker(app, SafetagClient(session, username, api_key)))
            try:
                yield
            finally:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

    app = FastAPI(
        title="Safetag telemetry gateway",
        version="1.0.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    app.state.store = store
    app.state.connected = False
    app.state.last_received = None
    app.state.last_error = None
    security = HTTPBearer(auto_error=False)

    def authorized(credentials: HTTPAuthorizationCredentials | None = Depends(security)):
        if credentials is None or not hmac.compare_digest(
            credentials.credentials.encode(), token.encode()
        ):
            raise HTTPException(401, "Unauthorized", headers={"WWW-Authenticate": "Bearer"})

    def snapshot():
        result = []
        for raw_id, tracker in store.trackers.items():
            tracker["device_id"] = hmac.new(
                token.encode(), raw_id.encode(), hashlib.sha256
            ).hexdigest()
            result.append(tracker)
        return result

    @app.middleware("http")
    async def no_cache(request, call_next):
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        return response

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/v1/trackers", dependencies=[Depends(authorized)], response_model=list[Tracker])
    def trackers():
        return snapshot()

    @app.get("/v1/trackers/{device_id}", dependencies=[Depends(authorized)], response_model=Tracker)
    def tracker(device_id: str):
        for item in snapshot():
            if item["device_id"] == device_id:
                return item
        raise HTTPException(404, "Tracker not found")

    @app.get("/v1/status", dependencies=[Depends(authorized)], response_model=GatewayStatus)
    def status():
        received = app.state.last_received
        return {
            "connected": app.state.connected,
            "last_error": app.state.last_error,
            "age_seconds": max(0, time.monotonic() - received) if received is not None else None,
            "stale": received is None or time.monotonic() - received > 120,
        }

    @app.get("/openapi.json", dependencies=[Depends(authorized)], include_in_schema=False)
    def openapi():
        return app.openapi()

    return app
