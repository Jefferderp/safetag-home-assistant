"""Dependency-light Safetag telemetry normalization and transport."""

from __future__ import annotations

import asyncio
import math
import ssl
from collections import deque
from copy import deepcopy
from datetime import datetime
from time import monotonic as _clock
from urllib.parse import quote

import aiohttp

BASE_URL = "https://api.safetagtracking.com/api/v1"
STREAM_URL = "wss://api.safetagtracking.com/api/v1/stream"
MAX_PAGES = 100
PAGE_SIZE = 500
_TLS_CONTEXT = ssl.create_default_context()


class SafetagAuthError(Exception):
    """Credentials were rejected."""


class SafetagConnectionError(Exception):
    """Upstream is unavailable."""


class SafetagRateLimitError(Exception):
    """Upstream requested a retry delay."""

    def __init__(self, retry_after=60):
        self.retry_after = retry_after
        super().__init__("Safetag rate limit reached")


def _check_status(status, headers):
    if status in (401, 403):
        raise SafetagAuthError("Safetag authentication failed")
    if status == 429:
        try:
            delay = max(1, int(headers.get("Retry-After", "60")))
        except (TypeError, ValueError):
            delay = 60
        raise SafetagRateLimitError(delay)
    if status >= 500:
        raise SafetagConnectionError("Safetag unavailable")
    if status != 200:
        raise SafetagResponseError("Unexpected Safetag response")


class SafetagClient:
    """Uses a caller-owned verified-TLS session; never follows redirects."""

    def __init__(self, session, username, api_key):
        self._session, self._username, self._api_key = session, username, api_key
        self._request_times = deque()
        self._request_lock = asyncio.Lock()
        self._stream_slots = asyncio.Semaphore(5)

    async def _wait_request(self):
        async with self._request_lock:
            while self._request_times and _clock() - self._request_times[0] >= 60:
                self._request_times.popleft()
            if len(self._request_times) >= 20:
                await asyncio.sleep(max(0, 60 - (_clock() - self._request_times[0])))
                while self._request_times and _clock() - self._request_times[0] >= 60:
                    self._request_times.popleft()
            self._request_times.append(_clock())

    async def stream(self):
        async with self._stream_slots:
            generator = self._stream()
            try:
                async for batch in generator:
                    yield batch
            finally:
                await generator.aclose()

    async def _stream(self):
        # aiohttp ws_connect follows redirects and has no allow_redirects option.
        # A dedicated session trace rejects them before any credential forwarding.
        trace = aiohttp.TraceConfig()

        async def reject_redirect(*args):
            raise SafetagResponseError("Safetag redirect rejected")

        trace.on_request_redirect.append(reject_redirect)
        try:
            async with aiohttp.ClientSession(
                connector=self._session.connector,
                connector_owner=False,
                timeout=aiohttp.ClientTimeout(total=20),
                trace_configs=[trace],
                trust_env=False,
            ) as session:
                async with session.ws_connect(
                    STREAM_URL,
                    headers={"X-API-KEY": self._api_key, "username": self._username},
                    ssl=_TLS_CONTEXT,
                    heartbeat=30,
                    timeout=aiohttp.ClientWSTimeout(ws_receive=90, ws_close=10),
                    max_msg_size=1024 * 1024,
                ) as socket:
                    async for message in socket:
                        if message.type == aiohttp.WSMsgType.TEXT:
                            try:
                                yield _validate(message.json())
                            except ValueError:
                                raise SafetagResponseError("Invalid stream response") from None
                        elif message.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.BINARY):
                            raise SafetagConnectionError("Safetag stream failed")
        except aiohttp.WSServerHandshakeError as exc:
            _check_status(exc.status, exc.headers or {})
            raise SafetagConnectionError("Safetag stream handshake failed") from None
        except (aiohttp.ClientError, asyncio.TimeoutError):
            raise SafetagConnectionError("Safetag stream connection failed") from None

    async def get_trackers(self):
        records = []
        for page in range(1, MAX_PAGES + 1):
            await self._wait_request()
            url = f"{BASE_URL}/devices/{quote(self._username, safe='')}?page={page}"
            try:
                async with self._session.get(
                    url,
                    headers={"X-API-KEY": self._api_key},
                    timeout=aiohttp.ClientTimeout(total=20),
                    allow_redirects=False,
                    ssl=_TLS_CONTEXT,
                ) as response:
                    _check_status(response.status, response.headers)
                    try:
                        batch = _validate(await response.json())
                    except (ValueError, aiohttp.ContentTypeError):
                        raise SafetagResponseError("Invalid tracker response") from None
            except (aiohttp.ClientError, asyncio.TimeoutError):
                raise SafetagConnectionError("Safetag connection failed") from None
            records.extend(batch)
            if len(batch) < PAGE_SIZE:
                return records
        raise SafetagResponseError("Safetag pagination limit reached")


class SafetagResponseError(Exception):
    """Upstream schema or response is unsafe/invalid."""


def _validate(records):
    if not isinstance(records, list):
        raise SafetagResponseError("Invalid tracker response")
    for raw in records:
        if not isinstance(raw, dict) or not isinstance(raw.get("_id"), str) or not raw["_id"]:
            raise SafetagResponseError("Invalid tracker response")
        status = raw.get("status", {})
        if status is not None and (
            not isinstance(status, dict)
            or (status.get("coords") is not None and not isinstance(status["coords"], dict))
        ):
            raise SafetagResponseError("Invalid tracker response")
    return records


def _date(value):
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return value if parsed.tzinfo is not None and parsed.utcoffset() is not None else None
    except ValueError:
        return None


class TrackerStore:
    """Atomic updates with a metadata allowlist and detached snapshots."""

    def __init__(self):
        self._raw = {}

    @property
    def trackers(self):
        return {key: normalize_tracker(value) for key, value in self._raw.items()}

    def replace(self, records):
        replacement = TrackerStore()
        replacement.update(records)
        self._raw = replacement._raw

    def update(self, records):
        _validate(records)
        updated = deepcopy(self._raw)
        for raw in records:
            key = raw["_id"]
            target = updated.setdefault(key, {"_id": key, "status": {}})
            for field in ("model", "product"):
                if field in raw:
                    target[field] = _number(raw[field])
            if "status" in raw and raw["status"] is None:
                target["status"] = {}
            incoming = raw.get("status") or {}
            status = target["status"]
            if "location" in incoming:
                status.pop("coords", None)
            if "coords" in incoming:
                status.pop("location", None)
                if incoming["coords"] is None:
                    status["coords"] = None
                else:
                    if status.get("coords") is None:
                        status["coords"] = {}
                    for coordinate, limit in (("lat", 90), ("lon", 180)):
                        if coordinate in incoming["coords"]:
                            status["coords"][coordinate] = _number(
                                incoming["coords"][coordinate], -limit, limit
                            )
            for field in (
                "location",
                "speed",
                "course",
                "battery",
                "signal",
                "external_voltage",
                "charge",
                "relay",
                "last_updated",
                "status_last_updated",
            ):
                if field in incoming:
                    value = incoming[field]
                    if field in ("last_updated", "status_last_updated"):
                        value = _date(value)
                    elif field in ("charge", "relay"):
                        value = value if isinstance(value, bool) else None
                    elif field == "location":
                        normalized = normalize_tracker(raw)
                        lat, lon = normalized["latitude"], normalized["longitude"]
                        value = f"{lat},{lon}" if lat is not None and lon is not None else None
                    else:
                        bounds = {
                            "speed": (0, None),
                            "course": (0, 360),
                            "battery": (0, 6),
                            "signal": (0, 100),
                        }
                        value = _number(value, *bounds.get(field, (None, None)))
                    status[field] = value
        self._raw = updated


def _number(value, low=None, high=None):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        finite = math.isfinite(value)
    except OverflowError:
        return None
    if not finite or (low is not None and value < low) or (high is not None and value > high):
        return None
    return value


def normalize_tracker(raw):
    _validate([raw])
    status = raw.get("status") or {}
    coords = status.get("coords") or {}
    lat, lon = _number(coords.get("lat"), -90, 90), _number(coords.get("lon"), -180, 180)
    if "coords" not in status and isinstance(status.get("location"), str):
        try:
            a, b = status["location"].split(",")
            lat, lon = _number(float(a), -90, 90), _number(float(b), -180, 180)
        except ValueError:
            lat = lon = None
    if lat is None or lon is None:
        lat = lon = None
    speed = _number(status.get("speed"), 0)
    return dict(
        device_id=raw.get("_id"),
        latitude=lat,
        longitude=lon,
        speed_mph=speed / 1.609344 if speed is not None else None,
        course=_number(status.get("course"), 0, 360),
        battery_level=_number(status.get("battery"), 0, 6),
        signal_percent=_number(status.get("signal"), 0, 100),
        external_voltage=_number(status.get("external_voltage")),
        charging=status.get("charge") if isinstance(status.get("charge"), bool) else None,
        relay=status.get("relay") if isinstance(status.get("relay"), bool) else None,
        last_updated=_date(status.get("last_updated")),
        status_last_updated=_date(status.get("status_last_updated")),
        model=_number(raw.get("model")),
        product=_number(raw.get("product")),
    )
