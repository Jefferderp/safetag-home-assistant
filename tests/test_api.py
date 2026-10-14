"""Synthetic, offline tests for the reusable API boundary."""

import importlib.util
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event

import pytest


@pytest.mark.parametrize("fail_build", [False, True])
def test_replace_keeps_old_snapshot_visible_until_publication(monkeypatch, fail_build):
    module = api()
    store = module.TrackerStore()
    store.replace([{"_id": "synthetic-old", "status": {"battery": 3}}])
    old_snapshot = store.trackers
    replacement = [
        {"_id": "synthetic-new-1", "model": 41},
        {"_id": "synthetic-new-2", "model": 42},
    ]
    building = Event()
    resume = Event()
    number = module._number

    def pause_during_build(value, *bounds):
        if value == 42:
            # Pause after one replacement record has already been built.
            building.set()
            assert resume.wait(5), "replacement was not released"
            if fail_build:
                raise module.SafetagResponseError("Invalid tracker response")
        return number(value, *bounds)

    monkeypatch.setattr(module, "_number", pause_during_build)
    with ThreadPoolExecutor(max_workers=1) as executor:
        writer = executor.submit(store.replace, replacement)
        try:
            assert building.wait(5), "replacement did not reach the build gate"
            # This reader runs concurrently with the paused cache writer.
            assert store.trackers == old_snapshot
        finally:
            resume.set()
        if fail_build:
            with pytest.raises(module.SafetagResponseError):
                writer.result(timeout=5)
            assert store.trackers == old_snapshot
        else:
            writer.result(timeout=5)
            assert store.trackers == {
                record["_id"]: module.normalize_tracker(record) for record in replacement
            }
    assert old_snapshot["synthetic-old"]["battery_level"] == 3


def test_store_merges_partial_updates_without_stale_coordinate_sources():
    module = api()
    assert hasattr(module, "TrackerStore"), "store missing"
    store = module.TrackerStore()
    store.replace(
        [{"_id": "synthetic-1", "status": {"coords": {"lat": 12, "lon": 24}, "battery": 3}}]
    )
    store.update([{"_id": "synthetic-1", "status": {"coords": {"lat": 13}}}])
    assert store.trackers["synthetic-1"]["longitude"] == 24
    store.update([{"_id": "synthetic-1", "status": {"location": "14,25"}}])
    assert store.trackers["synthetic-1"]["latitude"] == 14
    store.update([{"_id": "synthetic-1", "status": {"coords": {"lon": 26}}}])
    assert store.trackers["synthetic-1"]["latitude"] is None
    assert store.trackers["synthetic-1"]["battery_level"] == 3
    before = store.trackers
    with pytest.raises(module.SafetagResponseError):
        store.update(
            [{"_id": "synthetic-1", "status": {"speed": 50}}, {"_id": "bad", "status": []}]
        )
    assert store.trackers == before
    before["synthetic-1"]["battery_level"] = 99
    assert store.trackers["synthetic-1"]["battery_level"] == 3


@pytest.mark.parametrize(
    "status",
    [
        {"coords": {"lat": True, "lon": 20}},
        {"location": "91,20"},
        {"location": "nan,20"},
        {"coords": {"lat": 12}},
        {"location": "12,20,30"},
    ],
)
def test_invalid_coordinate_pairs_are_unknown(status):
    result = api().normalize_tracker({"_id": "synthetic-1", "status": status})
    assert result["latitude"] is None and result["longitude"] is None


def test_string_coordinates_and_invalid_values():
    result = api().normalize_tracker(
        {
            "_id": "synthetic-1",
            "last_updated": "not-a-date",
            "status": {"location": "12,24", "battery": True, "speed": float("inf"), "signal": 101},
        }
    )
    assert result["latitude"] == 12
    assert result["longitude"] == 24
    assert result["last_updated"] is None
    assert result["speed_mph"] is None
    assert result["battery_level"] is None
    assert result["signal_percent"] is None


def api():
    path = Path(__file__).parents[1] / "custom_components/safetag/api.py"
    assert path.exists(), "reusable API module is missing"
    spec = importlib.util.spec_from_file_location("safetag_test_api", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_normalizes_telemetry_without_personal_metadata():
    result = api().normalize_tracker(
        {
            "_id": "synthetic-1",
            "name": "ignored",
            "model": 12,
            "product": 3,
            "status": {
                "coords": {"lat": 12.5, "lon": -24.5},
                "speed": 16.09344,
                "course": 90,
                "battery": 5,
                "signal": 80,
                "external_voltage": 12.4,
                "charge": True,
                "relay": False,
            },
        }
    )
    assert result == dict(
        device_id="synthetic-1",
        latitude=12.5,
        longitude=-24.5,
        speed_mph=10,
        course=90,
        battery_level=5,
        signal_percent=80,
        external_voltage=12.4,
        charging=True,
        relay=False,
        last_updated=None,
        status_last_updated=None,
        model=12,
        product=3,
    )


@pytest.mark.enable_socket
@pytest.mark.allow_hosts(["127.0.0.1"])
@pytest.mark.asyncio
async def test_stream_reads_arrays_and_blocks_redirects(monkeypatch, socket_enabled):
    import aiohttp
    from aiohttp import web

    module = api()
    assert hasattr(module.SafetagClient, "stream"), "stream missing"
    received = []

    async def socket(request):
        received.append(dict(request.headers))
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.send_json([{"_id": "synthetic-1", "status": {"battery": 4}}])
        await ws.close()
        return ws

    async def redirect(request):
        raise web.HTTPFound("/leak")

    async def leak(request):
        received.append("leaked")
        return web.Response()

    app = web.Application()
    app.router.add_get("/stream", socket)
    app.router.add_get("/redirect", redirect)
    app.router.add_get("/leak", leak)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    try:
        async with aiohttp.ClientSession() as session:
            client = module.SafetagClient(session, "account@example.invalid", "placeholder")
            monkeypatch.setattr(module, "STREAM_URL", f"ws://127.0.0.1:{port}/stream")
            assert [batch async for batch in client.stream()] == [
                [{"_id": "synthetic-1", "status": {"battery": 4}}]
            ]
            assert received[0]["X-API-KEY"] == "placeholder"
            assert received[0]["username"] == "account@example.invalid"
            monkeypatch.setattr(module, "STREAM_URL", f"ws://127.0.0.1:{port}/redirect")
            with pytest.raises(module.SafetagResponseError):
                async for _ in client.stream():
                    pass
            assert "leaked" not in received
    finally:
        await runner.cleanup()


def test_store_preserves_nested_timezone_aware_timestamps():
    module = api()
    store = module.TrackerStore()
    store.replace(
        [
            {
                "_id": "synthetic-1",
                "status": {
                    "last_updated": "2026-01-01T12:00:00Z",
                    "status_last_updated": "2026-01-01T12:01:00+00:00",
                },
            }
        ]
    )
    assert store.trackers["synthetic-1"]["last_updated"] == "2026-01-01T12:00:00Z"
    store.update(
        [{"_id": "synthetic-1", "status": {"status_last_updated": "2026-01-01T12:02:00Z"}}]
    )
    assert store.trackers["synthetic-1"]["last_updated"] == "2026-01-01T12:00:00Z"
    assert store.trackers["synthetic-1"]["status_last_updated"] == "2026-01-01T12:02:00Z"
    store.update([{"_id": "synthetic-1", "status": {"last_updated": "2026-01-01T12:00:00"}}])
    assert store.trackers["synthetic-1"]["last_updated"] is None


def test_null_status_and_coords_are_unknown_and_clear_previous_fix():
    module = api()
    store = module.TrackerStore()
    store.replace([{"_id": "synthetic-1", "status": None}])
    assert store.trackers["synthetic-1"]["latitude"] is None
    store.update([{"_id": "synthetic-1", "status": {"coords": {"lat": 12, "lon": 24}}}])
    store.update([{"_id": "synthetic-1", "status": {"coords": None}}])
    assert store.trackers["synthetic-1"]["latitude"] is None
    store.update([{"_id": "synthetic-1", "status": {"location": "12,24"}}])
    store.update([{"_id": "synthetic-1", "status": None}])
    assert store.trackers["synthetic-1"]["latitude"] is None


@pytest.mark.asyncio
async def test_client_obeys_20_http_requests_per_minute(monkeypatch):
    module = api()
    now = [0.0]
    delays = []

    async def sleep(delay):
        delays.append(delay)
        now[0] += delay

    monkeypatch.setattr(module, "_clock", lambda: now[0], raising=False)
    monkeypatch.setattr(module.asyncio, "sleep", sleep)
    client = module.SafetagClient(Session([Response([]) for _ in range(21)]), "x", "y")
    for _ in range(21):
        await client.get_trackers()
    assert delays == [60]


def test_normalizer_rejects_malformed_schema_with_safe_error():
    module = api()
    with pytest.raises(module.SafetagResponseError):
        module.normalize_tracker({"_id": "synthetic-1", "status": ["unsafe"]})


def test_huge_number_is_unknown_not_an_overflow():
    assert (
        api().normalize_tracker({"_id": "synthetic-1", "status": {"speed": 10**1000}})["speed_mph"]
        is None
    )


@pytest.mark.asyncio
async def test_http_forces_certificate_and_hostname_validation():
    import ssl

    session = Session([Response([])])
    await api().SafetagClient(session, "x", "y").get_trackers()
    context = session.calls[0][1]["ssl"]
    assert isinstance(context, ssl.SSLContext)
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname


@pytest.mark.asyncio
async def test_client_caps_concurrent_sockets_at_five(socket_enabled, aiohttp_server, monkeypatch):
    import asyncio

    import aiohttp
    from aiohttp import web

    module = api()
    sockets = []

    async def handler(request):
        socket = web.WebSocketResponse()
        await socket.prepare(request)
        sockets.append(socket)
        await socket.send_json([])
        await socket.receive()
        return socket

    app = web.Application()
    app.router.add_get("/stream", handler)
    server = await aiohttp_server(app)
    monkeypatch.setattr(module, "STREAM_URL", str(server.make_url("/stream")))
    async with aiohttp.ClientSession() as session:
        client = module.SafetagClient(session, "x", "y")
        streams = [client.stream() for _ in range(6)]
        tasks = [asyncio.create_task(anext(stream)) for stream in streams]
        try:
            await asyncio.gather(*tasks[:5])
            await asyncio.sleep(0.02)
            assert len(sockets) == 5
            assert not tasks[5].done()
            await streams[0].aclose()
            assert await asyncio.wait_for(tasks[5], 1) == []
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for stream in streams:
                await stream.aclose()


def test_store_does_not_retain_metadata_inside_coordinate_objects():
    module = api()
    store = module.TrackerStore()
    store.replace(
        [
            {
                "_id": "synthetic-1",
                "owner": "not-stored",
                "status": {
                    "coords": {"lat": 12, "lon": 24, "label": "not-stored"},
                    "battery": {"label": "not-stored"},
                },
            }
        ]
    )
    assert "not-stored" not in repr(store.__dict__)
    assert store.trackers["synthetic-1"]["battery_level"] is None


class Response:
    def __init__(self, data, status=200, headers=None):
        self.data, self.status, self.headers = data, status, headers or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def json(self):
        return self.data


class Session:
    def __init__(self, responses):
        self.responses, self.calls = list(responses), []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses.pop(0)


@pytest.mark.asyncio
async def test_http_paginates_exact_500_and_protects_credentials():
    module = api()
    assert hasattr(module, "SafetagClient"), "HTTP client missing"
    session = Session(
        [
            Response([{"_id": f"synthetic-{i}"} for i in range(500)]),
            Response([{"_id": "synthetic-last"}]),
        ]
    )
    result = await module.SafetagClient(
        session, "account+test@example.invalid", "placeholder"
    ).get_trackers()
    assert len(result) == 501
    url, options = session.calls[0]
    assert (
        str(url)
        == "https://api.safetagtracking.com/api/v1/devices/account%2Btest%40example.invalid?page=1"
    )
    assert options["headers"] == {"X-API-KEY": "placeholder"}
    assert options["allow_redirects"] is False
    assert options["timeout"].total == 20
    assert session.calls[1][0].endswith("?page=2")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,error",
    [
        (401, "SafetagAuthError"),
        (403, "SafetagAuthError"),
        (429, "SafetagRateLimitError"),
        (302, "SafetagResponseError"),
        (500, "SafetagConnectionError"),
    ],
)
async def test_http_safe_typed_errors(status, error):
    module = api()
    session = Session([Response("sensitive-body", status, {"Retry-After": "17"})])
    with pytest.raises(getattr(module, error)) as raised:
        await module.SafetagClient(session, "account@example.invalid", "placeholder").get_trackers()
    assert "sensitive" not in str(raised.value)
    if status == 429:
        assert raised.value.retry_after == 17


@pytest.mark.asyncio
async def test_http_rejects_schema_and_limits_pages(monkeypatch):
    module = api()
    with pytest.raises(module.SafetagResponseError):
        await module.SafetagClient(Session([Response({"devices": []})]), "x", "y").get_trackers()
    monkeypatch.setattr(module, "MAX_PAGES", 1)
    with pytest.raises(module.SafetagResponseError):
        await module.SafetagClient(
            Session([Response([{"_id": "synthetic"}] * 500)]), "x", "y"
        ).get_trackers()
