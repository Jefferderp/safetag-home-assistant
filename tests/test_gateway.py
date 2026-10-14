"""Gateway tests use only synthetic cached telemetry."""

import importlib
import importlib.util
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

TOKEN = "synthetic-local-token-" + "x" * 32


def gateway():
    sys.path.insert(0, str(Path(__file__).parents[1]))
    assert importlib.util.find_spec("gateway.app"), "gateway missing"
    return importlib.import_module("gateway.app")


def test_gateway_authenticates_snapshot_and_openapi_without_raw_ids():
    module = gateway()
    store = module.TrackerStore()
    store.replace([{"_id": "synthetic-1", "owner": "not-stored", "status": {"location": "12,24"}}])
    app = module.create_app(token=TOKEN, store=store, start_worker=False)
    with TestClient(app) as client:
        assert client.get("/health").json() == {"status": "ok"}
        for path in ["/v1/trackers", "/v1/status", "/openapi.json", "/docs"]:
            assert client.get(path).status_code in (401, 404)
        headers = {"Authorization": f"Bearer {TOKEN}"}
        response = client.get("/v1/trackers", headers=headers)
        assert response.status_code == 200
        assert "synthetic-1" not in response.text
        tracker = response.json()[0]
        assert len(tracker["device_id"]) == 64
        assert tracker["latitude"] == 12
        assert "owner" not in tracker
        assert client.get("/v1/trackers/" + tracker["device_id"], headers=headers).json() == tracker
        assert client.get("/v1/trackers/missing", headers=headers).status_code == 404
        assert client.get("/v1/trackers/" + tracker["device_id"]).status_code == 401
        assert client.post("/v1/trackers", headers=headers).status_code == 405
        spec = client.get("/openapi.json", headers=headers).json()
        assert "/v1/trackers" in spec["paths"]
        assert spec["paths"]["/v1/status"]["get"]["responses"]["200"]["content"][
            "application/json"
        ]["schema"] == {"$ref": "#/components/schemas/GatewayStatus"}
        assert spec["components"]["securitySchemes"]["HTTPBearer"]["scheme"] == "bearer"
        assert client.get("/v1/status", headers=headers).json()["connected"] is False


@pytest.mark.parametrize("fail_construction", [False, True], ids=["success", "rollback"])
def test_gateway_readers_see_complete_snapshot_during_replace(fail_construction):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    module = gateway()
    store = module.TrackerStore()
    store.replace([
        {"_id": "retained", "status": {"battery": 1}},
        {"_id": "removed", "status": {"battery": 2}},
    ])
    replacement = [
        {"_id": "retained", "status": {"battery": 4}},
        {"_id": "added", "status": {"battery": 5}},
    ]
    entered, resume = Event(), Event()

    def trace(frame, event, arg):
        # Pause real normalization of the last record after the first is built.
        # Only the replacement thread is traced; HTTP readers run normally.
        if (
            event == "call"
            and frame.f_code is module._api._number.__code__
            and frame.f_locals == {"value": 5, "low": 0, "high": 6}
        ):
            entered.set()
            assert resume.wait(5), "Replacement barrier was not released"
            if fail_construction:
                raise ValueError("synthetic construction failure")
        return trace

    def replace():
        sys.settrace(trace)
        try:
            store.replace(replacement)
        finally:
            sys.settrace(None)

    app = module.create_app(token=TOKEN, store=store, start_worker=False)
    headers = {"Authorization": f"Bearer {TOKEN}"}
    with TestClient(app) as client, ThreadPoolExecutor(max_workers=1) as executor:
        before = client.get("/v1/trackers", headers=headers)
        assert before.status_code == 200
        old_snapshot = before.json()
        assert len(old_snapshot) == 2
        pending = executor.submit(replace)
        try:
            assert entered.wait(5), "Replacement did not reach normalization barrier"
            during = client.get("/v1/trackers", headers=headers)
            details = [
                client.get("/v1/trackers/" + item["device_id"], headers=headers)
                for item in old_snapshot
            ]
        finally:
            resume.set()
        if fail_construction:
            with pytest.raises(ValueError, match="synthetic construction failure"):
                pending.result(timeout=5)
        else:
            pending.result(timeout=5)
        assert during.status_code == 200
        assert during.json() == old_snapshot
        for response, item in zip(details, old_snapshot, strict=True):
            assert response.status_code == 200
            assert response.json() == item
        after = client.get("/v1/trackers", headers=headers)
        assert after.status_code == 200
        if fail_construction:
            assert after.json() == old_snapshot
        else:
            assert [item["battery_level"] for item in after.json()] == [4, 5]
            assert after.json()[0]["device_id"] == old_snapshot[0]["device_id"]
            assert after.json()[1]["device_id"] != old_snapshot[1]["device_id"]
            assert client.get(
                "/v1/trackers/" + old_snapshot[1]["device_id"], headers=headers
            ).status_code == 404


@pytest.mark.asyncio
async def test_worker_reconciles_each_reconnect_with_bounded_backoff():
    import asyncio

    module = gateway()
    assert hasattr(module, "run_worker"), "cache worker missing"
    app = module.create_app(token=TOKEN, start_worker=False)

    class Client:
        snapshots = 0
        streams = 0

        async def get_trackers(self):
            self.snapshots += 1
            return [{"_id": "synthetic-1", "status": {"battery": 3}}]

        async def stream(self):
            self.streams += 1
            if self.streams == 3:
                yield [{"_id": "synthetic-1", "status": {"battery": 4}}]
            raise module._api.SafetagConnectionError("generic")

    client = Client()
    delays = []

    async def sleep(delay):
        delays.append(delay)
        if len(delays) == 3:
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await module.run_worker(app, client, sleep=sleep)
    assert client.snapshots == 3
    assert client.streams == 3
    assert delays == [3, 6, 3]
    assert app.state.store.trackers["synthetic-1"]["battery_level"] == 4
    assert app.state.last_received is not None
    assert app.state.connected is False


@pytest.mark.asyncio
async def test_worker_reconciles_offline_changes_before_partial_reconnect_event():
    import asyncio

    module = gateway()
    app = module.create_app(token=TOKEN, start_worker=False)
    old_timestamp = "2026-01-01T00:00:00+00:00"
    new_timestamp = "2026-01-02T00:00:00+00:00"

    class Client:
        snapshots = 0
        streams = 0

        async def get_trackers(self):
            self.snapshots += 1
            assert app.state.connected is False
            if self.snapshots == 1:
                return [
                    {
                        "_id": "synthetic-1",
                        "model": 1,
                        "product": 2,
                        "status": {
                            "coords": {"lat": 12, "lon": 24},
                            "relay": False,
                            "last_updated": old_timestamp,
                            "status_last_updated": old_timestamp,
                        },
                    },
                    {"_id": "removed-offline", "status": {"battery": 2}},
                ]
            return [{
                "_id": "synthetic-1",
                "model": 3,
                "product": 4,
                "status": {
                    "coords": {"lat": 13, "lon": 25},
                    "relay": True,
                    "last_updated": new_timestamp,
                    "status_last_updated": new_timestamp,
                },
            }]

        async def stream(self):
            self.streams += 1
            assert app.state.connected is False
            yield [{"_id": "synthetic-1", "status": {"battery": 4}}]
            assert app.state.connected is True
            if self.streams == 2:
                trackers = app.state.store.trackers
                tracker = trackers["synthetic-1"]
                assert (tracker["latitude"], tracker["longitude"]) == (13, 25)
                assert tracker["relay"] is True
                assert tracker["last_updated"] == new_timestamp
                assert tracker["status_last_updated"] == new_timestamp
                assert (tracker["model"], tracker["product"]) == (3, 4)
                assert tracker["battery_level"] == 4
                assert set(trackers) == {"synthetic-1"}
                raise asyncio.CancelledError
            raise module._api.SafetagConnectionError("generic")

    client = Client()
    delays = []

    async def sleep(delay):
        delays.append(delay)
        assert app.state.connected is False

    with pytest.raises(asyncio.CancelledError):
        await module.run_worker(app, client, sleep=sleep)
    assert client.snapshots == 2
    assert client.streams == 2
    assert delays == [3]
    assert app.state.connected is False


@pytest.mark.asyncio
async def test_worker_healthy_stream_does_not_poll_snapshots():
    import asyncio

    module = gateway()
    app = module.create_app(token=TOKEN, start_worker=False)

    class Client:
        snapshots = 0

        async def get_trackers(self):
            self.snapshots += 1
            return [{"_id": "synthetic-1", "status": {"battery": 3}}]

        async def stream(self):
            for battery in (4, 5, 6):
                yield [{"_id": "synthetic-1", "status": {"battery": battery}}]
                assert app.state.connected is True
                assert app.state.store.trackers["synthetic-1"]["battery_level"] == battery
                assert self.snapshots == 1
            raise asyncio.CancelledError

    async def sleep(delay):
        pytest.fail("Healthy stream must not reconnect")

    client = Client()
    with pytest.raises(asyncio.CancelledError):
        await module.run_worker(app, client, sleep=sleep)
    assert client.snapshots == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["connection", "schema", "rate_limit", "authentication"])
async def test_worker_failed_resync_preserves_cache_and_freshness(failure):
    import asyncio

    module = gateway()
    app = module.create_app(token=TOKEN, start_worker=False)
    previous_cache = None
    previous_received = None

    class Client:
        snapshots = 0
        streams = 0

        async def get_trackers(self):
            self.snapshots += 1
            assert app.state.connected is False
            if self.snapshots == 1:
                return [{"_id": "synthetic-1", "status": {"location": "12,24", "relay": True}}]
            if failure == "schema":
                return [{"_id": "replacement"}, {"_id": "invalid", "status": "invalid"}]
            if failure == "connection":
                raise module._api.SafetagConnectionError("generic")
            if failure == "rate_limit":
                raise module._api.SafetagRateLimitError(37)
            raise module._api.SafetagAuthError("generic")

        async def stream(self):
            nonlocal previous_cache, previous_received
            self.streams += 1
            yield [{"_id": "synthetic-1", "status": {"battery": 4}}]
            previous_cache = app.state.store.trackers
            previous_received = app.state.last_received
            raise module._api.SafetagConnectionError("generic")

    delays = []

    async def sleep(delay):
        delays.append(delay)
        assert app.state.connected is False
        if len(delays) == 2:
            raise asyncio.CancelledError

    client = Client()
    if failure == "authentication":
        await module.run_worker(app, client, sleep=sleep)
        assert delays == [3]
    else:
        with pytest.raises(asyncio.CancelledError):
            await module.run_worker(app, client, sleep=sleep)
        assert delays == [3, 37 if failure == "rate_limit" else 6]
    assert client.snapshots == 2
    assert client.streams == 1
    assert app.state.store.trackers == previous_cache
    assert app.state.last_received == previous_received
    assert app.state.connected is False
    assert app.state.last_error == {
        "connection": "upstream", "schema": "upstream",
        "rate_limit": "rate_limit", "authentication": "authentication",
    }[failure]


@pytest.mark.asyncio
async def test_worker_does_not_retry_rejected_credentials():
    module = gateway()
    app = module.create_app(token=TOKEN, start_worker=False)

    class Client:
        async def get_trackers(self):
            raise module._api.SafetagAuthError("generic")

    await module.run_worker(app, Client())
    assert app.state.last_error == "authentication"


def test_cli_defaults_to_loopback_without_access_logs(monkeypatch):
    module = gateway()
    assert (Path(module.__file__).parent / "__main__.py").exists(), "CLI missing"
    import uvicorn

    cli = importlib.import_module("gateway.__main__")
    calls = []
    monkeypatch.setattr(uvicorn, "run", lambda *args, **kwargs: calls.append((args, kwargs)))
    monkeypatch.delenv("SAFETAG_GATEWAY_HOST", raising=False)
    monkeypatch.delenv("SAFETAG_GATEWAY_PORT", raising=False)
    cli.main()
    args, options = calls[0]
    assert args == ("gateway.app:create_app",)
    assert options == dict(
        factory=True, host="127.0.0.1", port=80, access_log=False, log_level="critical"
    )


@pytest.mark.asyncio
async def test_lifespan_starts_and_cancels_background_worker(monkeypatch):
    import asyncio

    module = gateway()
    monkeypatch.setenv("SAFETAG_USERNAME", "account@example.invalid")
    monkeypatch.setenv("SAFETAG_API_KEY", "placeholder-upstream")
    entered, stopped = asyncio.Event(), asyncio.Event()

    async def worker(app, client):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    monkeypatch.setattr(module, "run_worker", worker)
    app = module.create_app(token=TOKEN)
    async with app.router.lifespan_context(app):
        await asyncio.wait_for(entered.wait(), 1)
    assert stopped.is_set()


def test_gateway_environment_credentials_are_required_and_separate(monkeypatch):
    module = gateway()
    monkeypatch.setenv("SAFETAG_GATEWAY_TOKEN", TOKEN)
    monkeypatch.delenv("SAFETAG_USERNAME", raising=False)
    monkeypatch.delenv("SAFETAG_API_KEY", raising=False)
    with pytest.raises(ValueError, match="credentials"):
        module.create_app()
    monkeypatch.setenv("SAFETAG_USERNAME", "account@example.invalid")
    monkeypatch.setenv("SAFETAG_API_KEY", TOKEN)
    with pytest.raises(ValueError, match="different"):
        module.create_app()


@pytest.mark.parametrize("token", ["", "too-short"])
def test_gateway_fails_closed_for_weak_tokens(token):
    with pytest.raises(ValueError, match="32"):
        gateway().create_app(token=token, start_worker=False)
