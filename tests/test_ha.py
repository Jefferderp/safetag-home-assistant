"""Executable HA 2025.3 integration contract tests."""

from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.config_entries import SOURCE_USER


async def test_flow_manager_registers_user_form(hass):
    result = await hass.config_entries.flow.async_init("safetag", context={"source": SOURCE_USER})
    assert result["type"] == "form"
    assert result["step_id"] == "user"
    assert set(result["data_schema"].schema) == {"username", "api_key"}


async def test_flow_validates_snapshot_and_deduplicates_without_rotating(hass):
    from hashlib import sha256

    credentials = {"username": "Tester@example.invalid", "api_key": "test-only-key"}
    client = AsyncMock()
    client.get_trackers.return_value = [{"_id": "synthetic-tracker", "status": {}}]
    with (
        patch(
            "custom_components.safetag.config_flow.SafetagClient", return_value=client, create=True
        ),
        patch("custom_components.safetag.async_setup_entry", return_value=True, create=True),
    ):
        result = await hass.config_entries.flow.async_init(
            "safetag", context={"source": SOURCE_USER}, data=credentials
        )
        assert result["type"] == "create_entry"
        assert result["title"] == "Safetag"
        assert result["result"].unique_id == sha256(b"tester@example.invalid").hexdigest()
        client.get_trackers.assert_awaited_once()
        duplicate = await hass.config_entries.flow.async_init(
            "safetag",
            context={"source": SOURCE_USER},
            data={**credentials, "api_key": "different-test-key"},
        )
        assert duplicate["type"] == "abort"
        assert duplicate["reason"] == "already_configured"
        assert result["result"].data["api_key"] == "test-only-key"


@pytest.mark.parametrize(
    "failure,expected",
    [
        ("auth", "invalid_auth"),
        ("connection", "cannot_connect"),
        ("response", "invalid_response"),
        ("rate", "cannot_connect"),
        ("unexpected", "unknown"),
        ("malformed", "invalid_response"),
    ],
)
async def test_flow_validation_errors(hass, failure, expected):
    from custom_components.safetag.api import (
        SafetagAuthError,
        SafetagConnectionError,
        SafetagRateLimitError,
        SafetagResponseError,
    )

    client = AsyncMock()
    errors = {
        "auth": SafetagAuthError(),
        "connection": SafetagConnectionError(),
        "response": SafetagResponseError(),
        "rate": SafetagRateLimitError(30),
        "unexpected": RuntimeError("do-not-log-private-body"),
    }
    if failure == "malformed":
        client.get_trackers.return_value = [{}]
    else:
        client.get_trackers.side_effect = errors[failure]
    with patch("custom_components.safetag.config_flow.SafetagClient", return_value=client):
        result = await hass.config_entries.flow.async_init(
            "safetag",
            context={"source": SOURCE_USER},
            data={"username": "tester@example.invalid", "api_key": "test-only-key"},
        )
    assert result["type"] == "form"
    assert result["errors"] == {"base": expected}


@pytest.mark.parametrize(
    "source,step,reason",
    [
        ("reauth", "reauth_confirm", "reauth_successful"),
        ("reconfigure", "reconfigure", "reconfigure_successful"),
    ],
)
async def test_credential_rotation(hass, source, step, reason):
    from hashlib import sha256

    from pytest_homeassistant_custom_component.common import MockConfigEntry

    entry = MockConfigEntry(
        domain="safetag",
        unique_id=sha256(b"tester@example.invalid").hexdigest(),
        data={"username": "tester@example.invalid", "api_key": "old-test-key"},
    )
    entry.add_to_hass(hass)
    client = AsyncMock()
    client.get_trackers.return_value = []
    with (
        patch("custom_components.safetag.config_flow.SafetagClient", return_value=client),
        patch.object(hass.config_entries, "async_reload", return_value=True) as reload,
    ):
        result = await hass.config_entries.flow.async_init(
            "safetag",
            context={"source": source, "entry_id": entry.entry_id},
            data=entry.data if source == "reauth" else None,
        )
        assert result["type"] == "form"
        assert result["step_id"] == step
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {"api_key": "new-test-key"}
        )
        assert result["type"] == "abort"
        assert result["reason"] == reason
        assert entry.data == {"username": "tester@example.invalid", "api_key": "new-test-key"}
        reload.assert_awaited_once_with(entry.entry_id)


async def test_setup_snapshot_push_coordinator_and_unload(hass):
    import asyncio

    from pytest_homeassistant_custom_component.common import MockConfigEntry

    from custom_components.safetag import async_setup_entry, async_unload_entry

    entry = MockConfigEntry(
        domain="safetag",
        unique_id="synthetic-account-hash",
        data={"username": "tester@example.invalid", "api_key": "test-only-key"},
    )
    entry.add_to_hass(hass)
    from homeassistant.config_entries import ConfigEntryState

    entry.mock_state(hass, ConfigEntryState.SETUP_IN_PROGRESS)
    client = AsyncMock()
    client.get_trackers.return_value = [
        {"_id": "synthetic-tracker", "status": {"coords": {"lat": 0, "lon": 0}}}
    ]
    closed = asyncio.Event()

    async def stream():
        try:
            await asyncio.Event().wait()
            yield []
        finally:
            closed.set()

    client.stream = stream
    with (
        patch("custom_components.safetag.SafetagClient", return_value=client),
        patch.object(hass.config_entries, "async_forward_entry_setups", return_value=None),
        patch.object(hass.config_entries, "async_unload_platforms", return_value=True),
    ):
        assert await async_setup_entry(hass, entry)
        coordinator = entry.runtime_data
        assert coordinator.data["synthetic-tracker"]["latitude"] == 0
        assert coordinator.update_interval is None
        await asyncio.sleep(0)
        assert await async_unload_entry(hass, entry)
        assert closed.is_set()
        assert coordinator.stream_task.done()
        client.get_trackers.assert_awaited_once()


async def test_real_platform_setup_units_identity_and_dynamic_discovery(hass):
    import asyncio
    from hashlib import sha256

    from homeassistant.helpers import device_registry as dr
    from homeassistant.helpers import entity_registry as er
    from pytest_homeassistant_custom_component.common import MockConfigEntry

    entry = MockConfigEntry(
        domain="safetag",
        unique_id=sha256(b"tester@example.invalid").hexdigest(),
        data={"username": "tester@example.invalid", "api_key": "test-only-key"},
    )
    entry.add_to_hass(hass)
    client = AsyncMock()
    client.get_trackers.return_value = [
        {
            "_id": "synthetic-tracker",
            "name": "PRIVATE UPSTREAM NAME",
            "model": 1,
            "product": 2,
            "status": {
                "last_updated": "2025-03-01T12:00:00Z",
                "status_last_updated": "2025-03-01T12:01:00Z",
                "coords": {"lat": 0, "lon": 0},
                "speed": 16.09344,
                "course": 45,
                "battery": 3,
                "signal": 65,
                "external_voltage": 12.7,
                "charge": True,
                "relay": False,
            },
        }
    ]

    async def stream():
        await asyncio.Event().wait()
        yield []

    client.stream = stream
    with patch("custom_components.safetag.SafetagClient", return_value=client):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        registry = er.async_get(hass)
        entries = er.async_entries_for_config_entry(registry, entry.entry_id)
        assert len(entries) == 12
        by_metric = {
            metric: hass.states.get(
                next(item.entity_id for item in entries if item.unique_id.endswith("_" + metric))
            )
            for metric in (
                "speed_mph",
                "course",
                "battery_level",
                "signal_percent",
                "external_voltage",
                "last_updated",
                "status_last_updated",
                "model",
                "product",
                "charging",
                "relay",
                "location",
            )
        }
        assert float(by_metric["speed_mph"].state) == pytest.approx(10)
        assert by_metric["speed_mph"].attributes["unit_of_measurement"] == "mph"
        assert by_metric["course"].attributes["unit_of_measurement"] == "°"
        assert by_metric["battery_level"].state == "3"
        assert "unit_of_measurement" not in by_metric["battery_level"].attributes
        assert by_metric["signal_percent"].attributes["unit_of_measurement"] == "%"
        assert by_metric["external_voltage"].attributes["unit_of_measurement"] == "V"
        assert by_metric["external_voltage"].attributes["device_class"] == "voltage"
        assert by_metric["last_updated"].attributes["device_class"] == "timestamp"
        assert by_metric["charging"].state == "on"
        assert by_metric["relay"].state == "off"
        assert by_metric["location"].attributes["source_type"] == "gps"
        assert by_metric["location"].attributes["latitude"] == 0
        assert by_metric["location"].attributes["longitude"] == 0
        assert all("synthetic-tracker" not in item.unique_id for item in entries)
        assert all(
            "PRIVATE UPSTREAM NAME" not in str(hass.states.get(item.entity_id)) for item in entries
        )
        devices = dr.async_entries_for_config_entry(dr.async_get(hass), entry.entry_id)
        assert len(devices) == 1
        assert all(len(identifier) == 2 for identifier in devices[0].identifiers)
        coordinator = entry.runtime_data
        coordinator.store.update([{"_id": "new-synthetic-tracker", "status": {"battery": 4}}])
        coordinator.async_set_updated_data(coordinator.store.trackers)
        await hass.async_block_till_done()
        assert len(er.async_entries_for_config_entry(registry, entry.entry_id)) == 24
        location_id = next(
            item.entity_id for item in entries if item.unique_id.endswith("_location")
        )
        coordinator.connected = False
        from homeassistant.helpers.update_coordinator import UpdateFailed

        coordinator.async_set_update_error(UpdateFailed("Synthetic disconnect"))
        await hass.async_block_till_done()
        assert hass.states.get(location_id).state == "unavailable"
        # HA intentionally omits state attributes while unavailable; our stored fix remains.
        assert coordinator.data["synthetic-tracker"]["latitude"] == 0
        assert coordinator.data["synthetic-tracker"]["last_updated"] == "2025-03-01T12:00:00Z"
        coordinator.connected = True
        coordinator.async_set_updated_data(coordinator.store.trackers)
        await hass.async_block_till_done()
        assert hass.states.get(location_id).attributes["latitude"] == 0
        coordinator.store.replace([{"_id": "new-synthetic-tracker", "status": {"battery": 4}}])
        coordinator.async_set_updated_data(coordinator.store.trackers)
        await hass.async_block_till_done()
        assert hass.states.get(location_id).state == "unavailable"
        assert len(er.async_entries_for_config_entry(registry, entry.entry_id)) == 24
        assert await hass.config_entries.async_unload(entry.entry_id)
        assert coordinator.stream_task.done()


async def test_stream_reconnect_preserves_gps_and_merges_partial(hass):
    import asyncio

    from pytest_homeassistant_custom_component.common import MockConfigEntry

    from custom_components.safetag.api import SafetagConnectionError
    from custom_components.safetag.coordinator import SafetagCoordinator

    entry = MockConfigEntry(domain="safetag", unique_id="synthetic-account-hash")
    from homeassistant.config_entries import ConfigEntryState

    entry.mock_state(hass, ConfigEntryState.SETUP_IN_PROGRESS)
    client = AsyncMock()
    client.get_trackers.return_value = [
        {"_id": "synthetic-tracker", "status": {"coords": {"lat": 0, "lon": 0}, "battery": 3}}
    ]
    attempts = 0
    reconnecting = asyncio.Event()
    resume = asyncio.Event()
    updated = asyncio.Event()

    async def stream():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise SafetagConnectionError("private upstream body")
        yield [{"_id": "synthetic-tracker", "status": {"battery": 4}}]
        updated.set()
        await asyncio.Event().wait()

    client.stream = stream
    coordinator = SafetagCoordinator(hass, entry, client)
    await coordinator.async_config_entry_first_refresh()

    async def pause(delay):
        assert 0 < delay <= 120
        reconnecting.set()
        await resume.wait()

    with patch.object(coordinator, "_wait_to_reconnect", side_effect=pause, create=True):
        coordinator.start()
        await asyncio.wait_for(reconnecting.wait(), 1)
        assert not coordinator.connected
        assert not coordinator.last_update_success
        assert coordinator.data["synthetic-tracker"]["latitude"] == 0
        resume.set()
        await asyncio.wait_for(updated.wait(), 1)
        assert coordinator.connected
        assert coordinator.last_update_success
        assert coordinator.data["synthetic-tracker"]["battery_level"] == 4
        assert coordinator.data["synthetic-tracker"]["longitude"] == 0
        assert client.get_trackers.await_count == 1
        await coordinator.stop()


@pytest.mark.parametrize("phase", ["snapshot", "stream"])
async def test_auth_failure_requires_reauth_once(hass, phase):
    import asyncio

    from homeassistant.exceptions import ConfigEntryAuthFailed
    from pytest_homeassistant_custom_component.common import MockConfigEntry

    from custom_components.safetag.api import SafetagAuthError
    from custom_components.safetag.coordinator import SafetagCoordinator

    entry = MockConfigEntry(domain="safetag", unique_id="synthetic-account-hash")
    from homeassistant.config_entries import ConfigEntryState

    entry.mock_state(hass, ConfigEntryState.SETUP_IN_PROGRESS)
    client = AsyncMock()
    client.get_trackers.return_value = []
    client.get_trackers.side_effect = (
        SafetagAuthError("private upstream body") if phase == "snapshot" else None
    )

    async def stream():
        raise SafetagAuthError("private upstream body")
        yield []

    client.stream = stream
    coordinator = SafetagCoordinator(hass, entry, client)
    if phase == "snapshot":
        with pytest.raises(ConfigEntryAuthFailed, match="Authentication failed"):
            await coordinator.async_config_entry_first_refresh()
    else:
        await coordinator.async_config_entry_first_refresh()
        with patch.object(entry, "async_start_reauth") as reauth:
            coordinator.start()
            await asyncio.wait_for(coordinator.stream_task, 1)
            assert not coordinator.connected
            assert not coordinator.last_update_success
            reauth.assert_called_once_with(hass)
            await coordinator.stop()


async def test_ha_stop_closes_stream(hass):
    import asyncio

    from homeassistant.const import EVENT_HOMEASSISTANT_STOP
    from pytest_homeassistant_custom_component.common import MockConfigEntry

    from custom_components.safetag import async_setup_entry, async_unload_entry

    entry = MockConfigEntry(
        domain="safetag",
        unique_id="synthetic-account-hash",
        data={"username": "tester@example.invalid", "api_key": "test-only-key"},
    )
    entry.add_to_hass(hass)
    from homeassistant.config_entries import ConfigEntryState

    entry.mock_state(hass, ConfigEntryState.SETUP_IN_PROGRESS)
    client = AsyncMock()
    client.get_trackers.return_value = []
    closed = asyncio.Event()

    async def stream():
        try:
            await asyncio.Event().wait()
            yield []
        finally:
            closed.set()

    client.stream = stream
    with (
        patch("custom_components.safetag.SafetagClient", return_value=client),
        patch.object(hass.config_entries, "async_forward_entry_setups", return_value=None),
        patch.object(hass.config_entries, "async_unload_platforms", return_value=True),
    ):
        assert await async_setup_entry(hass, entry)
        hass.bus.async_fire(EVENT_HOMEASSISTANT_STOP)
        await hass.async_block_till_done()
        assert closed.is_set()
        assert entry.runtime_data.stream_task.done()
        assert await async_unload_entry(hass, entry)


async def test_diagnostics_contain_only_counts_connection_and_field_keys(hass):
    import json

    from pytest_homeassistant_custom_component.common import MockConfigEntry

    from custom_components.safetag.coordinator import SafetagCoordinator
    from custom_components.safetag.diagnostics import async_get_config_entry_diagnostics

    entry = MockConfigEntry(
        domain="safetag",
        unique_id="synthetic-account-hash",
        data={"username": "tester@example.invalid", "api_key": "test-only-key"},
    )
    coordinator = SafetagCoordinator(hass, entry, AsyncMock())
    coordinator.store.replace(
        [
            {
                "_id": "private-device-identifier",
                "name": "private-friendly-name",
                "status": {"coords": {"lat": 12.345, "lon": 67.891}, "battery": 3},
            }
        ]
    )
    coordinator.async_set_updated_data(coordinator.store.trackers)
    coordinator.connected = True
    entry.runtime_data = coordinator
    result = await async_get_config_entry_diagnostics(hass, entry)
    assert result == {
        "tracker_count": 1,
        "connected": True,
        "field_keys": ["battery_level", "latitude", "longitude"],
    }
    serialized = json.dumps(result)
    for private in (
        "tester@example.invalid",
        "test-only-key",
        "private-device-identifier",
        "private-friendly-name",
        "12.345",
        "67.891",
    ):
        assert private not in serialized


async def test_reconnect_snapshot_rate_limit_is_honored(hass):
    import asyncio

    from homeassistant.config_entries import ConfigEntryState
    from pytest_homeassistant_custom_component.common import MockConfigEntry

    from custom_components.safetag.api import SafetagConnectionError, SafetagRateLimitError
    from custom_components.safetag.coordinator import SafetagCoordinator

    entry = MockConfigEntry(domain="safetag", unique_id="synthetic-account-hash")
    entry.mock_state(hass, ConfigEntryState.SETUP_IN_PROGRESS)
    client = AsyncMock()
    client.get_trackers.side_effect = [[], SafetagRateLimitError(300)]

    async def stream():
        raise SafetagConnectionError()
        yield []

    client.stream = stream
    coordinator = SafetagCoordinator(hass, entry, client)
    with patch("custom_components.safetag.coordinator.monotonic", side_effect=[0, 61, 61]):
        await coordinator.async_config_entry_first_refresh()
        delays = []

        async def pause(delay):
            delays.append(delay)
            if len(delays) == 2:
                raise asyncio.CancelledError

        with patch.object(coordinator, "_wait_to_reconnect", side_effect=pause):
            coordinator.start()
            with pytest.raises(asyncio.CancelledError):
                await coordinator.stream_task
        assert delays[1] >= 300
        assert client.get_trackers.await_count == 2


def test_translations_match_flow_and_entity_keys():
    import json
    from pathlib import Path

    from custom_components.safetag.binary_sensor import SENSORS as binaries
    from custom_components.safetag.sensor import SENSORS as sensors

    root = Path(__file__).resolve().parents[1] / "custom_components" / "safetag"
    strings = json.loads((root / "strings.json").read_text())
    assert strings == json.loads((root / "translations" / "en.json").read_text())
    assert set(strings["config"]["step"]) == {"user", "reauth_confirm", "reconfigure"}
    assert set(strings["entity"]["sensor"]) == {desc.key for desc in sensors}
    assert set(strings["entity"]["binary_sensor"]) == {desc.key for desc in binaries}
    assert "location" in strings["entity"]["device_tracker"]


@pytest.mark.parametrize(
    "source,step", [("reauth", "reauth_confirm"), ("reconfigure", "reconfigure")]
)
async def test_rejected_rotation_preserves_working_key(hass, source, step):
    from pytest_homeassistant_custom_component.common import MockConfigEntry

    from custom_components.safetag.api import SafetagAuthError

    entry = MockConfigEntry(
        domain="safetag",
        unique_id="synthetic-account-hash",
        data={"username": "tester@example.invalid", "api_key": "old-test-key"},
    )
    entry.add_to_hass(hass)
    client = AsyncMock()
    client.get_trackers.side_effect = SafetagAuthError("private error body")
    with (
        patch("custom_components.safetag.config_flow.SafetagClient", return_value=client),
        patch.object(hass.config_entries, "async_reload", return_value=True) as reload,
    ):
        result = await hass.config_entries.flow.async_init(
            "safetag",
            context={"source": source, "entry_id": entry.entry_id},
            data=entry.data if source == "reauth" else None,
        )
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {"api_key": "bad-test-key"}
        )
        assert result["type"] == "form"
        assert result["step_id"] == step
        assert result["errors"] == {"base": "invalid_auth"}
        assert entry.data["api_key"] == "old-test-key"
        reload.assert_not_awaited()


async def test_reconnect_backoff_is_exponential_and_capped(hass):
    import asyncio

    from homeassistant.config_entries import ConfigEntryState
    from pytest_homeassistant_custom_component.common import MockConfigEntry

    from custom_components.safetag.api import SafetagConnectionError
    from custom_components.safetag.coordinator import SafetagCoordinator

    entry = MockConfigEntry(domain="safetag", unique_id="synthetic-account-hash")
    entry.mock_state(hass, ConfigEntryState.SETUP_IN_PROGRESS)
    client = AsyncMock()
    client.get_trackers.return_value = []

    async def stream():
        raise SafetagConnectionError("private body")
        yield []

    client.stream = stream
    coordinator = SafetagCoordinator(hass, entry, client)
    delays = []

    async def pause(delay):
        delays.append(delay)
        if len(delays) == 10:
            raise asyncio.CancelledError

    with (
        patch("custom_components.safetag.coordinator.monotonic", return_value=0),
        patch("custom_components.safetag.coordinator.random.uniform", return_value=1),
        patch.object(coordinator, "_wait_to_reconnect", side_effect=pause),
    ):
        await coordinator.async_config_entry_first_refresh()
        coordinator.start()
        with pytest.raises(asyncio.CancelledError):
            await coordinator.stream_task
    assert delays == [1, 2, 4, 8, 16, 32, 64, 120, 120, 120]
    client.get_trackers.assert_awaited_once()


def test_entity_identity_survives_entry_removal_and_readdition(hass):
    from pytest_homeassistant_custom_component.common import MockConfigEntry

    from custom_components.safetag.coordinator import SafetagCoordinator
    from custom_components.safetag.device_tracker import SafetagTracker

    first = MockConfigEntry(domain="safetag", unique_id="synthetic-account-hash")
    second = MockConfigEntry(domain="safetag", unique_id="synthetic-account-hash")
    assert first.entry_id != second.entry_id
    first_tracker = SafetagTracker(
        SafetagCoordinator(hass, first, AsyncMock()), "synthetic-tracker"
    )
    second_tracker = SafetagTracker(
        SafetagCoordinator(hass, second, AsyncMock()), "synthetic-tracker"
    )
    assert first_tracker.unique_id == second_tracker.unique_id
    assert first_tracker.device_info["identifiers"] == second_tracker.device_info["identifiers"]
