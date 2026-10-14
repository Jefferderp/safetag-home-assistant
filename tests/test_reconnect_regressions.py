"""Regression coverage for nullable Home Assistant reconnect hints."""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.config_entries import ConfigEntryState
from homeassistant.helpers.update_coordinator import UpdateFailed
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.safetag.api import SafetagConnectionError
from custom_components.safetag.coordinator import SafetagCoordinator


@pytest.mark.parametrize("retry_hint", [None, 300])
async def test_snapshot_failure_retries_and_recovers(hass, retry_hint):
    entry = MockConfigEntry(domain="safetag", unique_id="synthetic-account-hash")
    entry.mock_state(hass, ConfigEntryState.SETUP_IN_PROGRESS)
    client = AsyncMock()
    client.get_trackers.return_value = []
    attempts = 0
    recovered = asyncio.Event()

    async def stream():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise SafetagConnectionError()
        yield [{"_id": "synthetic-tracker", "status": {"battery": 4}}]
        recovered.set()
        await asyncio.Event().wait()

    client.stream = stream
    coordinator = SafetagCoordinator(hass, entry, client)
    failure = UpdateFailed("Unable to retrieve Safetag trackers")
    # Modern HA defines this optional attribute with None as its default.
    failure.retry_after = retry_hint
    delays = []

    async def pause(delay):
        delays.append(delay)

    with (
        patch("custom_components.safetag.coordinator.monotonic", return_value=61),
        patch("custom_components.safetag.coordinator.random.uniform", return_value=1),
        patch.object(coordinator, "_wait_to_reconnect", side_effect=pause),
    ):
        await coordinator.async_config_entry_first_refresh()
        coordinator._last_snapshot = 0
        with patch.object(
            coordinator, "_async_update_data", side_effect=[failure, {}]
        ) as reconcile:
            coordinator.start()
            try:
                # Await either recovery or task death to expose the exact crash promptly.
                recovery = asyncio.create_task(recovered.wait())
                await asyncio.wait(
                    [coordinator.stream_task, recovery],
                    timeout=1,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if coordinator.stream_task.done():
                    coordinator.stream_task.result()
                assert recovered.is_set()
                assert delays == [1, max(2, retry_hint or 0)]
                assert reconcile.await_count == 2
                assert coordinator.connected
                assert coordinator.last_update_success
                assert coordinator.data["synthetic-tracker"]["battery_level"] == 4
            finally:
                recovery.cancel()
                await asyncio.gather(recovery, return_exceptions=True)
                await coordinator.stop()


async def test_stop_handles_already_failed_stream_without_private_error(hass, caplog):
    entry = MockConfigEntry(domain="safetag", unique_id="synthetic-account-hash")
    coordinator = SafetagCoordinator(hass, entry, AsyncMock())

    async def failed_stream():
        raise RuntimeError("private-upstream-body")

    coordinator.stream_task = asyncio.create_task(failed_stream())
    await asyncio.sleep(0)
    assert coordinator.stream_task.done()
    await coordinator.stop()
    await coordinator.stop()
    assert "private-upstream-body" not in caplog.text
    assert "Safetag stream task failed before shutdown" in caplog.text


def test_tracker_uses_public_home_assistant_import():
    import ast
    import inspect

    from custom_components.safetag import device_tracker

    imports = ast.parse(inspect.getsource(device_tracker))
    assert any(
        isinstance(node, ast.ImportFrom)
        and node.module == "homeassistant.components.device_tracker"
        and any(alias.name == "TrackerEntity" for alias in node.names)
        for node in ast.walk(imports)
    )
    assert "device_tracker.config_entry" not in inspect.getsource(device_tracker)
