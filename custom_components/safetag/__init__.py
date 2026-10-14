"""Safetag cloud-push tracker integration."""

from homeassistant.const import EVENT_HOMEASSISTANT_STOP, Platform
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import SafetagClient
from .const import CONF_API_KEY, CONF_USERNAME
from .coordinator import SafetagCoordinator

PLATFORMS = [Platform.DEVICE_TRACKER, Platform.SENSOR, Platform.BINARY_SENSOR]


async def async_setup_entry(hass, entry):
    """Load an account after obtaining a validated initial snapshot."""
    client = SafetagClient(
        async_get_clientsession(hass), entry.data[CONF_USERNAME], entry.data[CONF_API_KEY]
    )
    coordinator = SafetagCoordinator(hass, entry, client)
    await coordinator.async_config_entry_first_refresh()
    entry.runtime_data = coordinator
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    coordinator.start()

    async def stop_stream(event):
        await coordinator.stop()

    entry.async_on_unload(hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, stop_stream))
    return True


async def async_unload_entry(hass, entry):
    """Unload entities and close the account stream."""
    if await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        await entry.runtime_data.stop()
        return True
    return False
