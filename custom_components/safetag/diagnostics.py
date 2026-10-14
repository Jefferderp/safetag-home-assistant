"""Diagnostics intentionally omit all tracker/account values and identities."""


async def async_get_config_entry_diagnostics(hass, entry):
    """Return only counts, connection health, and present normalized field keys."""
    coordinator = entry.runtime_data
    data = coordinator.data or {}
    fields = {
        key
        for tracker in data.values()
        for key, value in tracker.items()
        if value is not None and key != "device_id"
    }
    return {
        "tracker_count": len(data),
        "connected": coordinator.connected,
        "field_keys": sorted(fields),
    }
