"""GPS location without upstream names or identifiers in attributes."""

from homeassistant.components.device_tracker import SourceType
from homeassistant.components.device_tracker.config_entry import TrackerEntity

from .entity import SafetagEntity, discover

PARALLEL_UPDATES = 0


async def async_setup_entry(hass, entry, async_add_entities):
    discover(
        entry.runtime_data,
        entry,
        async_add_entities,
        lambda key: [SafetagTracker(entry.runtime_data, key)],
    )


class SafetagTracker(SafetagEntity, TrackerEntity):
    """Expose the last reported GPS fix; disconnect does not erase it."""

    _attr_name = "Location"
    _attr_translation_key = "location"

    def __init__(self, coordinator, device_id):
        super().__init__(coordinator, device_id, "location")

    @property
    def source_type(self):
        return SourceType.GPS

    @property
    def latitude(self):
        return self.tracker.get("latitude")

    @property
    def longitude(self):
        return self.tracker.get("longitude")
