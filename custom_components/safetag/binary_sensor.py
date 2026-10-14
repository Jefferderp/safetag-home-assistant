"""Charging and engine-cut relay telemetry, never controls."""

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
    BinarySensorEntityDescription,
)

from .entity import SafetagEntity, discover

PARALLEL_UPDATES = 0
SENSORS = (
    BinarySensorEntityDescription(
        key="charging",
        name="Charging",
        translation_key="charging",
        device_class=BinarySensorDeviceClass.BATTERY_CHARGING,
    ),
    BinarySensorEntityDescription(key="relay", name="Engine cut relay", translation_key="relay"),
)


async def async_setup_entry(hass, entry, async_add_entities):
    discover(
        entry.runtime_data,
        entry,
        async_add_entities,
        lambda key: [SafetagBinarySensor(entry.runtime_data, key, desc) for desc in SENSORS],
    )


class SafetagBinarySensor(SafetagEntity, BinarySensorEntity):
    """Report a boolean without inferring missing telemetry."""

    def __init__(self, coordinator, device_id, description):
        super().__init__(coordinator, device_id, description.key)
        self.entity_description = description

    @property
    def is_on(self):
        return self.tracker.get(self.entity_description.key)
