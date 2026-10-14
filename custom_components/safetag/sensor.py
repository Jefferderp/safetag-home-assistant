"""Safetag read-only telemetry sensors."""

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.const import DEGREE, PERCENTAGE, UnitOfElectricPotential, UnitOfSpeed
from homeassistant.helpers.entity import EntityCategory
from homeassistant.util import dt as dt_util

from .entity import SafetagEntity, discover

PARALLEL_UPDATES = 0
SENSORS = (
    SensorEntityDescription(
        key="speed_mph",
        name="Speed",
        translation_key="speed_mph",
        native_unit_of_measurement=UnitOfSpeed.MILES_PER_HOUR,
        suggested_unit_of_measurement=UnitOfSpeed.MILES_PER_HOUR,
        device_class=SensorDeviceClass.SPEED,
        state_class=SensorStateClass.MEASUREMENT,
    ),
    SensorEntityDescription(
        key="course",
        name="Heading",
        translation_key="course",
        native_unit_of_measurement=DEGREE,
        state_class=SensorStateClass.MEASUREMENT,
    ),
    SensorEntityDescription(
        key="battery_level", name="Battery level (0–6)", translation_key="battery_level"
    ),
    SensorEntityDescription(
        key="signal_percent",
        name="Signal",
        translation_key="signal_percent",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
    ),
    SensorEntityDescription(
        key="external_voltage",
        name="External voltage",
        translation_key="external_voltage",
        native_unit_of_measurement=UnitOfElectricPotential.VOLT,
        device_class=SensorDeviceClass.VOLTAGE,
        state_class=SensorStateClass.MEASUREMENT,
    ),
    SensorEntityDescription(
        key="last_updated",
        name="Location updated",
        translation_key="last_updated",
        device_class=SensorDeviceClass.TIMESTAMP,
    ),
    SensorEntityDescription(
        key="status_last_updated",
        name="Status updated",
        translation_key="status_last_updated",
        device_class=SensorDeviceClass.TIMESTAMP,
    ),
    SensorEntityDescription(
        key="model",
        name="Model code",
        translation_key="model",
        entity_category=EntityCategory.DIAGNOSTIC,
    ),
    SensorEntityDescription(
        key="product",
        name="Product code",
        translation_key="product",
        entity_category=EntityCategory.DIAGNOSTIC,
    ),
)


async def async_setup_entry(hass, entry, async_add_entities):
    discover(
        entry.runtime_data,
        entry,
        async_add_entities,
        lambda key: [SafetagSensor(entry.runtime_data, key, desc) for desc in SENSORS],
    )


class SafetagSensor(SafetagEntity, SensorEntity):
    """One normalized scalar, never raw billing or account metadata."""

    def __init__(self, coordinator, device_id, description):
        super().__init__(coordinator, device_id, description.key)
        self.entity_description = description

    @property
    def native_value(self):
        value = self.tracker.get(self.entity_description.key)
        if (
            self.entity_description.device_class == SensorDeviceClass.TIMESTAMP
            and value is not None
        ):
            parsed = dt_util.parse_datetime(value)
            return parsed if parsed is not None and parsed.tzinfo is not None else None
        return value
