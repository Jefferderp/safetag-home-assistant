"""Shared privacy-safe identity and dynamic discovery."""

from hashlib import sha256

from homeassistant.core import callback
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN


class SafetagEntity(CoordinatorEntity):
    """Only hashed identifiers and neutral tracker names reach registries."""

    _attr_has_entity_name = True

    def __init__(self, coordinator, device_id, key):
        super().__init__(coordinator)
        self.device_id = device_id
        digest = sha256(f"{coordinator.entry.unique_id}:{device_id}".encode()).hexdigest()
        self._attr_unique_id = f"{digest}_{key}"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, digest)},
            manufacturer="Safetag",
            name=f"Safetag tracker {digest[:8]}",
        )

    @property
    def tracker(self):
        return self.coordinator.data.get(self.device_id, {})

    @property
    def available(self):
        return (
            super().available
            and self.coordinator.connected
            and self.device_id in self.coordinator.data
        )


def discover(coordinator, entry, async_add_entities, factory):
    """Add new trackers from push data without changing existing identities."""
    known = set()

    @callback
    def add_new():
        new = set(coordinator.data) - known
        if new:
            known.update(new)
            async_add_entities(
                [entity for device_id in sorted(new) for entity in factory(device_id)]
            )

    add_new()
    entry.async_on_unload(coordinator.async_add_listener(add_new))
