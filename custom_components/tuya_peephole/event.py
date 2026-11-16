"""Doorbell event entity for the Tuya Peephole Camera integration.

Fires a "ring" event each time the camera reports a doorbell press
(MQTT protocol 43), as the official app does with its ringing screen.
"""

from __future__ import annotations

from typing import Any

from homeassistant.components.event import EventDeviceClass, EventEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .coordinator import TuyaPeepholeCoordinator
from .entity import TuyaPeepholeEntity

EVENT_TYPE_RING = "ring"


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the doorbell event entity from a config entry."""
    coordinator: TuyaPeepholeCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([TuyaDoorbellEvent(coordinator)])


class TuyaDoorbellEvent(TuyaPeepholeEntity, EventEntity):
    """Doorbell press event."""

    _attr_device_class = EventDeviceClass.DOORBELL
    _attr_event_types = [EVENT_TYPE_RING]

    def __init__(self, coordinator: TuyaPeepholeCoordinator) -> None:
        """Initialize the doorbell event entity."""
        super().__init__(coordinator, "doorbell", "Doorbell")

    async def async_added_to_hass(self) -> None:
        """Subscribe to doorbell rings from the coordinator."""
        await super().async_added_to_hass()
        self.async_on_remove(
            self.coordinator.register_doorbell_callback(self._handle_ring)
        )

    @callback
    def _handle_ring(self, call: dict[str, Any]) -> None:
        """Record a doorbell ring."""
        self._trigger_event(
            EVENT_TYPE_RING,
            {"message_id": call["message_id"], "call_type": call["type"]},
        )
        self.async_write_ha_state()
