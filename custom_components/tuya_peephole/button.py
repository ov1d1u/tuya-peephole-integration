"""Button entities for the Tuya Peephole Camera integration.

Provides wake and snapshot button entities:
- TuyaWakeButton: triggers the camera wake-up sequence
- TuyaSnapshotButton: grabs a frame from the live stream as a snapshot
"""

from __future__ import annotations

import logging

from homeassistant.components.button import ButtonEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .coordinator import TuyaPeepholeCoordinator
from .entity import TuyaPeepholeEntity

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Tuya Peephole button entities from a config entry.

    Args:
        hass: Home Assistant instance.
        entry: Config entry being set up.
        async_add_entities: Callback to register new entities.
    """
    coordinator: TuyaPeepholeCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([
        TuyaWakeButton(coordinator),
        TuyaSnapshotButton(coordinator),
    ])


class TuyaWakeButton(TuyaPeepholeEntity, ButtonEntity):
    """Wake camera button entity.

    Sends a CRC32 wake packet to the camera via the coordinator.
    Always available -- the user should be able to attempt wake
    even when MQTT is reconnecting (the coordinator handles
    cooldown and state transitions).
    """

    _attr_icon = "mdi:alarm-light-outline"

    def __init__(self, coordinator: TuyaPeepholeCoordinator) -> None:
        """Initialize the wake button.

        Args:
            coordinator: The Tuya Peephole coordinator instance.
        """
        super().__init__(coordinator, "wake_camera", "Wake Camera")

    @property
    def available(self) -> bool:
        """Return True always.

        The wake button is always available so users can attempt
        to wake the camera even during MQTT reconnection. The
        coordinator handles the case where MQTT is not connected.
        """
        return True

    async def async_press(self) -> None:
        """Handle button press -- send wake command to camera.

        Delegates to the coordinator which handles CRC32 payload
        creation, MQTT publishing, cooldown, and state transitions.
        """
        await self.coordinator.async_wake_camera()


class TuyaSnapshotButton(TuyaPeepholeEntity, ButtonEntity):
    """Snapshot button entity.

    Wakes the camera, grabs one frame from its live WebRTC stream, and
    stores it for the snapshot image entity.
    """

    _attr_icon = "mdi:camera"

    def __init__(self, coordinator: TuyaPeepholeCoordinator) -> None:
        """Initialize the snapshot button.

        Args:
            coordinator: The Tuya Peephole coordinator instance.
        """
        super().__init__(coordinator, "snapshot", "Snapshot")

    async def async_press(self) -> None:
        """Handle button press -- capture a fresh snapshot from the stream.

        Raises:
            HomeAssistantError: If no frame could be captured, so the
                failure shows in the UI instead of only in the log.
        """
        image = await self.coordinator.async_capture_snapshot()
        if image is None:
            raise HomeAssistantError(
                "Snapshot failed: no video frame from the camera "
                "(see the log for details)"
            )
        _LOGGER.info("Snapshot captured (%d bytes)", len(image))
