"""Binary sensors for the Tuya Peephole Camera integration.

Provides a BinarySensorDeviceClass.MOTION entity that reads
motion_detected state from the coordinator's push data, and an
awake sensor that shows whether the camera is awake or sleeping.
"""

from __future__ import annotations

from typing import Any

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .coordinator import TuyaPeepholeCoordinator
from .entity import TuyaPeepholeEntity
from .models import CameraState


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Tuya Peephole binary sensors from a config entry.

    Args:
        hass: Home Assistant instance.
        entry: Config entry being set up.
        async_add_entities: Callback to register new entities.
    """
    coordinator: TuyaPeepholeCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        [TuyaMotionSensor(coordinator), TuyaAwakeSensor(coordinator)]
    )


class TuyaMotionSensor(TuyaPeepholeEntity, BinarySensorEntity):
    """Motion detection binary sensor.

    Reports ON when the coordinator receives a PIR/motion event
    via MQTT, and automatically clears after MOTION_CLEAR_TIMEOUT.
    """

    _attr_device_class = BinarySensorDeviceClass.MOTION

    def __init__(self, coordinator: TuyaPeepholeCoordinator) -> None:
        """Initialize the motion sensor.

        Args:
            coordinator: The Tuya Peephole coordinator instance.
        """
        super().__init__(coordinator, "motion_detected", "Motion Detected")

    @property
    def is_on(self) -> bool | None:
        """Return True if motion is currently detected.

        Returns None if coordinator has no data yet (entity will show
        unknown state in HA until first MQTT update).
        """
        if self.coordinator.data is None:
            return None
        return self.coordinator.data.get("motion_detected", False)


class TuyaAwakeSensor(TuyaPeepholeEntity, BinarySensorEntity):
    """Camera awake/sleeping binary sensor.

    Reports ON while the camera is awake and OFF while it is sleeping
    or still waking up. The detailed lifecycle state (sleeping, waking,
    awake) is exposed as the ``camera_state`` attribute.
    """

    def __init__(self, coordinator: TuyaPeepholeCoordinator) -> None:
        """Initialize the awake sensor.

        Args:
            coordinator: The Tuya Peephole coordinator instance.
        """
        super().__init__(coordinator, "camera_awake", "Camera Awake")

    def _camera_state(self) -> str | None:
        """Return the camera lifecycle state as a plain string, if known."""
        if self.coordinator.data is None:
            return None
        state = self.coordinator.data.get("camera_state")
        if isinstance(state, CameraState):
            return state.value
        return state

    @property
    def is_on(self) -> bool | None:
        """Return True if the camera is awake, None if state is unknown."""
        state = self._camera_state()
        if state is None:
            return None
        return state == CameraState.AWAKE.value

    @property
    def icon(self) -> str:
        """Return an icon reflecting the awake/sleeping state."""
        return "mdi:cctv" if self.is_on else "mdi:sleep"

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Expose the detailed lifecycle state (sleeping/waking/awake)."""
        return {"camera_state": self._camera_state()}
