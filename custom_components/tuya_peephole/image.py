"""Image entity for the Tuya Peephole Camera integration.

Holds the last snapshot captured by the Snapshot button. The camera entity
only does live video; still images live here.
"""

from __future__ import annotations

from datetime import datetime

from homeassistant.components.image import ImageEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.util import dt as dt_util

from .const import DOMAIN
from .coordinator import TuyaPeepholeCoordinator
from .entity import TuyaPeepholeEntity


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the Tuya Peephole snapshot image entity."""
    coordinator: TuyaPeepholeCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([TuyaSnapshotImage(coordinator)])


class TuyaSnapshotImage(TuyaPeepholeEntity, ImageEntity):
    """The last snapshot grabbed from the camera's live stream.

    The state is the capture time. A new capture changes it, which makes
    Home Assistant hand the frontend a new image URL.
    """

    _attr_icon = "mdi:camera-image"
    _attr_content_type = "image/jpeg"

    def __init__(self, coordinator: TuyaPeepholeCoordinator) -> None:
        """Initialize the snapshot image entity.

        Args:
            coordinator: The Tuya Peephole coordinator instance.
        """
        TuyaPeepholeEntity.__init__(
            self, coordinator, "snapshot_image", "Snapshot"
        )
        ImageEntity.__init__(self, coordinator.hass)

    @property
    def available(self) -> bool:
        """Return True always: the stored image doesn't depend on MQTT."""
        return True

    @property
    def image_last_updated(self) -> datetime | None:
        """Return when the stored snapshot was captured."""
        captured = self.coordinator.last_snapshot_time
        if captured is None:
            return None
        return dt_util.utc_from_timestamp(captured)

    async def async_image(self) -> bytes | None:
        """Return the stored snapshot JPEG, or None if none was taken yet."""
        return self.coordinator.last_snapshot
