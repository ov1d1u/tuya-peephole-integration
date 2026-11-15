"""Camera entity for the Tuya Peephole Camera integration.

Live view: the camera's video is received inside Home Assistant (over a
WebRTC datachannel, see live_stream.py) and offered as a local MPEG-TS
stream through stream_source(). Home Assistant's go2rtc (or its HLS
stream component) plays that in the browser. The camera is connected only
while someone watches.

The camera cannot be played by browsers directly: on its regular WebRTC
media tracks it sends H.265 labelled as H.264.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from homeassistant.components.camera import Camera, CameraEntityFeature
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import CONF_LIVE_CODEC, DEFAULT_LIVE_CODEC, DOMAIN

if TYPE_CHECKING:
    from .coordinator import TuyaPeepholeCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the Tuya Peephole camera entity."""
    coordinator: TuyaPeepholeCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([TuyaPeepholeCamera(coordinator, entry)])


class TuyaPeepholeCamera(Camera):
    """Tuya Peephole camera streaming through a local restream."""

    _attr_supported_features = CameraEntityFeature.STREAM
    _attr_has_entity_name = True
    _attr_name = "Camera"

    def __init__(
        self, coordinator: TuyaPeepholeCoordinator, entry: ConfigEntry
    ) -> None:
        """Initialize the camera entity.

        Args:
            coordinator: The Tuya Peephole coordinator instance.
            entry: The config entry (its options pick the live codec).
        """
        super().__init__()
        self.coordinator = coordinator
        self._entry = entry
        self._attr_unique_id = f"{coordinator.device_id}_camera"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, coordinator.device_id)},
            name=f"Tuya Peephole {coordinator.device_id[-6:]}",
            manufacturer="Tuya",
            model="Peephole Camera",
        )

    @property
    def available(self) -> bool:
        """Return True if MQTT is connected (camera can be woken)."""
        return (
            self.coordinator.mqtt_client is not None
            and self.coordinator.mqtt_client.is_connected
        )

    async def async_camera_image(
        self, width: int | None = None, height: int | None = None
    ) -> bytes | None:
        """Return the last snapshot grabbed from the live stream.

        Home Assistant calls this every few seconds while a camera card is
        open. Opening a WebRTC stream each time would keep this battery
        camera awake, so this only returns the stored image. A fresh one is
        captured by the Snapshot button (coordinator.async_capture_snapshot).
        """
        return self.coordinator.last_snapshot

    async def async_added_to_hass(self) -> None:
        """Refresh entity state (and the image URL) when a snapshot lands."""
        await super().async_added_to_hass()
        last_seen = self.coordinator.last_snapshot_time

        @callback
        def _on_coordinator_update() -> None:
            nonlocal last_seen
            if self.coordinator.last_snapshot_time != last_seen:
                last_seen = self.coordinator.last_snapshot_time
                self.async_write_ha_state()

        self.async_on_remove(
            self.coordinator.async_add_listener(_on_coordinator_update)
        )

    async def stream_source(self) -> str | None:
        """Return the local MPEG-TS URL of the live stream.

        Reading it wakes and connects the camera; it disconnects shortly
        after the last reader stops.
        """
        restream = self.coordinator.restream
        if restream is None:
            return None
        codec = self._entry.options.get(CONF_LIVE_CODEC, DEFAULT_LIVE_CODEC)
        return restream.url(codec)
