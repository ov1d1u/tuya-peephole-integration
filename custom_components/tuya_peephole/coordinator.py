"""Push-based DataUpdateCoordinator for the Tuya Peephole Camera integration.

Owns the MQTT client lifecycle and provides camera state management
including wake-up, motion detection, and graceful sleep handling.
No polling interval -- all updates are push-based from MQTT messages.
"""

from __future__ import annotations

import asyncio
import logging
import struct
import time as _time
import zlib
from typing import Any

from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.helpers.event import async_call_later
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from .api import TuyaSmartAPI
from .const import (
    MOTION_CLEAR_TIMEOUT,
    MQTT_PORT,
    MQTT_RECONNECT_MAX_DELAY,
    MQTT_RECONNECT_MIN_DELAY,
    WAKE_COOLDOWN,
    WAKE_RESEND_INTERVAL,
    WAKE_TIMEOUT,
)
from .models import CameraState, TuyaMQTTMessage
from .mqtt import TuyaMQTTClient

_LOGGER = logging.getLogger(__name__)


def parse_wake_schedule(config: dict[str, Any] | None) -> list[float] | None:
    """Read the cloud's wake schedule from a jarvis/config result.

    The official app resends the wake packet at each offset of
    "wakeScheduleMs" (top level or under "p2pConfig") and gives up at the
    last one.

    Returns:
        Offsets in seconds, ascending, or None if no usable schedule.
    """
    if not isinstance(config, dict):
        return None
    schedule = config.get("wakeScheduleMs")
    if schedule is None and isinstance(config.get("p2pConfig"), dict):
        schedule = config["p2pConfig"].get("wakeScheduleMs")
    if not isinstance(schedule, list):
        return None
    offsets: list[float] = []
    for item in schedule:
        try:
            value = float(item)
        except (TypeError, ValueError):
            continue
        if value >= 0:
            offsets.append(value / 1000)
    return sorted(offsets) or None


class TuyaPeepholeCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Push-based coordinator owning the Tuya MQTT client lifecycle.

    Manages camera state transitions (SLEEPING/WAKING/AWAKE),
    processes MQTT messages for wake confirmations and motion events,
    and provides async_wake_camera for on-demand camera wake-up.

    No update_interval is set -- all data updates are pushed via
    async_set_updated_data when MQTT messages arrive.
    """

    def __init__(
        self,
        hass: HomeAssistant,
        api: TuyaSmartAPI,
        device_id: str,
        local_key: str,
    ) -> None:
        """Initialize the coordinator.

        Args:
            hass: Home Assistant instance.
            api: Tuya Smart API client for MQTT config retrieval.
            device_id: Tuya device ID for topic routing.
            local_key: Device local key for CRC32 wake payload.
        """
        super().__init__(
            hass,
            _LOGGER,
            name=f"Tuya Peephole {device_id}",
            # No update_interval -- push-only via MQTT
        )
        self.api = api
        self.device_id = device_id
        self.local_key = local_key

        self.mqtt_client: TuyaMQTTClient | None = None
        self._camera_state = CameraState.SLEEPING
        self._motion_detected = False
        self._motion_clear_unsub: CALLBACK_TYPE | None = None
        self._wake_cooldown = False
        self._awake_event = asyncio.Event()
        self._sleep_event = asyncio.Event()
        self._msid: str | None = None

        # MQTT reconnection (paho only auto-reconnects inside loop_forever,
        # which the asyncio socket-callback integration does not use)
        self._reconnect_task: asyncio.Task[None] | None = None
        self._shutting_down = False

        # Last snapshot (JPEG) grabbed from the WebRTC stream
        # Result of the capture in progress, shared with callers that arrive
        # while it runs
        self._snapshot_future: asyncio.Future[bytes | None] | None = None
        self.last_snapshot: bytes | None = None
        self.last_snapshot_time: float | None = None  # wall-clock epoch seconds

    async def async_connect_mqtt(self) -> None:
        """Connect to Tuya MQTT broker.

        Fetches MQTT credentials from Tuya API, creates client,
        connects over TLS, and subscribes to device topics.
        Called explicitly from __init__.py async_setup_entry.
        """
        _LOGGER.debug("Fetching MQTT config for device %s", self.device_id)
        mqtt_config = await self.api.async_get_mqtt_config(self.device_id)
        msid = mqtt_config["msid"]
        mqtt_password = mqtt_config["password"]
        broker = self.api.mqtt_url

        _LOGGER.debug(
            "MQTT config: broker=%s, msid=%s..., client_id=web_%s...",
            broker, msid[:10], msid[:10],
        )

        self.mqtt_client = TuyaMQTTClient(self.hass)
        self.mqtt_client.set_message_callback(self._handle_mqtt_message)
        self.mqtt_client.set_on_connected_callback(self._handle_mqtt_connected)
        self.mqtt_client.set_on_disconnected_callback(
            self._handle_mqtt_disconnect
        )

        await self.mqtt_client.async_connect(
            broker=broker,
            port=MQTT_PORT,
            client_id=f"web_{msid}",
            username=f"web_{msid}",
            password=mqtt_password,
        )

        # Store msid for WebRTC signaling topic construction
        self._msid = msid
        _LOGGER.info("MQTT connected to %s, subscribing to topics", broker)

        # Subscribe to device messages
        self.mqtt_client.subscribe(
            f"smart/decrypt/in/{self.device_id}", qos=0
        )

    async def _async_setup(self) -> None:
        """Called by DataUpdateCoordinator — MQTT is already connected."""
        pass

    def _build_state_dict(self) -> dict[str, Any]:
        """Build the state dict pushed to all listening entities.

        Centralizes the coordinator data shape so all async_set_updated_data
        calls produce a consistent dict.
        """
        return {
            "camera_state": self._camera_state,
            "motion_detected": self._motion_detected,
        }

    async def _async_update_data(self) -> dict[str, Any]:
        """Return current state snapshot.

        This is called by DataUpdateCoordinator but since we are push-only,
        it simply returns the current state without fetching.
        """
        return self._build_state_dict()

    def _handle_mqtt_connected(self) -> None:
        """Handle MQTT connection established.

        Re-subscribes to device topic on reconnection.
        """
        if self.mqtt_client is not None:
            self.mqtt_client.subscribe(
                f"smart/decrypt/in/{self.device_id}", qos=0
            )
            _LOGGER.debug(
                "MQTT connected, subscribed to smart/decrypt/in/%s",
                self.device_id,
            )

    def _handle_mqtt_message(self, message: TuyaMQTTMessage) -> None:
        """Process an incoming MQTT message.

        NOTE: This may be called from the event loop (AsyncioHelper)
        or from a paho thread. Use call_soon_threadsafe for safety.
        """
        _LOGGER.debug(
            "Processing MQTT: topic=%s awake=%s motion=%s raw=%s",
            message.topic,
            message.is_wireless_awake,
            message.is_motion,
            message.raw[:100].decode("utf-8", errors="replace") if message.raw else "",
        )

        if message.is_sleep_signal:
            if self._camera_state != CameraState.SLEEPING:
                _LOGGER.info("Camera reports going to sleep")
            self._camera_state = CameraState.SLEEPING
            self._awake_event.clear()
            self._sleep_event.set()
        elif message.is_wireless_awake:
            if self._camera_state != CameraState.AWAKE:
                _LOGGER.info("Camera wake confirmed via MQTT")
            self._camera_state = CameraState.AWAKE
            self._awake_event.set()

        if message.is_motion:
            self._motion_detected = True
            self._schedule_motion_clear()

        # Push state update to all entities
        try:
            self.async_set_updated_data(self._build_state_dict())
        except RuntimeError:
            # If called from wrong thread, schedule on event loop
            self.hass.loop.call_soon_threadsafe(
                self.async_set_updated_data, self._build_state_dict()
            )

    def _schedule_motion_clear(self) -> None:
        """Schedule automatic motion detection clear after timeout.

        Cancels any existing timer and schedules a new one.
        Motion auto-clears after MOTION_CLEAR_TIMEOUT seconds.
        """
        if self._motion_clear_unsub is not None:
            self._motion_clear_unsub()
        self._motion_clear_unsub = async_call_later(
            self.hass, MOTION_CLEAR_TIMEOUT, self._async_clear_motion
        )

    @callback
    def _async_clear_motion(self, _now: Any) -> None:
        """Clear motion detection flag after timeout.

        Called automatically by async_call_later after MOTION_CLEAR_TIMEOUT.
        """
        self._motion_detected = False
        self._motion_clear_unsub = None
        self.async_set_updated_data(self._build_state_dict())

    async def async_wake_camera(self, force: bool = False) -> bool:
        """Send CRC32 wake packet to the camera via MQTT.

        Computes CRC32 of the local_key, packs as big-endian 4 bytes,
        and publishes to m/w/{device_id} with QoS 1. Waits up to
        WAKE_TIMEOUT seconds for wireless_awake confirmation.

        Args:
            force: If True, skip cooldown and AWAKE state checks.
                   Used by WebRTC to always send wake before offer.

        Returns:
            True if camera confirmed awake, False on timeout.
        """
        if not force:
            if self._wake_cooldown:
                _LOGGER.debug("Wake cooldown active, skipping wake request")
                return self._camera_state == CameraState.AWAKE

            if self._camera_state == CameraState.AWAKE:
                return True

        if self.mqtt_client is None or not self.mqtt_client.is_connected:
            _LOGGER.warning("Cannot wake camera: MQTT not connected")
            return False

        if self._camera_state == CameraState.AWAKE:
            # Forced wake of a camera known to be awake (no sleep signal
            # since it woke): it sends nothing back, so don't wait for it.
            self._publish_wake()
            return True

        # Transition to WAKING state
        self._camera_state = CameraState.WAKING
        self._wake_cooldown = True
        self.async_set_updated_data(self._build_state_dict())

        self._awake_event.clear()
        self._sleep_event.clear()

        # Like the official app, keep sending the wake packet until the
        # camera reports awake: at the cloud's wakeScheduleMs offsets if it
        # gave any, otherwise every WAKE_RESEND_INTERVAL seconds. A camera
        # that reports going to sleep (the wake landed while it was
        # shutting down) is woken again straight away.
        schedule = parse_wake_schedule(self.api.cached_webrtc_config)
        if schedule is not None:
            _LOGGER.debug("Using cloud wake schedule %s s", schedule)
            timeout = max(schedule[-1], WAKE_TIMEOUT)
        else:
            timeout = WAKE_TIMEOUT

        loop = asyncio.get_running_loop()
        start = loop.time()
        deadline = start + timeout
        sent = 0
        result = False
        while True:
            now = loop.time()
            if schedule is None or sent < len(schedule):
                due = (
                    start + sent * WAKE_RESEND_INTERVAL
                    if schedule is None
                    else start + schedule[sent]
                )
                if now >= due:
                    self._publish_wake()
                    sent += 1
                    continue
                next_wake: float | None = due
            else:
                next_wake = None
            remaining = deadline - now
            if remaining <= 0:
                break
            wait_for = remaining if next_wake is None else min(
                remaining, next_wake - now
            )
            awake_wait = asyncio.ensure_future(self._awake_event.wait())
            sleep_wait = asyncio.ensure_future(self._sleep_event.wait())
            try:
                await asyncio.wait(
                    {awake_wait, sleep_wait},
                    timeout=wait_for,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                awake_wait.cancel()
                sleep_wait.cancel()
            if self._awake_event.is_set():
                _LOGGER.debug("Camera confirmed awake after %d wake packet(s)", sent)
                result = True
                break
            if self._sleep_event.is_set():
                _LOGGER.debug("Camera went to sleep while waking; resending wake")
                self._sleep_event.clear()
                self._publish_wake()

        if not result:
            _LOGGER.warning(
                "Camera wake timed out after %ds", WAKE_TIMEOUT
            )
            self._camera_state = CameraState.SLEEPING
            self.async_set_updated_data(self._build_state_dict())

        # Schedule cooldown reset
        async_call_later(self.hass, WAKE_COOLDOWN, self._reset_cooldown)

        return result

    def _publish_wake(self) -> None:
        """Publish the CRC32 wake packet (big-endian 4 bytes) to m/w/{id}."""
        crc = zlib.crc32(self.local_key.encode()) & 0xFFFFFFFF
        wake_topic = f"m/w/{self.device_id}"
        self.mqtt_client.publish(wake_topic, struct.pack(">I", crc), qos=1)
        _LOGGER.debug(
            "Wake packet sent: CRC32=%#010x to %s", crc, wake_topic
        )

    async def async_capture_snapshot(self) -> bytes | None:
        """Grab a fresh JPEG from the camera's live stream and keep it.

        If a capture is already running, waits for it and returns its
        result instead of opening a second stream. A failed capture returns
        None to every caller, never the previously stored image.

        Returns:
            The JPEG bytes, or None if the capture failed.
        """
        if self._snapshot_future is not None:
            # shield: a cancelled waiter must not cancel the shared result
            return await asyncio.shield(self._snapshot_future)

        future: asyncio.Future[bytes | None] = (
            asyncio.get_running_loop().create_future()
        )
        self._snapshot_future = future
        image: bytes | None = None
        try:
            from .snapshot import async_capture_snapshot

            image = await async_capture_snapshot(self)
            if image is not None:
                self.last_snapshot = image
                self.last_snapshot_time = _time.time()
                self.async_set_updated_data(self._build_state_dict())
            return image
        finally:
            # Also runs on errors and cancellation, so waiters get None
            # instead of hanging
            self._snapshot_future = None
            future.set_result(image)

    @callback
    def _reset_cooldown(self, _now: Any) -> None:
        """Reset the wake cooldown flag after WAKE_COOLDOWN seconds."""
        self._wake_cooldown = False

    def _handle_mqtt_disconnect(self) -> None:
        """Handle MQTT disconnection.

        Resets camera state to SLEEPING and invalidates API caches
        so MQTT credentials are refreshed on reconnection.
        """
        self._camera_state = CameraState.SLEEPING
        self.api.invalidate_cache()
        _LOGGER.warning(
            "MQTT disconnected, camera state reset to SLEEPING"
        )
        self.async_set_updated_data(self._build_state_dict())
        self._schedule_reconnect()

    def _schedule_reconnect(self) -> None:
        """Start the background reconnect loop unless one is already running."""
        if self._shutting_down:
            return
        if self._reconnect_task is not None and not self._reconnect_task.done():
            return
        self._reconnect_task = self.hass.async_create_background_task(
            self._async_reconnect_loop(),
            name=f"tuya_peephole_mqtt_reconnect_{self.device_id}",
        )

    async def _async_reconnect_loop(self) -> None:
        """Reconnect to the MQTT broker with fresh credentials.

        Each /api/jarvis/mqtt call mints a new msid/password pair, so a
        plain paho reconnect() is not enough: the old client is discarded
        and async_connect_mqtt() runs again. Retries with exponential
        backoff until it succeeds or the coordinator is torn down.
        """
        delay = MQTT_RECONNECT_MIN_DELAY
        attempt = 0
        while not self._shutting_down:
            await asyncio.sleep(delay)
            if self._shutting_down:
                return
            attempt += 1

            old_client = self.mqtt_client
            self.mqtt_client = None
            if old_client is not None:
                try:
                    await old_client.async_disconnect()
                except Exception:
                    _LOGGER.debug("Error discarding old MQTT client", exc_info=True)

            self.api.invalidate_cache()
            try:
                await self.async_connect_mqtt()
            except Exception as err:  # noqa: BLE001 - keep retrying
                delay = min(delay * 2, MQTT_RECONNECT_MAX_DELAY)
                _LOGGER.warning(
                    "MQTT reconnect attempt %d failed: %s; retrying in %ds",
                    attempt, err, delay,
                )
                continue

            _LOGGER.info("MQTT reconnected after %d attempt(s)", attempt)
            self.async_set_updated_data(self._build_state_dict())
            return

    async def async_teardown(self) -> None:
        """Clean up coordinator resources.

        Cancels motion clear timer and disconnects MQTT client.
        Must be called during config entry unload.
        """
        self._shutting_down = True
        if self._reconnect_task is not None:
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
            self._reconnect_task = None

        if self._motion_clear_unsub is not None:
            self._motion_clear_unsub()
            self._motion_clear_unsub = None

        if self.mqtt_client is not None:
            await self.mqtt_client.async_disconnect()
            self.mqtt_client = None

        _LOGGER.debug("Coordinator teardown complete for %s", self.device_id)

    @property
    def camera_state(self) -> CameraState:
        """Return the current camera lifecycle state."""
        return self._camera_state

    @property
    def msid(self) -> str | None:
        """Return the MQTT session ID (msid) for WebRTC signaling topic construction."""
        return self._msid
