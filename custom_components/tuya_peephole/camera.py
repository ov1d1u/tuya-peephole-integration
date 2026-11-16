"""Camera entity for the Tuya Peephole Camera integration.

Provides live WebRTC video streaming via SDP proxy pattern:
the HA backend relays SDP offers/answers and ICE candidates
between the browser and camera through MQTT (protocol 302),
while the browser establishes a direct WebRTC connection to
the camera's TURN relay servers. No media flows through HA.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from homeassistant.components.camera import Camera, CameraEntityFeature
from homeassistant.components.camera.webrtc import (
    WebRTCAnswer,
    WebRTCCandidate,
    WebRTCClientConfiguration,
    WebRTCError,
    WebRTCSendMessage,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.util import dt as dt_util
from webrtc_models import RTCConfiguration, RTCIceCandidateInit, RTCIceServer

from .const import (
    DOMAIN,
    WEBRTC_PUBLISH_TOPIC_TEMPLATE,
    WEBRTC_SESSION_TIMEOUT,
    WEBRTC_STREAM_TYPE_HD,
    WEBRTC_SUBSCRIBE_TOPIC_TEMPLATE,
)
from .webrtc_signaling import (
    build_candidate_payload,
    build_disconnect_payload,
    build_offer_payload,
    build_protocol_302_message,
    clean_candidate_from_camera,
    format_candidate_for_camera,
    generate_session_id,
    parse_protocol_302_message,
    strip_sdp_extmap,
)

if TYPE_CHECKING:
    from .coordinator import TuyaPeepholeCoordinator

_LOGGER = logging.getLogger(__name__)

ATTR_LAST_FRAME = "last_frame"


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the Tuya Peephole camera entity."""
    coordinator: TuyaPeepholeCoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([TuyaPeepholeCamera(coordinator)])


class TuyaPeepholeCamera(Camera):
    """Tuya Peephole camera with WebRTC SDP proxy via MQTT protocol 302.

    Extends Camera directly (not CoordinatorEntity) because the camera entity
    has its own lifecycle methods (async_handle_async_webrtc_offer,
    close_webrtc_session) that do not align with CoordinatorEntity's update
    pattern. Uses the coordinator for wake and MQTT access but manages its
    own WebRTC session state.
    """

    _attr_supported_features = CameraEntityFeature.STREAM
    _attr_has_entity_name = True
    _attr_name = "Camera"

    def __init__(self, coordinator: TuyaPeepholeCoordinator) -> None:
        """Initialize the camera entity.

        Args:
            coordinator: The Tuya Peephole coordinator instance.
        """
        super().__init__()
        self.coordinator = coordinator
        self._attr_unique_id = f"{coordinator.device_id}_camera"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, coordinator.device_id)},
            name=f"Tuya Peephole {coordinator.device_id[-6:]}",
            manufacturer="Tuya",
            model="Peephole Camera",
        )

        # WebRTC session state
        self._session_id: str | None = None
        self._signaling_id: str | None = None
        self._send_message: WebRTCSendMessage | None = None
        self._answer_event: asyncio.Event = asyncio.Event()
        self._signaling_task: asyncio.Task[None] | None = None
        self._signaling_cb: Callable[..., None] | None = None
        # Browser ICE candidates that arrive before the offer reaches the
        # camera (the wake wait can take up to 20s); sent once it has.
        self._pending_candidates: list[str] = []

        # Cached config (populated on first offer)
        self._webrtc_config: dict[str, Any] | None = None
        self._uid: str | None = None
        self._moto_id: str | None = None
        self._auth: str | None = None
        self._p2p_ices: list[dict[str, Any]] | None = None

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

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Expose when the stored frame (snapshot) was captured."""
        captured = self.coordinator.last_snapshot_time
        return {
            ATTR_LAST_FRAME: (
                dt_util.utc_from_timestamp(captured).isoformat()
                if captured is not None
                else None
            )
        }

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

    async def _async_ensure_config(self) -> None:
        """Fetch and cache WebRTC and MQTT config from Tuya API."""
        if self._webrtc_config is None:
            self._webrtc_config = (
                await self.coordinator.api.async_get_webrtc_config(
                    self.coordinator.device_id
                )
            )
            self._moto_id = self._webrtc_config["motoId"]
            self._auth = self._webrtc_config["auth"]
            self._p2p_ices = (
                self._webrtc_config.get("p2pConfig", {}).get("ices", [])
            )

        # The camera answers on /av/u/{msid}, and the Tuya broker only
        # delivers that topic to the client logged in as web_{msid}. Every
        # /api/jarvis/mqtt call mints a new msid, so use the one the live
        # MQTT connection was opened with, never a freshly fetched one.
        self._uid = self.coordinator.msid

    async def async_handle_async_webrtc_offer(
        self,
        offer_sdp: str,
        session_id: str,
        send_message: WebRTCSendMessage,
    ) -> None:
        """Relay SDP offer to camera via MQTT, return answer via send_message callback.

        Flow:
        1. Wake camera if sleeping (fire-and-forget, do not wait for full confirmation)
        2. Fetch WebRTC/MQTT config from API
        3. Subscribe to answer topic on MQTT
        4. Strip extmap from SDP, publish offer via protocol 302
        5. Wait for answer from camera, forward to browser via send_message
        6. Forward camera ICE candidates to browser via send_message
        """
        # The camera serves one stream at a time. The newest viewer wins: a
        # dashboard card that is already streaming, or a session the
        # frontend never closed, must not lock out the dialog the user just
        # opened.
        if self._session_id is not None:
            _LOGGER.debug(
                "Replacing WebRTC session %s with %s",
                self._session_id,
                session_id,
            )
            self._end_session()

        self._session_id = session_id
        self._send_message = send_message
        self._answer_event.clear()

        # Start the signaling flow in a background task (non-blocking)
        self._signaling_task = self.hass.async_create_task(
            self._async_webrtc_signaling(offer_sdp)
        )

    async def _async_webrtc_signaling(self, offer_sdp: str) -> None:
        """Run WebRTC signaling flow (background task)."""
        try:
            # Step 1: ALWAYS send wake before WebRTC (not just when sleeping)
            # CRC32 wake also signals camera to prepare for WebRTC session
            _LOGGER.debug("Sending wake command before WebRTC offer")
            awake = await self.coordinator.async_wake_camera(force=True)
            if not awake:
                _LOGGER.warning("Camera did not confirm wake, trying WebRTC anyway")

            # Step 2: Fetch FRESH config (invalidate cache — auth tokens expire)
            self._webrtc_config = None
            self.coordinator.api.invalidate_cache()
            await self._async_ensure_config()
            _LOGGER.debug(
                "WebRTC config: motoId=%s, auth=%s...",
                self._moto_id, (self._auth or "")[:20],
            )

            if not self._uid or not self._moto_id or not self._auth:
                self._send_error(
                    "WebRTC config incomplete (missing motoId, auth, or msid)"
                )
                return

            mqtt_client = self.coordinator.mqtt_client
            if mqtt_client is None or not mqtt_client.is_connected:
                self._send_error("MQTT not connected")
                return

            # Step 3: Generate signaling session ID (6-char, separate from HA's session_id)
            signaling_id = generate_session_id()
            self._signaling_id = signaling_id
            _LOGGER.debug(
                "WebRTC signaling session: %s (HA session: %s)",
                signaling_id,
                self._session_id,
            )

            # Step 4: Subscribe to answer/candidate topic
            subscribe_topic = WEBRTC_SUBSCRIBE_TOPIC_TEMPLATE.format(
                msid=self._uid
            )
            publish_topic = WEBRTC_PUBLISH_TOPIC_TEMPLATE.format(
                moto_id=self._moto_id,
                device_id=self.coordinator.device_id,
            )

            # Register per-topic callback for WebRTC signaling messages.
            # Keep a reference so cleanup removes only this callback (the
            # topic is shared with recording and snapshot sessions).
            self._signaling_cb = (
                lambda client, userdata, msg: self._on_signaling_message(
                    msg.payload, signaling_id
                )
            )
            mqtt_client.message_callback_add(
                subscribe_topic, self._signaling_cb
            )
            mqtt_client.subscribe(subscribe_topic, qos=1)
            _LOGGER.debug(
                "Subscribed to WebRTC signaling topic: %s", subscribe_topic
            )

            # Step 5: Strip extmap from SDP and build offer
            cleaned_sdp = strip_sdp_extmap(offer_sdp)
            offer_payload = build_offer_payload(
                sdp=cleaned_sdp,
                auth=self._auth,
                ice_servers=self._p2p_ices or [],
                stream_type=WEBRTC_STREAM_TYPE_HD,
            )
            offer_message = build_protocol_302_message(
                msg_type="offer",
                uid=self._uid,
                device_id=self.coordinator.device_id,
                session_id=signaling_id,
                moto_id=self._moto_id,
                msg_payload=offer_payload,
            )

            # Step 6: Publish offer to camera
            mqtt_client.publish(publish_topic, offer_message, qos=1)
            _LOGGER.debug(
                "Published WebRTC offer to %s (sdp_len=%d, cleaned_len=%d)",
                publish_topic,
                len(offer_sdp),
                len(cleaned_sdp),
            )

            # Browser candidates that arrived while waking the camera
            pending, self._pending_candidates = self._pending_candidates, []
            for candidate in pending:
                self._publish_candidate(candidate)

            # Step 7: Wait for SDP answer from camera
            try:
                await asyncio.wait_for(
                    self._answer_event.wait(),
                    timeout=WEBRTC_SESSION_TIMEOUT,
                )
            except TimeoutError:
                _LOGGER.warning(
                    "WebRTC SDP answer timed out after %ds",
                    WEBRTC_SESSION_TIMEOUT,
                )
                self._send_error("Camera did not respond with SDP answer")
                return

            # Answer was received and forwarded in _on_signaling_message
            # ICE candidates continue to flow via the MQTT subscription
            _LOGGER.debug("WebRTC signaling complete, ICE candidates flowing")

        except Exception:
            _LOGGER.exception("WebRTC signaling failed")
            self._send_error("WebRTC signaling error")

    def _on_signaling_message(
        self, payload: bytes, signaling_id: str
    ) -> None:
        """Handle incoming MQTT protocol 302 message (answer or candidate).

        This callback runs on the asyncio event loop via paho AsyncioHelper.

        Args:
            payload: Raw MQTT payload bytes.
            signaling_id: The 6-char signaling session ID to filter by.
        """
        parsed = parse_protocol_302_message(payload, signaling_id)
        if parsed is None:
            return  # Not our session or not protocol 302

        msg_type = parsed["type"]
        msg_data = parsed["msg"]

        if msg_type == "answer" and self._send_message is not None:
            sdp = msg_data.get("sdp", "")
            _LOGGER.debug("Received WebRTC SDP answer (len=%d)", len(sdp))
            self._send_message(WebRTCAnswer(answer=sdp))
            self._answer_event.set()

        elif msg_type == "candidate" and self._send_message is not None:
            raw_candidate = msg_data.get("candidate", "")
            if raw_candidate:
                cleaned = clean_candidate_from_camera(raw_candidate)
                _LOGGER.debug(
                    "Received ICE candidate from camera: %s", cleaned[:60]
                )
                self._send_message(
                    WebRTCCandidate(RTCIceCandidateInit(candidate=cleaned))
                )

        elif msg_type == "disconnect":
            _LOGGER.debug("Camera sent disconnect signal")
            self._reset_session()

    async def async_on_webrtc_candidate(
        self, session_id: str, candidate: RTCIceCandidateInit
    ) -> None:
        """Forward browser ICE candidate to camera via MQTT protocol 302.

        Args:
            session_id: HA WebRTC session ID.
            candidate: ICE candidate from the browser.
        """
        if self._session_id != session_id:
            _LOGGER.debug(
                "Ignoring candidate for unknown session: %s", session_id
            )
            return

        formatted = format_candidate_for_camera(candidate.candidate)
        if not self._signaling_id:
            # Offer not sent yet (camera still waking): send it afterwards
            self._pending_candidates.append(formatted)
            return
        self._publish_candidate(formatted)

    def _publish_candidate(self, formatted: str) -> None:
        """Send one browser ICE candidate (camera format) over MQTT."""
        if not self._uid or not self._moto_id or not self._signaling_id:
            _LOGGER.warning(
                "Cannot forward ICE candidate: config not loaded"
            )
            return

        mqtt_client = self.coordinator.mqtt_client
        if mqtt_client is None or not mqtt_client.is_connected:
            _LOGGER.warning(
                "Cannot forward ICE candidate: MQTT not connected"
            )
            return

        candidate_payload = build_candidate_payload(formatted)
        publish_topic = WEBRTC_PUBLISH_TOPIC_TEMPLATE.format(
            moto_id=self._moto_id,
            device_id=self.coordinator.device_id,
        )
        message = build_protocol_302_message(
            msg_type="candidate",
            uid=self._uid,
            device_id=self.coordinator.device_id,
            session_id=self._signaling_id,
            moto_id=self._moto_id,
            msg_payload=candidate_payload,
        )
        mqtt_client.publish(publish_topic, message, qos=1)
        _LOGGER.debug("Forwarded browser ICE candidate to camera")

    @callback
    def close_webrtc_session(self, session_id: str) -> None:
        """Clean up WebRTC session: send disconnect, unsubscribe MQTT, clear state.

        This is a synchronous callback (called by HA when frontend disconnects).
        paho-mqtt publish() is sync-safe (non-blocking, fire-and-forget).

        Args:
            session_id: HA WebRTC session ID to close.
        """
        if self._session_id != session_id:
            return

        _LOGGER.debug("Closing WebRTC session: %s", session_id)
        self._end_session()

    def _end_session(self) -> None:
        """Tell the camera to hang up (if it was called) and reset state."""
        mqtt_client = self.coordinator.mqtt_client
        if (
            mqtt_client is not None
            and mqtt_client.is_connected
            and self._uid
            and self._moto_id
            and self._signaling_id
        ):
            message = build_protocol_302_message(
                msg_type="disconnect",
                uid=self._uid,
                device_id=self.coordinator.device_id,
                session_id=self._signaling_id,
                moto_id=self._moto_id,
                msg_payload=build_disconnect_payload(),
            )
            publish_topic = WEBRTC_PUBLISH_TOPIC_TEMPLATE.format(
                moto_id=self._moto_id,
                device_id=self.coordinator.device_id,
            )
            # paho publish() is non-blocking and safe from a callback
            mqtt_client.publish(publish_topic, message, qos=0)
        self._reset_session()

    def _reset_session(self) -> None:
        """Drop this session's MQTT callback, stop its task, clear state.

        Always leaves the camera free for the next session, even when the
        session ended before its config or subscription existed.
        """
        mqtt_client = self.coordinator.mqtt_client
        if mqtt_client is not None and self._signaling_cb is not None and self._uid:
            subscribe_topic = WEBRTC_SUBSCRIBE_TOPIC_TEMPLATE.format(
                msid=self._uid
            )
            try:
                mqtt_client.message_callback_remove(
                    subscribe_topic, self._signaling_cb
                )
                mqtt_client.unsubscribe(subscribe_topic)
            except Exception:
                _LOGGER.debug(
                    "Error during signaling cleanup", exc_info=True
                )

        # Stop the signaling task, unless that is what is calling us
        task = self._signaling_task
        if (
            task is not None
            and not task.done()
            and task is not asyncio.current_task()
        ):
            task.cancel()

        self._session_id = None
        self._send_message = None
        self._signaling_id = None
        self._signaling_task = None
        self._signaling_cb = None
        self._pending_candidates = []
        self._answer_event.clear()

    def _send_error(self, message: str) -> None:
        """Send WebRTC error to frontend and end the session.

        Args:
            message: Error message description.
        """
        if self._send_message is not None:
            self._send_message(
                WebRTCError("webrtc_offer_failed", message)
            )
        self._reset_session()

    def async_get_webrtc_client_configuration(
        self,
    ) -> WebRTCClientConfiguration:
        """Provide Tuya's ICE servers to the browser PeerConnection.

        Returns TURN/STUN servers from p2pConfig.ices so the browser can
        establish a direct connection to the camera's relay.
        """
        ice_servers: list[RTCIceServer] = []
        if self._p2p_ices:
            for ice in self._p2p_ices:
                server = RTCIceServer(
                    urls=ice["urls"],
                    username=ice.get("username"),
                    credential=ice.get("credential"),
                )
                ice_servers.append(server)

        config = RTCConfiguration(ice_servers=ice_servers)
        return WebRTCClientConfiguration(configuration=config)

    async def async_will_remove_from_hass(self) -> None:
        """Clean up when entity is removed."""
        if self._session_id is not None:
            self.close_webrtc_session(self._session_id)
