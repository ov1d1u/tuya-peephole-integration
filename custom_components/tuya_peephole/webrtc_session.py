"""Server-side aiortc WebRTC session to the camera.

Creates an independent RTCPeerConnection inside Home Assistant (separate
from the SDP proxy camera entity, which relays to the browser), negotiates
SDP offer/answer via MQTT protocol 302, and hands the received video track
to a subclass. Used by recording (MP4 muxing) and snapshots (one frame).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from .const import (
    WEBRTC_PUBLISH_TOPIC_TEMPLATE,
    WEBRTC_SESSION_TIMEOUT,
    WEBRTC_STREAM_TYPE_HD,
    WEBRTC_SUBSCRIBE_TOPIC_TEMPLATE,
)
from .webrtc_signaling import (
    build_disconnect_payload,
    build_offer_payload,
    build_protocol_302_message,
    generate_session_id,
    parse_protocol_302_message,
    strip_sdp_extmap,
)

if TYPE_CHECKING:
    from .coordinator import TuyaPeepholeCoordinator

# aiortc is an optional dependency -- may not be installed in all environments
try:
    from aiortc import (
        RTCConfiguration,
        RTCIceServer,
        RTCPeerConnection,
        RTCSessionDescription,
    )

    AIORTC_AVAILABLE = True
except ImportError:
    AIORTC_AVAILABLE = False

_LOGGER = logging.getLogger(__name__)


class ServerWebRTCSession:
    """Base class for a server-side WebRTC session receiving camera video.

    Subclasses override the media hooks:
    - _setup_media(): called once the PeerConnection exists
    - _async_on_video_track(track): called when the camera's video arrives
    - _async_close_media(): called first during async_stop()
    """

    _label = "WebRTC"

    def __init__(self, coordinator: TuyaPeepholeCoordinator) -> None:
        """Initialize the session.

        Args:
            coordinator: The Tuya Peephole coordinator instance.
        """
        self._coordinator = coordinator
        self._pc: Any = None  # RTCPeerConnection (typed as Any for optional dep)
        self._session_id: str | None = None
        self._answer_event = asyncio.Event()
        self._answer_sdp: str | None = None
        self._started = False
        self._stopped = False

        # MQTT signaling state
        self._subscribe_topic: str | None = None
        self._publish_topic: str | None = None
        self._signaling_cb: Callable[..., None] | None = None
        self._uid: str | None = None
        self._moto_id: str | None = None

    # --- Media hooks (override in subclasses) ---

    def _setup_media(self) -> None:
        """Prepare media sinks after the PeerConnection is created."""

    async def _async_on_video_track(self, track: Any) -> None:
        """Handle the camera's video track."""

    async def _async_close_media(self) -> None:
        """Release media sinks during stop."""

    # --- Session lifecycle ---

    async def async_start(self) -> bool:
        """Create PeerConnection, negotiate SDP with the camera, start media.

        Returns True if the session started successfully, False on any error.
        """
        if not AIORTC_AVAILABLE:
            _LOGGER.error(
                "aiortc is not installed -- cannot create %s session",
                self._label,
            )
            return False

        try:
            # Step 1: Fetch WebRTC config from Tuya API
            webrtc_config = (
                await self._coordinator.api.async_get_webrtc_config(
                    self._coordinator.device_id
                )
            )
            moto_id = webrtc_config["motoId"]
            auth = webrtc_config["auth"]
            p2p_ices = webrtc_config.get("p2pConfig", {}).get("ices", [])

            # Step 2: Fetch MQTT config for uid (msid)
            mqtt_config = await self._coordinator.api.async_get_mqtt_config(
                self._coordinator.device_id
            )
            uid = mqtt_config["msid"]
            self._uid = uid
            self._moto_id = moto_id

            # Step 3: Build ICE server configuration
            ice_servers = [
                RTCIceServer(
                    urls=ice["urls"],
                    username=ice.get("username"),
                    credential=ice.get("credential"),
                )
                for ice in p2p_ices
            ]
            config = RTCConfiguration(iceServers=ice_servers)

            # Step 4: Create PeerConnection and media sinks
            self._pc = RTCPeerConnection(configuration=config)
            self._setup_media()

            # Step 5: Add recvonly video transceiver
            self._pc.addTransceiver("video", direction="recvonly")

            # Step 6: Register track handler
            @self._pc.on("track")
            async def on_track(track: Any) -> None:
                if track.kind == "video":
                    _LOGGER.debug(
                        "%s session received video track", self._label
                    )
                    await self._async_on_video_track(track)

            # Step 7: Register connection state change handler
            @self._pc.on("connectionstatechange")
            async def on_state_change() -> None:
                state = self._pc.connectionState
                _LOGGER.debug(
                    "%s PeerConnection state: %s", self._label, state
                )
                if state == "failed":
                    _LOGGER.warning(
                        "%s PeerConnection failed, stopping session",
                        self._label,
                    )
                    await self.async_stop()

            # Step 8: Create SDP offer
            offer = await self._pc.createOffer()
            await self._pc.setLocalDescription(offer)

            # Step 9: Generate signaling session ID
            self._session_id = generate_session_id()
            _LOGGER.debug(
                "%s signaling session: %s", self._label, self._session_id
            )

            # Step 10: Subscribe to answer topic via MQTT
            mqtt_client = self._coordinator.mqtt_client
            if mqtt_client is None or not mqtt_client.is_connected:
                _LOGGER.error(
                    "Cannot start %s session: MQTT not connected",
                    self._label,
                )
                return False

            self._subscribe_topic = (
                WEBRTC_SUBSCRIBE_TOPIC_TEMPLATE.format(msid=uid)
            )
            self._publish_topic = WEBRTC_PUBLISH_TOPIC_TEMPLATE.format(
                moto_id=moto_id,
                device_id=self._coordinator.device_id,
            )

            # Register per-topic callback for signaling messages. Keep a
            # reference so only this session's callback is removed later.
            session_id = self._session_id
            self._signaling_cb = (
                lambda client, userdata, msg: self._on_signaling_message(
                    msg.payload
                )
            )
            mqtt_client.message_callback_add(
                self._subscribe_topic, self._signaling_cb
            )
            mqtt_client.subscribe(self._subscribe_topic, qos=1)

            # Step 11: Build and publish offer
            cleaned_sdp = strip_sdp_extmap(offer.sdp)
            offer_payload = build_offer_payload(
                sdp=cleaned_sdp,
                auth=auth,
                ice_servers=p2p_ices,
                stream_type=WEBRTC_STREAM_TYPE_HD,
            )
            offer_message = build_protocol_302_message(
                msg_type="offer",
                uid=uid,
                device_id=self._coordinator.device_id,
                session_id=session_id,
                moto_id=moto_id,
                msg_payload=offer_payload,
            )
            mqtt_client.publish(
                self._publish_topic, offer_message, qos=1
            )
            _LOGGER.debug(
                "%s offer published to %s", self._label, self._publish_topic
            )

            # Step 12: Wait for SDP answer
            try:
                await asyncio.wait_for(
                    self._answer_event.wait(),
                    timeout=WEBRTC_SESSION_TIMEOUT,
                )
            except TimeoutError:
                _LOGGER.warning(
                    "%s SDP answer timed out after %ds",
                    self._label,
                    WEBRTC_SESSION_TIMEOUT,
                )
                return False

            # Step 13: Set remote description from received answer
            if self._answer_sdp is None:
                _LOGGER.error(
                    "%s answer SDP is None after event", self._label
                )
                return False

            await self._pc.setRemoteDescription(
                RTCSessionDescription(
                    sdp=self._answer_sdp, type="answer"
                )
            )

            self._started = True
            _LOGGER.debug("%s session started", self._label)
            return True

        except Exception:
            _LOGGER.exception("Failed to start %s session", self._label)
            return False
        finally:
            if not self._started:
                await self.async_stop()

    def _on_signaling_message(self, payload: bytes) -> None:
        """Handle incoming MQTT protocol 302 message (answer or candidate).

        Args:
            payload: Raw MQTT payload bytes.
        """
        if self._session_id is None:
            return

        parsed = parse_protocol_302_message(payload, self._session_id)
        if parsed is None:
            return

        msg_type = parsed["type"]
        msg_data = parsed["msg"]

        if msg_type == "answer":
            sdp = msg_data.get("sdp", "")
            _LOGGER.debug(
                "%s received SDP answer (len=%d)", self._label, len(sdp)
            )
            self._answer_sdp = sdp
            self._answer_event.set()

        elif msg_type == "candidate":
            # aiortc handles ICE gathering internally -- log and skip
            raw_candidate = msg_data.get("candidate", "")
            _LOGGER.debug(
                "%s received ICE candidate (ignored, aiortc gathers internally): %s",
                self._label,
                raw_candidate[:60] if raw_candidate else "",
            )

    async def async_stop(self) -> None:
        """Stop the session and clean up resources."""
        if self._stopped:
            return
        self._stopped = True

        try:
            await self._async_close_media()
        except Exception:
            _LOGGER.debug(
                "Error closing %s media", self._label, exc_info=True
            )

        # Send disconnect via MQTT protocol 302
        mqtt_client = self._coordinator.mqtt_client
        if (
            mqtt_client is not None
            and mqtt_client.is_connected
            and self._publish_topic
            and self._session_id
        ):
            try:
                uid = self._uid
                moto_id = self._moto_id
                if uid is None or moto_id is None:
                    mqtt_config = (
                        await self._coordinator.api.async_get_mqtt_config(
                            self._coordinator.device_id
                        )
                    )
                    uid = mqtt_config["msid"]
                    webrtc_config = (
                        await self._coordinator.api.async_get_webrtc_config(
                            self._coordinator.device_id
                        )
                    )
                    moto_id = webrtc_config["motoId"]

                message = build_protocol_302_message(
                    msg_type="disconnect",
                    uid=uid,
                    device_id=self._coordinator.device_id,
                    session_id=self._session_id,
                    moto_id=moto_id,
                    msg_payload=build_disconnect_payload(),
                )
                mqtt_client.publish(self._publish_topic, message, qos=0)
            except Exception:
                _LOGGER.debug(
                    "Error sending %s disconnect", self._label, exc_info=True
                )

        # Remove this session's MQTT callback and unsubscribe
        if (
            mqtt_client is not None
            and self._subscribe_topic is not None
        ):
            try:
                mqtt_client.message_callback_remove(
                    self._subscribe_topic, self._signaling_cb
                )
                mqtt_client.unsubscribe(self._subscribe_topic)
            except Exception:
                _LOGGER.debug(
                    "Error during %s MQTT cleanup", self._label, exc_info=True
                )

        # Close PeerConnection
        if self._pc is not None:
            try:
                await self._pc.close()
            except Exception:
                _LOGGER.debug(
                    "Error closing %s PeerConnection",
                    self._label,
                    exc_info=True,
                )

        _LOGGER.debug("%s session stopped", self._label)
