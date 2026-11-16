"""Server-side aiortc WebRTC session to the camera.

Creates an RTCPeerConnection inside Home Assistant, negotiates it with the
camera over MQTT protocol 302, and receives the camera's media through a
WebRTC datachannel.

Why the datachannel: on its regular media tracks the camera sends H.265
labelled as H.264 and packed with H.264's RTP packetization, which no
browser and no standard depacketizer can handle. When the offer sets
``datachannel_enable`` and opens a channel named ``fmp4Stream``, the camera
instead sends standard RTP packets (RFC 7798 H.265 video, PCMU audio) as
binary datachannel messages after this handshake (the same one go2rtc's
Tuya source uses):

    client -> {"type": "codec", "msg": ""}
    camera -> {"type": "codec", "msg": "hev1.1.6.L123.b0"}
    client -> {"type": "start", "msg": "frame"}
    camera -> {"type": "recv", "msg": "{\"video\":{\"ssrc\":0},\"audio\":{\"ssrc\":1}}"}
    client -> {"type": "complete", "msg": ""}
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
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

DATACHANNEL_LABEL = "fmp4Stream"
DATACHANNEL_MAX_RETRANSMITS = 5

# close_reason the camera sends when a new session arrives too soon after
# the previous one ended; it accepts calls again after ~30-40 s.
CLOSE_REASON_BUSY = 93

# What aiortc's parser accepts (aiortc.rtcicetransport STUN/TURN_REGEX):
# no IPv6 brackets, optional port, optional ?transport=.
_AIORTC_ICE_URI = re.compile(
    r"(?P<scheme>stuns?|turns?):(?P<host>[^?:\[\]]+)(:(?P<port>[0-9]+))?"
    r"(\?transport=(?P<transport>[a-z]+))?"
)


def normalize_ice_uri(uri: str) -> str | None:
    """Adapt one Tuya ICE URI to what aiortc can use, or None to drop it.

    Tuya's p2pConfig.ices contains URIs aiortc cannot handle:
    - IPv6 hosts such as "stun:[2a05:...]:3478": aiortc raises
      "malformed uri" and the whole PeerConnection fails to start.
    - TURN URIs without "?transport=": aiortc silently ignores them,
      leaving no relay, so the camera is usually unreachable.
    TURN URIs without a transport get "?transport=udp" (the TURN default).
    """
    match = _AIORTC_ICE_URI.fullmatch(uri.strip())
    if match is None:
        return None
    if match["scheme"].startswith("turn") and match["transport"] is None:
        transport = "tcp" if match["scheme"] == "turns" else "udp"
        return f"{uri.strip()}?transport={transport}"
    return uri.strip()


def build_aiortc_ice_servers(p2p_ices: list[dict[str, Any]]) -> list[Any]:
    """Convert Tuya p2pConfig.ices to aiortc RTCIceServer objects.

    URIs aiortc cannot parse are dropped (logged at debug level), and
    servers left with no usable URI are skipped.
    """
    servers = []
    for ice in p2p_ices:
        raw = ice.get("urls") or []
        uris = [raw] if isinstance(raw, str) else list(raw)
        usable = []
        for uri in uris:
            normalized = normalize_ice_uri(uri)
            if normalized is None:
                _LOGGER.debug("Skipping ICE URI aiortc cannot use: %s", uri)
            else:
                usable.append(normalized)
        if not usable:
            continue
        servers.append(
            RTCIceServer(
                urls=usable if len(usable) > 1 else usable[0],
                username=ice.get("username"),
                credential=ice.get("credential"),
            )
        )
    return servers


class ServerWebRTCSession:
    """A WebRTC session receiving the camera's media over a datachannel.

    on_packet(data, video) is called with each raw RTP packet (bytes) and
    whether it belongs to the video stream. on_closed() is called once when
    the session ends for any reason other than async_stop().
    """

    _label = "WebRTC"

    def __init__(
        self,
        coordinator: TuyaPeepholeCoordinator,
        on_packet: Callable[[bytes, bool], None] | None = None,
        on_closed: Callable[[], None] | None = None,
    ) -> None:
        """Initialize the session.

        Args:
            coordinator: The Tuya Peephole coordinator instance.
            on_packet: Receives each RTP packet and whether it is video.
            on_closed: Called when the camera or the connection ends it.
        """
        self._coordinator = coordinator
        self._on_packet = on_packet
        self._on_closed = on_closed
        self._pc: Any = None  # RTCPeerConnection (typed as Any for optional dep)
        self._channel: Any = None  # RTCDataChannel
        self._session_id: str | None = None
        self._answer_event = asyncio.Event()
        self._answer_sdp: str | None = None
        self._started = False
        self._stopped = False
        self._video_ssrc: int | None = None
        self._audio_ssrc: int | None = None
        # close_reason from the camera's disconnect message, if any
        self.close_reason: int | None = None

        # MQTT signaling state
        self._subscribe_topic: str | None = None
        self._publish_topic: str | None = None
        self._signaling_cb: Callable[..., None] | None = None
        self._uid: str | None = None
        self._moto_id: str | None = None

    @property
    def stopped(self) -> bool:
        """Return True once the session has ended."""
        return self._stopped

    # --- Session lifecycle ---

    async def async_start(self) -> bool:
        """Create the PeerConnection and negotiate it with the camera.

        Returns True once the camera's SDP answer is applied. Media starts
        flowing to on_packet after the datachannel handshake. Returns False
        on any error (the session is then stopped).
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

            # Step 2: uid is the msid the live MQTT connection logged in
            # with. The camera answers on /av/u/{msid} and the broker only
            # delivers that topic to client web_{msid}; a freshly fetched
            # msid (each /api/jarvis/mqtt call mints a new one) never
            # receives the answer.
            uid = self._coordinator.msid
            if not uid:
                _LOGGER.error(
                    "Cannot start %s session: MQTT msid unknown", self._label
                )
                return False
            self._uid = uid
            self._moto_id = moto_id

            # Step 3: PeerConnection. Audio goes before video in the offer,
            # otherwise the camera sends a corrupt SDP answer.
            config = RTCConfiguration(
                iceServers=build_aiortc_ice_servers(p2p_ices)
            )
            self._pc = RTCPeerConnection(configuration=config)
            self._pc.addTransceiver("audio", direction="recvonly")
            self._pc.addTransceiver("video", direction="recvonly")
            self._setup_datachannel()

            @self._pc.on("connectionstatechange")
            async def on_state_change() -> None:
                state = self._pc.connectionState
                _LOGGER.debug(
                    "%s PeerConnection state: %s", self._label, state
                )
                if state in ("failed", "closed"):
                    await self._async_closed_remotely(f"connection {state}")

            # Step 4: Create SDP offer
            offer = await self._pc.createOffer()
            await self._pc.setLocalDescription(offer)

            # Step 5: Generate signaling session ID
            self._session_id = generate_session_id()
            _LOGGER.debug(
                "%s signaling session: %s", self._label, self._session_id
            )

            # Step 6: Subscribe to answer topic via MQTT
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

            # Step 7: Build and publish offer
            cleaned_sdp = strip_sdp_extmap(self._pc.localDescription.sdp)
            offer_payload = build_offer_payload(
                sdp=cleaned_sdp,
                auth=auth,
                ice_servers=p2p_ices,
                stream_type=WEBRTC_STREAM_TYPE_HD,
                datachannel_enable=True,
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

            # Step 8: Wait for SDP answer (or an early disconnect)
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

            if self._answer_sdp is None:
                _LOGGER.debug(
                    "%s: camera hung up before answering (close_reason=%s)",
                    self._label,
                    self.close_reason,
                )
                return False

            # Step 9: Set remote description from received answer
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

    def _setup_datachannel(self) -> None:
        """Open the camera's media datachannel and run its handshake."""
        channel = self._pc.createDataChannel(
            DATACHANNEL_LABEL,
            ordered=True,
            maxRetransmits=DATACHANNEL_MAX_RETRANSMITS,
        )
        self._channel = channel

        @channel.on("open")
        def on_open() -> None:
            _LOGGER.debug("%s datachannel open", self._label)
            self._send_control("codec")

        @channel.on("message")
        def on_message(message: bytes | str) -> None:
            if isinstance(message, str):
                self._on_control_message(message)
            else:
                self._on_media_message(message)

        @channel.on("close")
        def on_close() -> None:
            if not self._stopped:
                asyncio.get_running_loop().create_task(
                    self._async_closed_remotely("datachannel closed")
                )

    def _send_control(self, msg_type: str, msg: str = "") -> None:
        if self._channel is not None and self._channel.readyState == "open":
            self._channel.send(json.dumps({"type": msg_type, "msg": msg}))

    def _on_control_message(self, text: str) -> None:
        """Handle a JSON control message from the camera's datachannel."""
        try:
            message = json.loads(text)
        except ValueError:
            _LOGGER.debug("%s: bad datachannel message: %s", self._label, text)
            return
        msg_type = message.get("type")
        _LOGGER.debug("%s datachannel %s: %s", self._label, msg_type, message.get("msg"))
        if msg_type == "codec":
            self._send_control("start", "frame")
        elif msg_type == "recv":
            try:
                streams = json.loads(message.get("msg") or "{}")
                self._video_ssrc = streams["video"]["ssrc"]
                self._audio_ssrc = streams.get("audio", {}).get("ssrc")
            except (ValueError, KeyError, TypeError):
                _LOGGER.warning(
                    "%s: unexpected stream description: %s",
                    self._label,
                    message.get("msg"),
                )
                return
            self._send_control("complete")

    def _on_media_message(self, data: bytes) -> None:
        """Route one RTP packet from the datachannel by its SSRC."""
        if self._on_packet is None or self._video_ssrc is None or len(data) < 12:
            return
        ssrc = int.from_bytes(data[8:12], "big")
        if ssrc == self._video_ssrc:
            self._on_packet(data, True)
        elif ssrc == self._audio_ssrc:
            self._on_packet(data, False)

    def _on_signaling_message(self, payload: bytes) -> None:
        """Handle incoming MQTT protocol 302 message for this session.

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
            # aiortc handles ICE gathering internally (the camera's TURN
            # relay candidates arrive in the SDP answer)
            pass

        elif msg_type == "disconnect":
            self.close_reason = msg_data.get("close_reason")
            _LOGGER.debug(
                "%s: camera disconnected (close_reason=%s)",
                self._label,
                self.close_reason,
            )
            # Unblock async_start if it is still waiting for the answer
            self._answer_event.set()
            if self._started:
                asyncio.get_running_loop().create_task(
                    self._async_closed_remotely("camera disconnected")
                )

    async def _async_closed_remotely(self, reason: str) -> None:
        """End the session because the camera or the network ended it."""
        if self._stopped or not self._started:
            return
        _LOGGER.debug("%s session ended: %s", self._label, reason)
        await self.async_stop()
        if self._on_closed is not None:
            self._on_closed()

    async def async_stop(self) -> None:
        """Stop the session and clean up resources."""
        if self._stopped:
            return
        self._stopped = True

        # Send disconnect via MQTT protocol 302
        mqtt_client = self._coordinator.mqtt_client
        if (
            mqtt_client is not None
            and mqtt_client.is_connected
            and self._publish_topic
            and self._session_id
            and self._moto_id
        ):
            try:
                message = build_protocol_302_message(
                    msg_type="disconnect",
                    uid=self._uid or self._coordinator.msid,
                    device_id=self._coordinator.device_id,
                    session_id=self._session_id,
                    moto_id=self._moto_id,
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
