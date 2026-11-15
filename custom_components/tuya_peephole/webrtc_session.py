"""Server-side aiortc WebRTC session to the camera.

Creates an independent RTCPeerConnection inside Home Assistant (separate
from the SDP proxy camera entity, which relays to the browser), negotiates
SDP offer/answer via MQTT protocol 302, and hands the received video track
to a subclass. Used by recording (MP4 muxing) and snapshots (one frame).
"""

from __future__ import annotations

import asyncio
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

# The camera streams H.265/HEVC but announces it as H.264 (payload type 99)
# and packs it with H.264's RTP packetization: HEVC NAL units go out whole
# when small and as H.264 FU-A fragments when large. aiortc's H.264
# depayloader rebuilds a valid HEVC Annex-B stream from that, except that it
# rejects NAL units whose low five header bits are 0, 30 or 31 (unspecified
# in H.264) -- which includes the HEVC VPS (header 40 01). Its H.264 decoder
# then cannot decode HEVC. The two patches below cover both gaps.
_UNSPECIFIED_NAL_TYPES = frozenset({0, 30, 31})
_ANNEXB_START_CODES = (b"\x00\x00\x00\x01", b"\x00\x00\x01")
_START_CODE = b"\x00\x00\x00\x01"

# HEVC VPS NAL header (type 32, layer 0, temporal id 1). Never valid H.264:
# 0x40 is NAL type 0 there.
_HEVC_VPS_MARKER = _START_CODE + b"\x40\x01"
_HEVC_TYPE_VPS, _HEVC_TYPE_SPS, _HEVC_TYPE_PPS = 32, 33, 34
_HEVC_TYPE_AUD, _HEVC_TYPE_PREFIX_SEI = 35, 39
_HEVC_AU_START_TYPES = frozenset(
    {
        _HEVC_TYPE_VPS,
        _HEVC_TYPE_SPS,
        _HEVC_TYPE_PPS,
        _HEVC_TYPE_AUD,
        _HEVC_TYPE_PREFIX_SEI,
    }
)

# Diagnostics: how many raw RTP payloads to log (debug level) for the
# current session. Reset at each session start.
_PAYLOAD_LOG_LIMIT = 12
_payload_log_remaining = 0


def patch_aiortc_h264_depayloader(h264_module: Any = None) -> bool:
    """Make aiortc's H.264 depayloader keep the camera's HEVC NAL units.

    aiortc raises "NAL unit type 0 is not supported" for the HEVC VPS and
    drops the packet. That loses the VPS (so nothing can be decoded) and
    leaves a hole in the jitter buffer, which only emits frames from a
    contiguous run of packets, so no frame comes out at all.

    The patched parser strips an Annex-B start code if a payload carries one
    and passes unspecified NAL types through as single NAL units. Everything
    else goes to aiortc unchanged. Returns True once the patch is in place.
    """
    if h264_module is None:
        try:
            from aiortc.codecs import h264 as h264_module
        except ImportError:
            return False

    descriptor_cls = h264_module.H264PayloadDescriptor
    if getattr(descriptor_cls.parse, "_tuya_patched", False):
        return True
    original_parse = descriptor_cls.parse.__func__

    def parse(cls, data: bytes):  # type: ignore[no-untyped-def]
        global _payload_log_remaining
        if _payload_log_remaining > 0 and _LOGGER.isEnabledFor(logging.DEBUG):
            _payload_log_remaining -= 1
            shown = data if len(data) <= 64 else data[:24]
            _LOGGER.debug(
                "RTP video payload len=%d: %s%s",
                len(data),
                shown.hex(" "),
                "" if shown is data else " ...",
            )
        for start_code in _ANNEXB_START_CODES:
            if data.startswith(start_code) and len(data) > len(start_code):
                data = data[len(start_code):]
                break
        if len(data) >= 2 and (data[0] & 0x1F) in _UNSPECIFIED_NAL_TYPES:
            return cls(first_fragment=True), _START_CODE + data
        return original_parse(cls, data)

    parse._tuya_patched = True  # type: ignore[attr-defined]
    descriptor_cls.parse = classmethod(parse)
    return True


def split_annexb(data: bytes) -> list[bytes]:
    """Split an Annex-B byte stream into NAL units (without start codes)."""
    nals: list[bytes] = []
    i, n = 0, len(data)
    start = -1
    while i + 3 <= n:
        if data[i] == 0 and data[i + 1] == 0 and data[i + 2] == 1:
            if start >= 0:
                end = i - 1 if i > 0 and data[i - 1] == 0 else i
                if end > start:
                    nals.append(data[start:end])
            i += 3
            start = i
        else:
            i += 1
    if 0 <= start < n:
        nals.append(data[start:])
    return nals


def split_hevc_access_units(data: bytes) -> list[bytes]:
    """Split HEVC Annex-B data into access units (one picture each).

    The camera's first burst carries a keyframe plus several P-frames under
    one RTP timestamp, so aiortc hands them over as a single "frame". A new
    access unit starts at a parameter set, AUD or prefix SEI, or at a slice
    with first_slice_segment_in_pic_flag set, once the current one already
    holds a picture.
    """
    units: list[bytes] = []
    current: list[bytes] = []
    has_picture = False
    for nal in split_annexb(data):
        if len(nal) < 2:
            continue
        nal_type = (nal[0] >> 1) & 0x3F
        is_slice = nal_type < 32
        first_slice = is_slice and len(nal) > 2 and bool(nal[2] & 0x80)
        if has_picture and (
            nal_type in _HEVC_AU_START_TYPES or first_slice
        ):
            units.append(b"".join(_START_CODE + x for x in current))
            current, has_picture = [], False
        current.append(nal)
        has_picture = has_picture or is_slice
    if current:
        units.append(b"".join(_START_CODE + x for x in current))
    return units


def patch_aiortc_h264_decoder(h264_module: Any = None) -> bool:
    """Let aiortc's H.264 decoder decode the camera's HEVC stream.

    The first time a frame contains an HEVC VPS, the decoder's codec context
    is replaced with an HEVC one, and from then on each frame is split into
    access units that are decoded one by one. Real H.264 never contains the
    VPS marker, so H.264 streams are decoded exactly as before. Returns True
    once the patch is in place.
    """
    if h264_module is None:
        try:
            from aiortc.codecs import h264 as h264_module
        except ImportError:
            return False

    decoder_cls = h264_module.H264Decoder
    if getattr(decoder_cls.decode, "_tuya_patched", False):
        return True
    original_decode = decoder_cls.decode
    av_module = h264_module.av

    def decode(self, encoded_frame):  # type: ignore[no-untyped-def]
        if not getattr(self, "_tuya_hevc", False):
            if _HEVC_VPS_MARKER not in encoded_frame.data:
                return original_decode(self, encoded_frame)
            _LOGGER.debug("Camera stream is HEVC; switching decoder to hevc")
            self.codec = av_module.CodecContext.create("hevc", "r")
            self._tuya_hevc = True

        frames: list[Any] = []
        error: Exception | None = None
        for unit in split_hevc_access_units(encoded_frame.data):
            packet = av_module.Packet(unit)
            packet.pts = encoded_frame.timestamp
            packet.time_base = h264_module.VIDEO_TIME_BASE
            try:
                frames.extend(self.codec.decode(packet))
            except av_module.FFmpegError as exc:
                error = exc
        if not frames and error is not None:
            raise error
        return frames

    decode._tuya_patched = True  # type: ignore[attr-defined]
    decoder_cls.decode = decode
    return True


if AIORTC_AVAILABLE:
    patch_aiortc_h264_depayloader()
    patch_aiortc_h264_decoder()

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

        global _payload_log_remaining
        _payload_log_remaining = _PAYLOAD_LOG_LIMIT

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

            # Step 3: Build ICE server configuration
            config = RTCConfiguration(
                iceServers=build_aiortc_ice_servers(p2p_ices)
            )

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
                uid = self._uid or self._coordinator.msid
                moto_id = self._moto_id
                if moto_id is None:
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
