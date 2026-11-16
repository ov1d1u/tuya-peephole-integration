"""H.265 RTP depacketizing (RFC 7798) into Annex-B access units.

With ``datachannel_enable`` the camera sends its video as standard H.265
RTP packets inside WebRTC datachannel messages (see live_stream.py). This
module turns those packets into whole pictures (access units) that PyAV
can decode or mux. It has no Home Assistant or aiortc dependencies.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass

START_CODE = b"\x00\x00\x00\x01"

# NAL unit types (ITU-T H.265 table 7-1)
NAL_IRAP_FIRST, NAL_IRAP_LAST = 16, 21  # BLA/IDR/CRA: decodable on its own
NAL_VPS, NAL_SPS, NAL_PPS = 32, 33, 34
NAL_AP, NAL_FU, NAL_PACI = 48, 49, 50  # RTP payload structures

# Smallest pts step between pictures (1/30 s). The camera sends a burst of
# buffered pictures at stream start; spacing them out keeps players happy,
# and the timestamps catch up with real time within a fraction of a second.
MIN_FRAME_INTERVAL = 3000

# Wider than any real gap between consecutive packets; sequence numbers
# further "behind" than this are treated as old duplicates.
_SEQ_HALF = 0x8000


@dataclass(frozen=True, slots=True)
class RtpPacket:
    """The RTP header fields the depacketizer needs, plus the payload."""

    payload_type: int
    marker: bool
    sequence_number: int
    timestamp: int
    ssrc: int
    payload: bytes


@dataclass(frozen=True, slots=True)
class AccessUnit:
    """One coded picture in Annex-B format.

    pts is in 90 kHz units from the arrival time of the picture's last
    packet, at least MIN_FRAME_INTERVAL apart. The camera's RTP timestamps
    are not used: its clock runs several times too slow, and the pictures
    in the burst at stream start share one timestamp. Keyframes carry the
    VPS/SPS/PPS, so each one can start a decoder.
    """

    data: bytes
    pts: int
    keyframe: bool


def parse_rtp(data: bytes) -> RtpPacket | None:
    """Parse an RTP packet; return None if it is malformed."""
    if len(data) < 12 or data[0] >> 6 != 2:
        return None
    csrc_count = data[0] & 0x0F
    offset = 12 + 4 * csrc_count
    if data[0] & 0x10:  # header extension
        if len(data) < offset + 4:
            return None
        (ext_words,) = struct.unpack_from(">H", data, offset + 2)
        offset += 4 + 4 * ext_words
    end = len(data)
    if data[0] & 0x20 and end > offset:  # padding
        end -= data[-1]
    if end < offset:
        return None
    seq, timestamp, ssrc = struct.unpack_from(">HII", data, 2)
    return RtpPacket(
        payload_type=data[1] & 0x7F,
        marker=bool(data[1] & 0x80),
        sequence_number=seq,
        timestamp=timestamp,
        ssrc=ssrc,
        payload=bytes(data[offset:end]),
    )


def nal_type(nal: bytes) -> int:
    """Return the H.265 NAL unit type from its 2-byte header."""
    return (nal[0] >> 1) & 0x3F


class HevcDepacketizer:
    """Rebuild H.265 access units from RTP packets (RFC 7798).

    Handles single NAL unit packets, aggregation packets (AP) and
    fragmentation units (FU); DONL fields are not used by the camera.
    A missing packet drops the picture it belonged to, and output then
    pauses until the next keyframe, so a decoder never sees a picture
    whose references are broken.
    """

    def __init__(self) -> None:
        """Initialize empty state (waiting for the first keyframe)."""
        self._nals: list[bytes] = []
        self._fu: bytearray | None = None
        self._timestamp: int | None = None
        self._broken = False
        self._synced = False
        self._last_seq: int | None = None
        self._param_sets: dict[int, bytes] = {}
        self._clock_start: float | None = None
        self._last_pts = -MIN_FRAME_INTERVAL

    def push(self, packet: RtpPacket, now: float) -> list[AccessUnit]:
        """Feed one RTP packet received at monotonic time ``now`` (seconds).

        Returns the access units it completed.
        """
        if self._last_seq is not None:
            delta = (packet.sequence_number - self._last_seq) & 0xFFFF
            if delta == 0 or delta >= _SEQ_HALF:
                return []  # duplicate or late packet
            if delta != 1:
                # Packet(s) lost: the picture in progress cannot be decoded
                self._broken = True
                self._fu = None
                self._synced = False
        self._last_seq = packet.sequence_number

        units: list[AccessUnit] = []
        if self._timestamp is not None and packet.timestamp != self._timestamp:
            units.extend(self._flush(now))
        self._timestamp = packet.timestamp
        self._add_payload(packet.payload)
        if packet.marker:
            units.extend(self._flush(now))
        return units

    def _add_payload(self, payload: bytes) -> None:
        if len(payload) < 3:
            return
        kind = nal_type(payload)
        if kind == NAL_AP:
            pos = 2
            while pos + 2 <= len(payload):
                (size,) = struct.unpack_from(">H", payload, pos)
                pos += 2
                if size == 0 or pos + size > len(payload):
                    self._broken = True
                    return
                self._nals.append(payload[pos : pos + size])
                pos += size
        elif kind == NAL_FU:
            fu_header = payload[2]
            if fu_header & 0x80:  # start
                header = bytes(
                    [(payload[0] & 0x81) | ((fu_header & 0x3F) << 1), payload[1]]
                )
                self._fu = bytearray(header)
            elif self._fu is None:
                self._broken = True  # middle/end without a start
                return
            self._fu += payload[3:]
            if fu_header & 0x40:  # end
                self._nals.append(bytes(self._fu))
                self._fu = None
        elif kind == NAL_PACI:
            return
        else:
            self._nals.append(payload)

    def _next_pts(self, now: float) -> int:
        if self._clock_start is None:
            self._clock_start = now
        pts = max(
            round((now - self._clock_start) * 90000),
            self._last_pts + MIN_FRAME_INTERVAL,
        )
        self._last_pts = pts
        return pts

    def _flush(self, now: float) -> list[AccessUnit]:
        nals, self._nals = self._nals, []
        broken, self._broken = self._broken or self._fu is not None, False
        self._fu = None
        timestamp, self._timestamp = self._timestamp, None
        if not nals or broken or timestamp is None:
            return []

        types = [nal_type(nal) for nal in nals]
        for kind, nal in zip(types, nals, strict=True):
            if kind in (NAL_VPS, NAL_SPS, NAL_PPS):
                self._param_sets[kind] = nal
        keyframe = any(NAL_IRAP_FIRST <= kind <= NAL_IRAP_LAST for kind in types)
        if keyframe:
            missing = [
                self._param_sets[kind]
                for kind in (NAL_VPS, NAL_SPS, NAL_PPS)
                if kind not in types and kind in self._param_sets
            ]
            if len(missing) + sum(
                kind in (NAL_VPS, NAL_SPS, NAL_PPS) for kind in set(types)
            ) < 3:
                return []  # cannot decode without all parameter sets yet
            nals = missing + nals
            self._synced = True
        elif not self._synced:
            return []

        pts = self._next_pts(now)
        data = b"".join(START_CODE + nal for nal in nals)
        return [AccessUnit(data=data, pts=pts, keyframe=keyframe)]
