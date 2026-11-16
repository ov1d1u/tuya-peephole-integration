"""Tests for H.265 depacketizing (hevc.py) and the PyAV pipelines (media.py).

The camera sends standard RFC 7798 H.265 RTP over its WebRTC datachannel.
The fixture is a real HEVC clip (libx265, 160x120, 2 GOPs of 6, no
B-frames); the tests packetize it the way the camera does (single NAL
units, FU fragments of 1188 bytes) and run it through the real code and
the real PyAV decoder.
"""

from __future__ import annotations

import io
import struct
from pathlib import Path

import av
import pytest

from custom_components.tuya_peephole.hevc import (
    MIN_FRAME_INTERVAL,
    START_CODE,
    AccessUnit,
    HevcDepacketizer,
    RtpPacket,
    parse_rtp,
)
from custom_components.tuya_peephole.media import (
    ChunkSink,
    HevcDecoder,
    StreamMuxer,
    frame_to_jpeg,
)

FIXTURE = Path(__file__).parent / "fixtures" / "camera_hevc.h265"
MTU_PAYLOAD = 1188  # FU payload size the camera uses
FIXTURE_PICTURES = 12

# Real packet headers from the camera (PT 95, SSRC 0)
CAMERA_VPS = bytes.fromhex("40010c01ffff016000000300b0000003000003007bac09")


def _access_units() -> list[bytes]:
    codec = av.CodecContext.create("hevc", "r")
    data = FIXTURE.read_bytes()
    return [bytes(p) for p in codec.parse(data) + codec.parse(None)]


def _nals(unit: bytes) -> list[bytes]:
    parts = unit.split(b"\x00\x00\x01")
    return [p.rstrip(b"\x00") for p in parts if p.rstrip(b"\x00")]


def _rtp_payloads(nal: bytes, mtu: int = MTU_PAYLOAD) -> list[bytes]:
    """RFC 7798: whole NAL if it fits, else FU fragments."""
    if len(nal) <= mtu:
        return [nal]
    kind = (nal[0] >> 1) & 0x3F
    fu_indicator = bytes([(nal[0] & 0x81) | (49 << 1), nal[1]])
    body = nal[2:]
    step = mtu - 3
    pieces = [body[i : i + step] for i in range(0, len(body), step)]
    out = []
    for index, piece in enumerate(pieces):
        header = kind
        if index == 0:
            header |= 0x80
        if index == len(pieces) - 1:
            header |= 0x40
        out.append(fu_indicator + bytes([header]) + piece)
    return out


def _packets(start_seq: int = 29806, aggregate_params: bool = False) -> list[RtpPacket]:
    packets = []
    seq, ts = start_seq, 3553816562
    for unit in _access_units():
        nals = _nals(unit)
        payloads: list[bytes] = []
        if aggregate_params and (nals[0][0] >> 1) & 0x3F == 32:
            params = [n for n in nals if (n[0] >> 1) & 0x3F in (32, 33, 34)]
            ap = bytes([48 << 1, 1]) + b"".join(
                struct.pack(">H", len(n)) + n for n in params
            )
            payloads.append(ap)
            nals = [n for n in nals if n not in params]
        for nal in nals:
            payloads.extend(_rtp_payloads(nal))
        for index, payload in enumerate(payloads):
            packets.append(
                RtpPacket(
                    payload_type=95,
                    marker=index == len(payloads) - 1,
                    sequence_number=seq & 0xFFFF,
                    timestamp=ts,
                    ssrc=0,
                    payload=payload,
                )
            )
            seq += 1
        ts += 5400
    return packets


def _depacketize(packets: list[RtpPacket]) -> list[AccessUnit]:
    depacketizer = HevcDepacketizer()
    units: list[AccessUnit] = []
    for index, packet in enumerate(packets):
        units.extend(depacketizer.push(packet, now=index * 0.05))
    return units


def _decoded_count(units: list[AccessUnit]) -> int:
    decoder = HevcDecoder()
    return sum(len(decoder.decode(unit)) for unit in units)


# ---------------------------------------------------------------------------
# RTP parsing
# ---------------------------------------------------------------------------


class TestParseRtp:
    def test_camera_packet(self) -> None:
        data = bytes.fromhex("805f746fd3d2eff200000000") + CAMERA_VPS
        packet = parse_rtp(data)
        assert packet is not None
        assert packet.payload_type == 95
        assert packet.marker is False
        assert packet.sequence_number == 0x746F
        assert packet.ssrc == 0
        assert packet.payload == CAMERA_VPS

    def test_marker_csrc_extension_padding(self) -> None:
        header = bytes([0x80 | 0x20 | 0x10 | 1, 0x80 | 95]) + struct.pack(">HII", 1, 2, 3)
        csrc = b"\x00\x00\x00\x09"
        extension = b"\xbe\xde\x00\x01" + b"\x00" * 4
        payload = b"\x02\x01\xaa"
        packet = parse_rtp(header + csrc + extension + payload + b"\x00\x02")
        assert packet is not None
        assert packet.marker is True
        assert packet.payload == payload

    @pytest.mark.parametrize("data", [b"", b"\x80" * 5, bytes(12)])
    def test_rejects_garbage(self, data: bytes) -> None:
        assert parse_rtp(data) is None


# ---------------------------------------------------------------------------
# Depacketizer
# ---------------------------------------------------------------------------


def test_fixture_is_fragmented_like_the_camera() -> None:
    packets = _packets()
    assert any((p.payload[0] >> 1) & 0x3F == 49 for p in packets)
    assert len(_access_units()) == FIXTURE_PICTURES


def test_every_picture_decodes() -> None:
    units = _depacketize(_packets())
    assert len(units) == FIXTURE_PICTURES
    assert [u.keyframe for u in units].count(True) == 2
    assert _decoded_count(units) == FIXTURE_PICTURES


def test_aggregation_packets() -> None:
    units = _depacketize(_packets(aggregate_params=True))
    assert len(units) == FIXTURE_PICTURES
    assert _decoded_count(units) == FIXTURE_PICTURES


def test_sequence_wraparound() -> None:
    units = _depacketize(_packets(start_seq=0xFFF0))
    assert len(units) == FIXTURE_PICTURES


def test_lost_packet_skips_to_next_keyframe() -> None:
    packets = _packets()
    # drop a packet from the third picture
    third = [i for i, p in enumerate(packets) if p.timestamp == packets[0].timestamp + 2 * 5400]
    del packets[third[0]]
    units = _depacketize(packets)
    # pictures 1-2 decode, 3-6 are lost (broken / missing reference), 7-12 decode
    assert len(units) == 2 + 6
    assert units[2].keyframe
    assert _decoded_count(units) == len(units)


def test_duplicate_and_late_packets_are_ignored() -> None:
    packets = _packets()
    packets.insert(3, packets[2])  # duplicate
    packets.insert(6, packets[1])  # late
    assert len(_depacketize(packets)) == FIXTURE_PICTURES


def test_waits_for_keyframe_when_joining_mid_stream() -> None:
    packets = _packets()
    second_gop = next(
        i for i, p in enumerate(packets) if i and (p.payload[0] >> 1) & 0x3F == 32
    )
    units = _depacketize(packets[second_gop - 3 :])
    assert units[0].keyframe
    assert len(units) == 6


def test_keyframe_without_parameter_sets_gets_cached_ones() -> None:
    """Parameter sets seen once are prepended to later keyframes that lack them."""
    packets = _packets()
    second_vps = next(
        i for i, p in enumerate(packets) if i and (p.payload[0] >> 1) & 0x3F == 32
    )
    # remove the VPS/SPS/PPS of the second GOP, keep its IDR
    del packets[second_vps : second_vps + 3]
    packets = [
        RtpPacket(p.payload_type, p.marker, i, p.timestamp, p.ssrc, p.payload)
        for i, p in enumerate(packets)
    ]
    units = _depacketize(packets)
    assert len(units) == FIXTURE_PICTURES
    assert units[6].keyframe
    assert units[6].data.startswith(START_CODE + CAMERA_VPS[:2])
    assert _decoded_count(units) == FIXTURE_PICTURES


def test_timestamps_follow_arrival_and_stay_apart() -> None:
    """The camera's RTP clock is unreliable; pts come from arrival time."""
    depacketizer = HevcDepacketizer()
    packets = _packets()
    units: list[AccessUnit] = []
    for packet in packets:
        units.extend(depacketizer.push(packet, now=100.0))  # burst: same time
    pts = [u.pts for u in units]
    assert pts[0] == 0
    assert all(b - a == MIN_FRAME_INTERVAL for a, b in zip(pts, pts[1:]))

    later = [u.pts for p in _packets(start_seq=packets[-1].sequence_number + 1)
             for u in depacketizer.push(p, now=110.0)]
    assert later[0] == 10 * 90000


# ---------------------------------------------------------------------------
# PyAV pipelines
# ---------------------------------------------------------------------------


def _mux_to_bytes(transcode: bool, units: list[AccessUnit]) -> bytes:
    sink = ChunkSink()
    muxer = StreamMuxer(sink, "mpegts", transcode=transcode)
    out = b""
    for unit in units:
        muxer.write(unit)
        out += sink.drain()
    muxer.close()
    return out + sink.drain()


@pytest.mark.parametrize(
    ("transcode", "codec"), [(False, "hevc"), (True, "h264")]
)
def test_mpegts_output_plays(transcode: bool, codec: str) -> None:
    data = _mux_to_bytes(transcode, _depacketize(_packets()))
    with av.open(io.BytesIO(data), format="mpegts") as container:
        stream = container.streams.video[0]
        assert stream.codec_context.name == codec
        frames = list(container.decode(stream))
    assert len(frames) == FIXTURE_PICTURES
    assert (frames[0].width, frames[0].height) == (160, 120)
    pts = [f.pts for f in frames]
    assert pts == sorted(set(pts))


def test_muxer_starts_at_a_keyframe() -> None:
    units = _depacketize(_packets())
    sink = ChunkSink()
    muxer = StreamMuxer(sink, "mpegts", transcode=False)
    muxer.write(units[1])
    assert not muxer.started
    assert sink.drain() == b""
    muxer.write(units[0])
    assert muxer.started
    muxer.close()


def test_mp4_recording(tmp_path: Path) -> None:
    path = tmp_path / "clip.mp4"
    muxer = StreamMuxer(str(path), "mp4", transcode=True)
    for unit in _depacketize(_packets()):
        muxer.write(unit)
    muxer.close()
    with av.open(str(path)) as container:
        stream = container.streams.video[0]
        assert stream.codec_context.name == "h264"
        assert len(list(container.decode(stream))) == FIXTURE_PICTURES


def test_close_without_output_is_safe(tmp_path: Path) -> None:
    muxer = StreamMuxer(str(tmp_path / "none.mp4"), "mp4", transcode=True)
    muxer.close()
    assert not (tmp_path / "none.mp4").exists()


def test_decoder_survives_garbage() -> None:
    decoder = HevcDecoder()
    assert decoder.decode(AccessUnit(b"\x00\x00\x00\x01\x26\x01garbage", 0, False)) == []


def test_frame_to_jpeg() -> None:
    frame = HevcDecoder().decode(_depacketize(_packets())[0])[0]
    assert frame_to_jpeg(frame).startswith(b"\xff\xd8")
