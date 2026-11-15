"""Tests for decoding the camera's HEVC-over-H.264-RTP stream with aiortc.

The camera announces H.264 but sends H.265/HEVC, packed with H.264's RTP
packetization (whole NAL units, FU-A for large ones). The fixture is a real
HEVC clip (libx265, 160x120, 2 GOPs of 6, no B-frames) that the tests pack
the way the camera does and push through aiortc's real depayloader, jitter
buffer and decoder.

conftest.py replaces aiortc with a MagicMock for the whole suite, so this
module briefly lifts that stub to load the real aiortc, then puts it back.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

from custom_components.tuya_peephole.webrtc_session import (
    patch_aiortc_h264_decoder,
    patch_aiortc_h264_depayloader,
    split_annexb,
    split_hevc_access_units,
)

FIXTURE = Path(__file__).parent / "fixtures" / "camera_hevc.h265"
MTU_PAYLOAD = 1188  # what the camera uses


def _load_real_aiortc():
    def is_aiortc(name: str) -> bool:
        return name == "aiortc" or name.startswith("aiortc.")

    stubs = {k: v for k, v in sys.modules.items() if is_aiortc(k)}
    for name in stubs:
        del sys.modules[name]
    try:
        modules = tuple(
            importlib.import_module(f"aiortc.{m}")
            for m in ("codecs.h264", "jitterbuffer", "rtp")
        )
    except ImportError:
        modules = (None, None, None)
    finally:
        for name in [k for k in sys.modules if is_aiortc(k)]:
            del sys.modules[name]
        sys.modules.update(stubs)
    return modules


h264, jitterbuffer, rtp = _load_real_aiortc()
if h264 is None:
    pytest.skip("real aiortc not installed", allow_module_level=True)

# Captured before patching, to show what stock aiortc does with the stream
ORIGINAL_PARSE = h264.H264PayloadDescriptor.parse
ORIGINAL_DECODE = h264.H264Decoder.decode

# Real packets from the camera log
CAMERA_VPS = bytes.fromhex(
    "40010c01ffff016000000300b0000003000003007bac09"
)
CAMERA_SPS_PREFIX = bytes.fromhex("420101016000000300b0")


@pytest.fixture
def patched() -> None:
    assert patch_aiortc_h264_depayloader(h264) is True
    assert patch_aiortc_h264_decoder(h264) is True


# ---------------------------------------------------------------------------
# Camera-style packetization
# ---------------------------------------------------------------------------


def _packetize_nal(nal: bytes) -> list[bytes]:
    """Pack one HEVC NAL unit the way the camera does (H.264 RTP rules)."""
    if len(nal) <= MTU_PAYLOAD:
        return [nal]
    indicator = (nal[0] & 0xE0) | 28  # FU-A
    body = nal[1:]
    chunk = MTU_PAYLOAD - 2
    pieces = [body[i : i + chunk] for i in range(0, len(body), chunk)]
    out = []
    for index, piece in enumerate(pieces):
        header = nal[0] & 0x1F
        if index == 0:
            header |= 0x80
        if index == len(pieces) - 1:
            header |= 0x40
        out.append(bytes([indicator, header]) + piece)
    return out


def _camera_packets(same_ts_first_gop: bool = True) -> list:
    """RTP packets for the fixture. Like the camera's opening burst, the
    whole first GOP can share one RTP timestamp."""
    units = split_hevc_access_units(FIXTURE.read_bytes())
    packets, seq, ts = [], 57358, 3182752342
    for index, unit in enumerate(units):
        if index and not (same_ts_first_gop and index < 6):
            ts += 9000
        payloads = [p for nal in split_annexb(unit) for p in _packetize_nal(nal)]
        for p_index, payload in enumerate(payloads):
            packet = rtp.RtpPacket(
                payload_type=99,
                marker=int(p_index == len(payloads) - 1),
                sequence_number=seq,
                timestamp=ts,
            )
            packet.payload = payload
            packets.append(packet)
            seq += 1
    # trailing packet with a new timestamp so the last frame is released
    packets.append(rtp.RtpPacket(payload_type=99, sequence_number=seq, timestamp=ts + 9000))
    packets[-1].payload = bytes([0x02, 0x01, 0x00])
    return packets


def _decode_stream(parse, decoder) -> list:
    """Run packets through depayload + JitterBuffer + decoder like aiortc's
    RTCRtpReceiver and decoder_worker do."""
    buffer = jitterbuffer.JitterBuffer(capacity=512, is_video=True)
    frames = []
    for packet in _camera_packets():
        try:
            _, packet._data = parse(packet.payload)
        except ValueError:
            continue  # RTCRtpReceiver drops the packet
        _, encoded = buffer.add(packet)
        if encoded is None:
            continue
        try:
            frames.extend(decoder.decode(encoded))
        except h264.av.FFmpegError:
            continue  # decoder_worker logs and moves on
    return frames


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_fixture_looks_like_the_camera_stream() -> None:
    nals = split_annexb(FIXTURE.read_bytes())
    assert [(n[0] >> 1) & 0x3F for n in nals[:3]] == [32, 33, 34]
    assert len(nals[0]) == len(CAMERA_VPS)


def test_stock_aiortc_decodes_nothing() -> None:
    decoder = h264.H264Decoder()
    decoder.decode = ORIGINAL_DECODE.__get__(decoder)
    assert _decode_stream(ORIGINAL_PARSE, decoder) == []


@pytest.mark.usefixtures("patched")
def test_patched_aiortc_decodes_every_picture() -> None:
    frames = _decode_stream(h264.H264PayloadDescriptor.parse, h264.H264Decoder())
    assert len(frames) == 12
    assert (frames[0].width, frames[0].height) == (160, 120)
    # converts like the snapshot code does
    assert frames[-1].to_image().size == (160, 120)


@pytest.mark.usefixtures("patched")
class TestDepayloader:
    def test_camera_vps_is_kept(self) -> None:
        assert h264.h264_depayload(CAMERA_VPS) == b"\x00\x00\x00\x01" + CAMERA_VPS

    def test_camera_sps_unchanged(self) -> None:
        sps = CAMERA_SPS_PREFIX + b"\x00" * 10
        assert h264.h264_depayload(sps) == b"\x00\x00\x00\x01" + sps

    def test_camera_fu_a_rebuilds_hevc_idr_header(self) -> None:
        # first fragment of the camera's keyframe: 3c 86 01 af ...
        out = h264.h264_depayload(bytes.fromhex("3c8601af1574"))
        assert out == bytes.fromhex("00000001" "2601af1574")

    def test_annexb_prefixed_nal_is_parsed(self) -> None:
        sps = bytes([0x67, 0x42, 0x00, 0x1F])
        for start_code in (b"\x00\x00\x00\x01", b"\x00\x00\x01"):
            assert h264.h264_depayload(start_code + sps) == b"\x00\x00\x00\x01" + sps

    def test_real_errors_still_raise(self) -> None:
        with pytest.raises(ValueError):
            h264.h264_depayload(b"\x67")  # too short
        with pytest.raises(ValueError):
            h264.h264_depayload(bytes([0x19, 0x00, 0x00]))  # STAP-B

    def test_patches_are_idempotent(self) -> None:
        parse = h264.H264PayloadDescriptor.parse.__func__
        decode = h264.H264Decoder.decode
        assert patch_aiortc_h264_depayloader(h264) is True
        assert patch_aiortc_h264_decoder(h264) is True
        assert h264.H264PayloadDescriptor.parse.__func__ is parse
        assert h264.H264Decoder.decode is decode


@pytest.mark.usefixtures("patched")
def test_real_h264_still_uses_h264_decoder() -> None:
    decoder = h264.H264Decoder()
    frame = jitterbuffer.JitterFrame(
        data=b"\x00\x00\x00\x01\x67\x42\x00\x1f", timestamp=0
    )
    try:
        decoder.decode(frame)
    except h264.av.FFmpegError:
        pass
    assert decoder.codec.name == "h264"
    assert not getattr(decoder, "_tuya_hevc", False)


class TestAccessUnitSplit:
    def test_fixture_splits_into_one_unit_per_picture(self) -> None:
        units = split_hevc_access_units(FIXTURE.read_bytes())
        assert len(units) == 12

    def test_merged_burst_is_split(self) -> None:
        units = split_hevc_access_units(FIXTURE.read_bytes())
        merged = b"".join(units[:6])
        assert split_hevc_access_units(merged) == units[:6]
