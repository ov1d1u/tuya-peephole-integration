"""PyAV pipelines for the camera's H.265 access units.

Everything here is blocking (PyAV calls into FFmpeg), so callers run it in
an executor. Used by the live restream (MPEG-TS), recordings (MP4) and
snapshots (one decoded frame).
"""

from __future__ import annotations

import io
import logging
from fractions import Fraction
from typing import Any

import av

from .hevc import AccessUnit

_LOGGER = logging.getLogger(__name__)

TIME_BASE = Fraction(1, 90000)

# libx264 settings for live view and recordings. The camera sends ~6 fps,
# so even 1080p costs little CPU. Keyframes follow the camera's keyframes.
H264_OPTIONS = {
    "preset": "veryfast",
    "tune": "zerolatency",
    "crf": "26",
    "x264-params": "forced-idr=1",
}
H264_MAX_GOP = 250


class ChunkSink(io.RawIOBase):
    """Write-only file object that collects what a muxer writes."""

    def __init__(self) -> None:
        """Initialize an empty sink."""
        super().__init__()
        self._chunks: list[bytes] = []

    def writable(self) -> bool:
        """Return True (PyAV checks this)."""
        return True

    def write(self, data: Any) -> int:
        """Store a chunk of muxer output."""
        self._chunks.append(bytes(data))
        return len(data)

    def drain(self) -> bytes:
        """Return and clear everything written so far."""
        data = b"".join(self._chunks)
        self._chunks.clear()
        return data


class HevcDecoder:
    """Decode access units to frames, skipping undecodable ones."""

    def __init__(self) -> None:
        """Create the H.265 decoder."""
        self._codec = av.CodecContext.create("hevc", "r")

    def decode(self, unit: AccessUnit) -> list[Any]:
        """Decode one access unit; return the frames it produced."""
        packet = av.Packet(unit.data)
        packet.pts = packet.dts = unit.pts
        packet.time_base = TIME_BASE
        try:
            return list(self._codec.decode(packet))
        except av.error.FFmpegError as err:
            _LOGGER.debug("H.265 decode error: %s", err)
            return []


class StreamMuxer:
    """Mux access units into a container.

    transcode=False copies the H.265 as is (cheap; needs an H.265-capable
    player). transcode=True decodes it and encodes H.264 (plays everywhere).
    Output starts at the first keyframe; timestamps start at zero.
    """

    def __init__(self, target: Any, container_format: str, transcode: bool) -> None:
        """Prepare a muxer writing to a path or file object."""
        self._target = target
        self._format = container_format
        self._transcode = transcode
        self._out: Any = None
        self._stream: Any = None
        self._template: Any = None
        self._decoder: HevcDecoder | None = None
        self._base_pts: int | None = None
        self._last_pts = -1

    @property
    def started(self) -> bool:
        """Return True once output has begun (a keyframe was seen)."""
        return self._out is not None

    def write(self, unit: AccessUnit) -> None:
        """Add one access unit to the output."""
        if self._out is None:
            if not unit.keyframe:
                return
            self._out = av.open(self._target, "w", format=self._format)
            self._base_pts = unit.pts
            if self._transcode:
                self._decoder = HevcDecoder()
            else:
                # The stream parameters (size, profile) come from the
                # keyframe's VPS/SPS/PPS, read through a tiny input.
                self._template = av.open(io.BytesIO(unit.data), format="hevc")
                self._stream = self._out.add_stream_from_template(
                    self._template.streams.video[0]
                )
                self._stream.time_base = TIME_BASE

        pts = max(unit.pts - (self._base_pts or 0), self._last_pts + 1)
        self._last_pts = pts
        if self._transcode:
            self._encode(unit, pts)
        else:
            packet = av.Packet(unit.data)
            packet.stream = self._stream
            packet.pts = packet.dts = pts
            packet.time_base = TIME_BASE
            packet.is_keyframe = unit.keyframe
            self._out.mux(packet)

    def _encode(self, unit: AccessUnit, pts: int) -> None:
        assert self._decoder is not None
        for frame in self._decoder.decode(unit):
            if self._stream is None:
                self._stream = self._out.add_stream("libx264", rate=10)
                self._stream.width = frame.width
                self._stream.height = frame.height
                self._stream.pix_fmt = "yuv420p"
                self._stream.codec_context.time_base = TIME_BASE
                self._stream.codec_context.gop_size = H264_MAX_GOP
                self._stream.options = dict(H264_OPTIONS)
            out_frame = frame.reformat(format="yuv420p")
            out_frame.pts = pts
            out_frame.time_base = TIME_BASE
            if unit.keyframe:
                out_frame.pict_type = av.video.frame.PictureType.I
            for packet in self._stream.encode(out_frame):
                self._out.mux(packet)

    def close(self) -> None:
        """Flush and close the output (finalizes MP4 files)."""
        try:
            if self._out is not None:
                if self._transcode and self._stream is not None:
                    for packet in self._stream.encode(None):
                        self._out.mux(packet)
                self._out.close()
        finally:
            self._out = None
            if self._template is not None:
                self._template.close()
                self._template = None


def frame_to_jpeg(frame: Any) -> bytes:
    """Encode a decoded av.VideoFrame as JPEG."""
    image = frame.to_image()
    buf = io.BytesIO()
    image.save(buf, format="JPEG", quality=85)
    return buf.getvalue()
