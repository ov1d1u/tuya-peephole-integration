"""On-demand snapshot: one frame from the camera's live stream.

The Tuya web portal (protect-*.ismartlife.me) has no snapshot endpoint, and
its message center returns no alert thumbnails for these cameras. The live
stream is the only image source, so a snapshot reads the shared LiveStream
(connecting the camera if nobody else is watching), decodes a frame and
encodes it to JPEG.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from .const import (
    SNAPSHOT_SKIP_FRAMES,
    SNAPSHOT_TIMEOUT,
    WAKE_TIMEOUT,
    WEBRTC_SESSION_TIMEOUT,
)
from .live_stream import StreamClosedError, StreamReader
from .media import HevcDecoder, frame_to_jpeg
from .webrtc_session import AIORTC_AVAILABLE

if TYPE_CHECKING:
    from .coordinator import TuyaPeepholeCoordinator

_LOGGER = logging.getLogger(__name__)

# Waking and connecting the camera comes before the frame timeout.
CONNECT_TIMEOUT = WAKE_TIMEOUT + WEBRTC_SESSION_TIMEOUT


def _decode_until_settled(
    decoder: HevcDecoder, units: list[Any], state: dict[str, Any]
) -> None:
    """Decode access units, keeping the newest frame and a frame count."""
    for unit in units:
        for frame in decoder.decode(unit):
            state["frame"] = frame
            state["count"] += 1


async def async_read_frame(
    hass: Any, reader: StreamReader, connect_timeout: float, frame_timeout: float
) -> Any:
    """Return a settled decoded frame from a reader, or the newest one.

    The first frames after a keyframe can be grey or partly drawn while
    the decoder catches up, so SNAPSHOT_SKIP_FRAMES are skipped. Returns
    None if no frame arrives in time.
    """
    decoder = HevcDecoder()
    state: dict[str, Any] = {"frame": None, "count": 0}
    loop = asyncio.get_running_loop()
    deadline = loop.time() + connect_timeout
    first_frame_deadline: float | None = None
    while state["count"] <= SNAPSHOT_SKIP_FRAMES:
        end = first_frame_deadline or deadline
        remaining = end - loop.time()
        if remaining <= 0:
            break
        try:
            unit = await asyncio.wait_for(reader.get(), timeout=remaining)
        except (TimeoutError, StreamClosedError):
            break
        await hass.async_add_executor_job(
            _decode_until_settled, decoder, [unit], state
        )
        if state["frame"] is not None and first_frame_deadline is None:
            first_frame_deadline = loop.time() + frame_timeout
    return state["frame"]


async def async_capture_snapshot(
    coordinator: TuyaPeepholeCoordinator,
) -> bytes | None:
    """Grab one frame from the live stream and return it as JPEG.

    Returns None if aiortc is missing, the camera does not connect, or no
    frame arrives in time.
    """
    if not AIORTC_AVAILABLE:
        _LOGGER.error("aiortc is not installed -- cannot capture snapshot")
        return None

    try:
        reader = coordinator.live_stream.open()
    except StreamClosedError:
        return None
    with reader:
        frame = await async_read_frame(
            coordinator.hass, reader, CONNECT_TIMEOUT, SNAPSHOT_TIMEOUT
        )
    if frame is None:
        _LOGGER.warning("Snapshot: no video frame received from the camera")
        return None
    image = await coordinator.hass.async_add_executor_job(frame_to_jpeg, frame)
    _LOGGER.debug(
        "Snapshot captured: %dx%d, %d bytes", frame.width, frame.height, len(image)
    )
    return image
