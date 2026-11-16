"""On-demand snapshot by grabbing one frame from the camera's WebRTC stream.

The Tuya web portal (protect-*.ismartlife.me) has no snapshot endpoint, and
its message center returns no alert thumbnails for these cameras. The live
WebRTC stream is the only image source, so a snapshot opens a short
server-side session, decodes a frame, encodes it to JPEG, and disconnects.
"""

from __future__ import annotations

import asyncio
import io
import logging
from typing import TYPE_CHECKING, Any

from .const import SNAPSHOT_SKIP_FRAMES, SNAPSHOT_TIMEOUT
from .webrtc_session import AIORTC_AVAILABLE, ServerWebRTCSession

if TYPE_CHECKING:
    from .coordinator import TuyaPeepholeCoordinator

_LOGGER = logging.getLogger(__name__)


class SnapshotSession(ServerWebRTCSession):
    """WebRTC session that keeps a single decoded video frame."""

    _label = "Snapshot"

    def __init__(self, coordinator: TuyaPeepholeCoordinator) -> None:
        """Initialize the snapshot session."""
        super().__init__(coordinator)
        self._frame_ready = asyncio.Event()
        self._latest_frame: Any = None  # av.VideoFrame
        self._reader_task: asyncio.Task[None] | None = None

    async def _async_on_video_track(self, track: Any) -> None:
        """Start reading decoded frames from the camera's video track."""
        self._reader_task = asyncio.get_running_loop().create_task(
            self._async_read_frames(track)
        )

    async def _async_read_frames(self, track: Any) -> None:
        """Keep the newest frame; signal once the stream has settled.

        The first decoded frames after connecting can be grey or partially
        drawn while the decoder catches up, so a few are skipped.
        """
        count = 0
        try:
            while True:
                frame = await track.recv()
                self._latest_frame = frame
                count += 1
                if count > SNAPSHOT_SKIP_FRAMES:
                    self._frame_ready.set()
                    return
        except asyncio.CancelledError:
            raise
        except Exception:
            # MediaStreamError when the track ends, or a decode error
            _LOGGER.debug("Snapshot frame reader stopped", exc_info=True)
        finally:
            if self._latest_frame is not None:
                self._frame_ready.set()

    async def async_wait_frame(self, timeout: float) -> Any:
        """Wait for a settled frame; on timeout return the newest one, if any."""
        try:
            await asyncio.wait_for(self._frame_ready.wait(), timeout=timeout)
        except TimeoutError:
            _LOGGER.debug(
                "Snapshot: no settled frame after %ss (have frame: %s)",
                timeout,
                self._latest_frame is not None,
            )
        return self._latest_frame

    async def _async_close_media(self) -> None:
        """Stop reading frames."""
        if self._reader_task is not None and not self._reader_task.done():
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass


def frame_to_jpeg(frame: Any) -> bytes:
    """Encode a decoded av.VideoFrame as JPEG (blocking, run in executor)."""
    image = frame.to_image()
    buf = io.BytesIO()
    image.save(buf, format="JPEG", quality=85)
    return buf.getvalue()


async def async_capture_snapshot(
    coordinator: TuyaPeepholeCoordinator,
) -> bytes | None:
    """Wake the camera, grab one frame over WebRTC, and return it as JPEG.

    Returns None if aiortc is missing, the camera does not answer, or no
    frame arrives within SNAPSHOT_TIMEOUT seconds.
    """
    if not AIORTC_AVAILABLE:
        _LOGGER.error("aiortc is not installed -- cannot capture snapshot")
        return None

    # Same flow as the official app's live view: always send the CRC32 wake before an offer,
    # and fetch fresh WebRTC config because auth tokens expire.
    if not await coordinator.async_wake_camera(force=True):
        _LOGGER.warning("Camera did not confirm wake, trying snapshot anyway")
    coordinator.api.invalidate_cache()

    session = SnapshotSession(coordinator)
    try:
        if not await session.async_start():
            _LOGGER.warning("Snapshot: could not open a stream to the camera")
            return None
        frame = await session.async_wait_frame(SNAPSHOT_TIMEOUT)
        if frame is None:
            _LOGGER.warning(
                "Snapshot: no video frame received within %ds",
                SNAPSHOT_TIMEOUT,
            )
            return None
        image = await coordinator.hass.async_add_executor_job(
            frame_to_jpeg, frame
        )
        _LOGGER.debug(
            "Snapshot captured: %dx%d, %d bytes",
            frame.width,
            frame.height,
            len(image),
        )
        return image
    finally:
        await session.async_stop()
