"""One shared, on-demand video stream from the camera.

The camera serves one WebRTC session at a time and runs on battery, so
every consumer (live view restream, recordings, snapshots) reads from a
single LiveStream. The camera is woken and connected when the first reader
opens, and disconnected IDLE_TIMEOUT seconds after the last one closes.

Readers receive H.265 access units (hevc.AccessUnit). A new reader first
gets the pictures since the latest keyframe, so it can start decoding at
once when the stream is already running.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Callable
from typing import TYPE_CHECKING, Self

from .hevc import AccessUnit, HevcDepacketizer, parse_rtp
from .webrtc_session import CLOSE_REASON_BUSY, ServerWebRTCSession

if TYPE_CHECKING:
    from .coordinator import TuyaPeepholeCoordinator

_LOGGER = logging.getLogger(__name__)

IDLE_TIMEOUT = 10  # seconds to keep the camera connected after the last reader
NO_VIDEO_TIMEOUT = 15  # seconds without a picture before reconnecting
# The first picture normally arrives within a second of connecting. A
# camera that fell asleep unnoticed still answers signaling but sends none.
FIRST_PICTURE_TIMEOUT = 8
RETRY_DELAY_MIN = 2
RETRY_DELAY_MAX = 30
BUSY_RETRY_DELAY = 15  # camera refuses calls ~30-40 s after the last one
GOP_CACHE_LIMIT = 300  # pictures kept for new readers (~50 s at 6 fps)
READER_QUEUE_LIMIT = 150  # pictures a slow reader may fall behind


class _LiveSession(ServerWebRTCSession):
    _label = "Live stream"


class StreamClosedError(Exception):
    """The live stream was shut down (integration unloading)."""


class StreamReader:
    """A consumer's queue of access units from the LiveStream.

    Falling more than READER_QUEUE_LIMIT pictures behind drops the backlog
    and resumes at the next keyframe.
    """

    def __init__(self, release: Callable[[StreamReader], None]) -> None:
        """Initialize (use LiveStream.open() instead)."""
        self._release = release
        self._queue: asyncio.Queue[AccessUnit | None] = asyncio.Queue()
        self._skip_to_keyframe = False
        self._closed = False

    def put(self, unit: AccessUnit) -> None:
        """Queue one access unit (called by the LiveStream)."""
        if self._closed:
            return
        if self._queue.qsize() >= READER_QUEUE_LIMIT:
            _LOGGER.debug("Stream reader fell behind; skipping to next keyframe")
            while not self._queue.empty():
                self._queue.get_nowait()
            self._skip_to_keyframe = True
        if self._skip_to_keyframe:
            if not unit.keyframe:
                return
            self._skip_to_keyframe = False
        self._queue.put_nowait(unit)

    def end(self) -> None:
        """Signal that no more access units will come."""
        self._queue.put_nowait(None)

    async def get(self) -> AccessUnit:
        """Wait for the next access unit.

        Raises StreamClosedError once the stream is shut down.
        """
        unit = await self._queue.get()
        if unit is None:
            self._queue.put_nowait(None)  # keep later get() calls failing
            raise StreamClosedError
        return unit

    def close(self) -> None:
        """Stop reading; the camera disconnects when no readers remain."""
        if not self._closed:
            self._closed = True
            self._release(self)

    def __enter__(self) -> Self:
        """Use as a context manager that closes the reader."""
        return self

    def __exit__(self, *exc: object) -> None:
        """Close the reader."""
        self.close()


class LiveStream:
    """The camera's video, shared by all readers and started on demand."""

    def __init__(self, coordinator: TuyaPeepholeCoordinator) -> None:
        """Initialize (nothing connects until a reader opens)."""
        self._coordinator = coordinator
        self._readers: set[StreamReader] = set()
        self._gop: deque[AccessUnit] = deque(maxlen=GOP_CACHE_LIMIT)
        self._task: asyncio.Task[None] | None = None
        self._idle_handle: asyncio.TimerHandle | None = None
        self._shutdown = False
        self._last_picture = 0.0

    @property
    def active(self) -> bool:
        """Return True while the stream is connecting or connected."""
        return self._task is not None and not self._task.done()

    @property
    def reader_count(self) -> int:
        """Return the number of open readers."""
        return len(self._readers)

    def open(self) -> StreamReader:
        """Open a reader, starting the camera stream if needed."""
        if self._shutdown:
            raise StreamClosedError
        reader = StreamReader(self._release)
        for unit in self._gop:
            reader.put(unit)
        self._readers.add(reader)
        if self._idle_handle is not None:
            self._idle_handle.cancel()
            self._idle_handle = None
        if not self.active:
            self._task = self._coordinator.hass.async_create_background_task(
                self._async_run(), "tuya_peephole live stream"
            )
        return reader

    def _release(self, reader: StreamReader) -> None:
        self._readers.discard(reader)
        if not self._readers and self.active and self._idle_handle is None:
            self._idle_handle = asyncio.get_running_loop().call_later(
                IDLE_TIMEOUT, self._stop_if_idle
            )

    def _stop_if_idle(self) -> None:
        self._idle_handle = None
        if not self._readers and self._task is not None:
            _LOGGER.debug("Live stream idle; disconnecting from camera")
            self._task.cancel()

    def _dispatch(self, unit: AccessUnit) -> None:
        self._last_picture = time.monotonic()
        if unit.keyframe:
            self._gop.clear()
        self._gop.append(unit)
        for reader in list(self._readers):
            reader.put(unit)

    async def _async_run(self) -> None:
        """Keep a camera session up while there are readers."""
        delay = RETRY_DELAY_MIN
        try:
            while self._readers or self._idle_handle is not None:
                session_ok, busy = await self._async_run_session()
                if not (self._readers or self._idle_handle is not None):
                    break
                if session_ok:
                    delay = RETRY_DELAY_MIN
                wait = BUSY_RETRY_DELAY if busy else delay
                _LOGGER.debug("Live stream reconnecting in %ds", wait)
                await asyncio.sleep(wait)
                if not session_ok:
                    delay = min(delay * 2, RETRY_DELAY_MAX)
        finally:
            self._gop.clear()
            if self._idle_handle is not None:
                self._idle_handle.cancel()
                self._idle_handle = None

    async def _async_run_session(self) -> tuple[bool, bool]:
        """Run one camera session until it ends.

        Returns (delivered_video, camera_was_busy).
        """
        coordinator = self._coordinator
        # Always wake first: the CRC32 wake also prepares the camera for
        # WebRTC. Auth tokens expire, so fetch fresh WebRTC config.
        if not await coordinator.async_wake_camera(force=True):
            _LOGGER.warning("Camera did not confirm wake, connecting anyway")
        coordinator.api.invalidate_cache()

        depacketizer = HevcDepacketizer()
        ended = asyncio.Event()

        def on_packet(data: bytes, video: bool) -> None:
            if not video:
                return
            packet = parse_rtp(data)
            if packet is None:
                return
            for unit in depacketizer.push(packet, time.monotonic()):
                self._dispatch(unit)

        session = _LiveSession(
            coordinator, on_packet=on_packet, on_closed=ended.set
        )
        self._gop.clear()
        delivered = False
        try:
            if not await session.async_start():
                return False, session.close_reason == CLOSE_REASON_BUSY
            _LOGGER.debug("Live stream connected")
            self._last_picture = time.monotonic()
            while not ended.is_set():
                try:
                    await asyncio.wait_for(ended.wait(), timeout=1)
                except TimeoutError:
                    pass
                if self._gop:
                    delivered = True
                silent = time.monotonic() - self._last_picture
                if not delivered and silent > FIRST_PICTURE_TIMEOUT:
                    _LOGGER.debug("No video after connecting; waking camera again")
                    coordinator.mark_camera_asleep("connected without video")
                    break
                if silent > NO_VIDEO_TIMEOUT:
                    _LOGGER.warning(
                        "No video from camera for %ds; reconnecting",
                        NO_VIDEO_TIMEOUT,
                    )
                    break
            return delivered, session.close_reason == CLOSE_REASON_BUSY
        finally:
            await asyncio.shield(session.async_stop())

    async def async_shutdown(self) -> None:
        """Disconnect and end all readers (integration unload)."""
        self._shutdown = True
        if self._idle_handle is not None:
            self._idle_handle.cancel()
            self._idle_handle = None
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        for reader in list(self._readers):
            reader.end()
        self._readers.clear()
