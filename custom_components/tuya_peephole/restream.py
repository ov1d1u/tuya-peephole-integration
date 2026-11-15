"""Local MPEG-TS restream of the camera for Home Assistant's players.

Serves the shared LiveStream over plain HTTP on 127.0.0.1 so the camera
entity can offer it as its stream_source(). Home Assistant's go2rtc (or
its HLS "stream" component when go2rtc is not available) reads it and
handles playback in the browser.

Two URLs per camera:
- .../hevc.ts: the camera's H.265, remuxed only (no CPU cost; needs a
  browser/device that can decode H.265).
- .../h264.ts: transcoded to H.264 (plays in every browser).

The camera is only connected while at least one client reads a URL. The
URLs carry a random token, so other local processes cannot guess them.
"""

from __future__ import annotations

import asyncio
import logging
import secrets
from typing import TYPE_CHECKING

from aiohttp import web

from .live_stream import StreamClosedError
from .media import ChunkSink, StreamMuxer

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

    from .live_stream import LiveStream

_LOGGER = logging.getLogger(__name__)

CODEC_H264 = "h264"
CODEC_HEVC = "hevc"
CODECS = (CODEC_H264, CODEC_HEVC)
BIND_HOST = "127.0.0.1"


class RestreamServer:
    """HTTP server publishing the live stream as MPEG-TS."""

    def __init__(self, hass: HomeAssistant, live_stream: LiveStream) -> None:
        """Initialize (call async_start before use)."""
        self._hass = hass
        self._live_stream = live_stream
        self._token = secrets.token_urlsafe(16)
        self._runner: web.AppRunner | None = None
        self._port: int | None = None
        self._clients: set[asyncio.Task[object]] = set()

    @property
    def port(self) -> int | None:
        """Return the listening port (None before start)."""
        return self._port

    def url(self, codec: str) -> str | None:
        """Return the stream URL for a codec, or None if not running."""
        if self._port is None:
            return None
        return f"http://{BIND_HOST}:{self._port}/{self._token}/{codec}.ts"

    async def async_start(self) -> None:
        """Start listening on a free local port."""
        app = web.Application()
        app.router.add_get("/{token}/{codec}.ts", self._handle_stream)
        runner = web.AppRunner(app, handle_signals=False, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, BIND_HOST, 0)
        await site.start()
        self._runner = runner
        self._port = runner.addresses[0][1]
        _LOGGER.debug("Restream server listening on %s:%s", BIND_HOST, self._port)

    async def async_stop(self) -> None:
        """Disconnect clients and stop listening."""
        for task in list(self._clients):
            task.cancel()
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
        self._port = None

    async def _handle_stream(self, request: web.Request) -> web.StreamResponse:
        if not secrets.compare_digest(request.match_info["token"], self._token):
            raise web.HTTPNotFound
        codec = request.match_info["codec"]
        if codec not in CODECS:
            raise web.HTTPNotFound

        task = asyncio.current_task()
        if task is not None:
            self._clients.add(task)
        try:
            reader = self._live_stream.open()
        except StreamClosedError as err:
            raise web.HTTPServiceUnavailable from err

        _LOGGER.debug("Restream client connected (%s)", codec)
        response = web.StreamResponse(
            headers={"Content-Type": "video/mp2t", "Cache-Control": "no-store"}
        )
        sink = ChunkSink()
        muxer = StreamMuxer(sink, "mpegts", transcode=codec == CODEC_H264)

        def mux(unit: object) -> bytes:
            muxer.write(unit)  # type: ignore[arg-type]
            return sink.drain()

        try:
            await response.prepare(request)
            with reader:
                while True:
                    unit = await reader.get()
                    data = await self._hass.async_add_executor_job(mux, unit)
                    if data:
                        await response.write(data)
        except (ConnectionResetError, StreamClosedError):
            pass
        finally:
            if task is not None:
                self._clients.discard(task)
            await self._hass.async_add_executor_job(muxer.close)
            _LOGGER.debug("Restream client disconnected (%s)", codec)
        return response
