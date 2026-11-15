"""Tests for the datachannel WebRTC session, the shared LiveStream and the
local MPEG-TS restream server."""

from __future__ import annotations

import asyncio
import io
import json
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import av
import pytest

from custom_components.tuya_peephole import live_stream as ls
from custom_components.tuya_peephole import webrtc_session
from custom_components.tuya_peephole.hevc import AccessUnit
from custom_components.tuya_peephole.live_stream import (
    LiveStream,
    StreamClosedError,
    StreamReader,
)
from custom_components.tuya_peephole.webrtc_session import ServerWebRTCSession

from .test_hevc_stream import FIXTURE_PICTURES, _depacketize, _packets


def _signal(msg_type: str, session_id: str, msg: dict) -> bytes:
    return json.dumps(
        {
            "protocol": 302,
            "data": {"header": {"type": msg_type, "sessionid": session_id}, "msg": msg},
        }
    ).encode()


def _mock_pc() -> MagicMock:
    pc = MagicMock()
    pc.createOffer = AsyncMock(return_value=MagicMock(sdp="v=0\r\n"))
    pc.setLocalDescription = AsyncMock()
    pc.setRemoteDescription = AsyncMock()
    pc.localDescription.sdp = "v=0\r\na=extmap:1 foo\r\n"
    pc.close = AsyncMock()
    pc.connectionState = "new"
    return pc


class _Channel:
    """RTCDataChannel stand-in that records sent messages."""

    def __init__(self) -> None:
        self.handlers: dict[str, object] = {}
        self.sent: list[str] = []
        self.readyState = "open"

    def on(self, event: str):
        def register(fn):
            self.handlers[event] = fn
            return fn

        return register

    def send(self, message: str) -> None:
        self.sent.append(message)


# ---------------------------------------------------------------------------
# ServerWebRTCSession
# ---------------------------------------------------------------------------


class TestServerSession:
    def _patches(self, pc):
        return (
            patch.object(webrtc_session, "AIORTC_AVAILABLE", True),
            patch.object(webrtc_session, "RTCPeerConnection", return_value=pc, create=True),
            patch.object(webrtc_session, "RTCConfiguration", create=True),
            patch.object(webrtc_session, "RTCSessionDescription", create=True),
        )

    async def _start(self, coord, session, pc, reply):
        """Run async_start, answering the published offer with reply()."""

        def on_publish(topic, payload, qos=0):
            message = json.loads(payload)
            if message["data"]["header"]["type"] == "offer":
                asyncio.get_running_loop().call_soon(reply, message)

        coord.mqtt_client.publish = MagicMock(side_effect=on_publish)
        p1, p2, p3, p4 = self._patches(pc)
        with p1, p2, p3, p4:
            return await session.async_start()

    @pytest.mark.asyncio
    async def test_offer_asks_for_datachannel(self, mock_recording_coordinator) -> None:
        """Audio before video, a fmp4Stream channel, datachannel_enable, msid."""
        coord = mock_recording_coordinator
        pc = _mock_pc()
        session = ServerWebRTCSession(coord)
        offers = []

        def reply(message):
            offers.append(message)
            session._on_signaling_message(
                _signal("answer", message["data"]["header"]["sessionid"], {"sdp": "v=0"})
            )

        assert await self._start(coord, session, pc, reply) is True

        kinds = [c.args[0] for c in pc.addTransceiver.call_args_list]
        assert kinds == ["audio", "video"]
        pc.createDataChannel.assert_called_once()
        assert pc.createDataChannel.call_args.args[0] == "fmp4Stream"
        offer = offers[0]["data"]["msg"]
        assert offer["datachannel_enable"] is True
        assert "extmap" not in offer["sdp"]
        assert offers[0]["data"]["header"]["from"] == "test_msid_789"
        coord.mqtt_client.subscribe.assert_any_call("/av/u/test_msid_789", qos=1)
        coord.api.async_get_mqtt_config.assert_not_called()
        await session.async_stop()

    @pytest.mark.asyncio
    async def test_busy_camera(self, mock_recording_coordinator) -> None:
        """A disconnect instead of an answer fails fast and keeps close_reason."""
        coord = mock_recording_coordinator
        session = ServerWebRTCSession(coord)

        def reply(message):
            session._on_signaling_message(
                _signal(
                    "disconnect",
                    message["data"]["header"]["sessionid"],
                    {"mode": "webrtc", "close_reason": 93},
                )
            )

        assert await self._start(coord, session, _mock_pc(), reply) is False
        assert session.close_reason == webrtc_session.CLOSE_REASON_BUSY
        assert session.stopped

    @pytest.mark.asyncio
    async def test_no_mqtt(self, mock_recording_coordinator) -> None:
        coord = mock_recording_coordinator
        coord.mqtt_client = None
        p1, p2, p3, p4 = self._patches(_mock_pc())
        with p1, p2, p3, p4:
            assert await ServerWebRTCSession(coord).async_start() is False

    @pytest.mark.asyncio
    async def test_camera_hangup_after_start(self, mock_recording_coordinator) -> None:
        """A disconnect during the session stops it and calls on_closed."""
        coord = mock_recording_coordinator
        closed = MagicMock()
        session = ServerWebRTCSession(coord, on_closed=closed)

        def reply(message):
            session._on_signaling_message(
                _signal("answer", message["data"]["header"]["sessionid"], {"sdp": "v=0"})
            )

        assert await self._start(coord, session, _mock_pc(), reply) is True
        session._on_signaling_message(
            _signal("disconnect", session._session_id, {"close_reason": 100})
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert session.stopped
        closed.assert_called_once()
        coord.mqtt_client.message_callback_remove.assert_called_with(
            "/av/u/test_msid_789", session._signaling_cb
        )

    def test_datachannel_handshake_and_routing(self, mock_recording_coordinator) -> None:
        packets = []
        session = ServerWebRTCSession(
            mock_recording_coordinator,
            on_packet=lambda data, video: packets.append((data[11], video)),
        )
        channel = _Channel()
        session._pc = MagicMock()
        session._pc.createDataChannel.return_value = channel
        session._setup_datachannel()

        def sent_types():
            return [json.loads(m)["type"] for m in channel.sent]

        channel.handlers["open"]()
        assert sent_types() == ["codec"]
        channel.handlers["message"]('{"type":"codec","msg":"hev1.1.6.L123.b0"}')
        assert json.loads(channel.sent[-1]) == {"type": "start", "msg": "frame"}

        rtp = bytes.fromhex("805f746fd3d2eff2000000")
        channel.handlers["message"](rtp + b"\x00" + b"x")  # before recv: dropped
        assert packets == []

        channel.handlers["message"](
            json.dumps({"type": "recv", "msg": '{"video":{"ssrc":0},"audio":{"ssrc":1}}'})
        )
        assert sent_types()[-1] == "complete"
        channel.handlers["message"](rtp + b"\x00" + b"x")
        channel.handlers["message"](rtp + b"\x01" + b"x")
        channel.handlers["message"](rtp + b"\x07" + b"x")  # unknown SSRC
        channel.handlers["message"]("not json")
        assert packets == [(0, True), (1, False)]


# ---------------------------------------------------------------------------
# StreamReader
# ---------------------------------------------------------------------------


def _unit(index: int, keyframe: bool = False) -> AccessUnit:
    return AccessUnit(data=bytes([index % 256]), pts=index, keyframe=keyframe)


class TestStreamReader:
    @pytest.mark.asyncio
    async def test_overflow_resumes_at_keyframe(self) -> None:
        reader = StreamReader(MagicMock())
        for i in range(ls.READER_QUEUE_LIMIT):
            reader.put(_unit(i, keyframe=i == 0))
        reader.put(_unit(1000))  # overflow: backlog dropped, waits for keyframe
        reader.put(_unit(1001))
        reader.put(_unit(1002, keyframe=True))
        assert (await reader.get()).pts == 1002

    @pytest.mark.asyncio
    async def test_end_and_close(self) -> None:
        release = MagicMock()
        reader = StreamReader(release)
        reader.end()
        with pytest.raises(StreamClosedError):
            await reader.get()
        with pytest.raises(StreamClosedError):
            await reader.get()
        with reader:
            pass
        reader.close()
        release.assert_called_once_with(reader)


# ---------------------------------------------------------------------------
# LiveStream
# ---------------------------------------------------------------------------


@pytest.fixture
def live(mock_recording_coordinator):
    coord = mock_recording_coordinator
    coord.hass.async_create_background_task = MagicMock(
        side_effect=lambda coro, name: asyncio.get_running_loop().create_task(coro)
    )
    return LiveStream(coord)


def _fake_session(stream: LiveStream, units, result=(True, False), hold=None):
    """Replace _async_run_session with one that dispatches units."""
    calls = []

    async def run():
        calls.append(1)
        for unit in units:
            stream._dispatch(unit)
        if hold is not None:
            await hold.wait()
        return result

    stream._async_run_session = run
    return calls


class TestLiveStream:
    @pytest.mark.asyncio
    async def test_first_reader_starts_stream(self, live) -> None:
        hold = asyncio.Event()
        calls = _fake_session(live, [_unit(0, True), _unit(1)], hold=hold)
        reader = live.open()
        assert (await reader.get()).pts == 0
        assert (await reader.get()).pts == 1
        assert live.active and calls == [1]

        # a second reader shares the session and gets the cached GOP
        second = live.open()
        assert (await second.get()).pts == 0
        assert calls == [1]
        assert live.reader_count == 2
        reader.close()
        second.close()
        await live.async_shutdown()

    @pytest.mark.asyncio
    async def test_gop_cache_starts_at_latest_keyframe(self, live) -> None:
        hold = asyncio.Event()
        _fake_session(
            live, [_unit(0, True), _unit(1), _unit(2, True), _unit(3)], hold=hold
        )
        first = live.open()
        for _ in range(4):
            await first.get()
        late = live.open()
        assert [(await late.get()).pts for _ in range(2)] == [2, 3]
        await live.async_shutdown()

    @pytest.mark.asyncio
    async def test_idle_disconnect(self, live) -> None:
        hold = asyncio.Event()
        _fake_session(live, [_unit(0, True)], hold=hold)
        with patch.object(ls, "IDLE_TIMEOUT", 0.01):
            reader = live.open()
            await reader.get()
            reader.close()
            assert live.active  # grace period
            await asyncio.sleep(0.05)
        assert not live.active

    @pytest.mark.asyncio
    async def test_reopen_within_grace_keeps_session(self, live) -> None:
        hold = asyncio.Event()
        calls = _fake_session(live, [_unit(0, True)], hold=hold)
        with patch.object(ls, "IDLE_TIMEOUT", 0.05):
            live.open().close()
            await asyncio.sleep(0.01)
            reader = live.open()
            await asyncio.sleep(0.1)
            assert live.active
        assert calls == [1]
        reader.close()
        await live.async_shutdown()

    @pytest.mark.asyncio
    async def test_retries_while_readers_remain(self, live) -> None:
        calls = _fake_session(live, [], result=(False, True))
        with (
            patch.object(ls, "BUSY_RETRY_DELAY", 0),
            patch.object(ls, "RETRY_DELAY_MIN", 0),
        ):
            reader = live.open()
            await asyncio.sleep(0.02)
            assert len(calls) > 1
            reader.close()
        await live.async_shutdown()

    @pytest.mark.asyncio
    async def test_shutdown_ends_readers(self, live) -> None:
        _fake_session(live, [], hold=asyncio.Event())
        reader = live.open()
        await live.async_shutdown()
        with pytest.raises(StreamClosedError):
            await reader.get()
        with pytest.raises(StreamClosedError):
            live.open()

    @pytest.mark.asyncio
    async def test_session_wakes_and_feeds_depacketizer(
        self, mock_recording_coordinator
    ) -> None:
        """_async_run_session wakes the camera and turns RTP into pictures."""
        coord = mock_recording_coordinator
        stream = LiveStream(coord)
        reader = StreamReader(MagicMock())
        stream._readers.add(reader)

        class FakeSession:
            close_reason = None

            def __init__(self, coordinator, on_packet, on_closed):
                self.on_packet, self.on_closed = on_packet, on_closed

            async def async_start(self):
                for packet in _packets():
                    header = bytes([0x80 | 0, (0x80 if packet.marker else 0) | 95])
                    header += packet.sequence_number.to_bytes(2, "big")
                    header += packet.timestamp.to_bytes(4, "big") + bytes(4)
                    self.on_packet(header + packet.payload, True)
                self.on_packet(b"audio", False)
                asyncio.get_running_loop().call_soon(self.on_closed)
                return True

            async def async_stop(self):
                pass

        with patch.object(ls, "_LiveSession", FakeSession):
            delivered, busy = await stream._async_run_session()

        assert (delivered, busy) == (True, False)
        coord.async_wake_camera.assert_awaited_once_with(force=True)
        coord.api.invalidate_cache.assert_called()
        assert reader._queue.qsize() == FIXTURE_PICTURES


    @pytest.mark.asyncio
    async def test_no_first_picture_marks_camera_asleep(
        self, mock_recording_coordinator
    ) -> None:
        """Connected but silent (camera fell asleep unnoticed): give up
        quickly and make the next attempt wait for a real wake."""
        coord = mock_recording_coordinator
        stream = LiveStream(coord)

        class SilentSession:
            close_reason = None

            def __init__(self, coordinator, on_packet, on_closed):
                pass

            async def async_start(self):
                return True

            async def async_stop(self):
                pass

        clock = iter(range(0, 1000, 5))
        with (
            patch.object(ls, "_LiveSession", SilentSession),
            patch.object(ls.time, "monotonic", side_effect=lambda: next(clock)),
            patch.object(ls.asyncio, "wait_for", AsyncMock(side_effect=TimeoutError)),
        ):
            delivered, busy = await stream._async_run_session()

        assert (delivered, busy) == (False, False)
        coord.mark_camera_asleep.assert_called_once()


# ---------------------------------------------------------------------------
# RestreamServer
# ---------------------------------------------------------------------------


class TestRestreamServer:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(("codec", "expected"), [("hevc", "hevc"), ("h264", "h264")])
    async def test_serves_mpegts(self, mock_hass_with_loop, codec, expected) -> None:
        from custom_components.tuya_peephole.restream import RestreamServer

        from .test_snapshot import _FakeReader

        hass = mock_hass_with_loop
        hass.async_add_executor_job = AsyncMock(side_effect=lambda fn, *a: fn(*a))
        reader = _FakeReader(_depacketize(_packets()), end=True)
        stream = MagicMock()
        stream.open.return_value = reader

        server = RestreamServer(hass, stream)
        await server.async_start()
        try:
            url = server.url(codec)
            assert url.startswith("http://127.0.0.1:")
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    assert response.status == 200
                    assert response.headers["Content-Type"] == "video/mp2t"
                    data = await response.read()
                bad = url.replace(server._token, "wrong")
                async with session.get(bad) as response:
                    assert response.status == 404
                async with session.get(url.replace(f"{codec}.ts", "vp9.ts")) as response:
                    assert response.status == 404
        finally:
            await server.async_stop()

        assert reader.closed
        with av.open(io.BytesIO(data), format="mpegts") as container:
            video = container.streams.video[0]
            assert video.codec_context.name == expected
            assert len(list(container.decode(video))) == FIXTURE_PICTURES
        assert server.url(codec) is None
