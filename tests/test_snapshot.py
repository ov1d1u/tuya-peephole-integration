"""Tests for stream snapshots (snapshot.py), shared MQTT topic callbacks, and
the coordinator's snapshot storage.

Requirements covered: CTRL-02 (snapshot)
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from custom_components.tuya_peephole.live_stream import StreamClosedError

from .test_hevc_stream import FIXTURE_PICTURES, _depacketize, _packets


class _FakeReader:
    """StreamReader stand-in: yields access units, then hangs or ends."""

    def __init__(self, units, end: bool = False) -> None:
        self._units = list(units)
        self._end = end
        self.closed = False

    async def get(self):
        if self._units:
            return self._units.pop(0)
        if self._end:
            raise StreamClosedError
        await asyncio.Event().wait()

    def close(self) -> None:
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        self.close()


# ---------------------------------------------------------------------------
# Reading a frame from the live stream
# ---------------------------------------------------------------------------


class TestReadFrame:
    """async_read_frame decodes until the picture has settled."""

    @pytest.mark.asyncio
    async def test_skips_unsettled_frames(self, mock_hass_with_loop) -> None:
        from custom_components.tuya_peephole.const import SNAPSHOT_SKIP_FRAMES
        from custom_components.tuya_peephole.snapshot import async_read_frame

        units = _depacketize(_packets())
        reader = _FakeReader(units)
        frame = await async_read_frame(mock_hass_with_loop, reader, 1, 1)

        assert frame is not None
        assert (frame.width, frame.height) == (160, 120)
        # consumed exactly the frames it needed
        assert len(reader._units) == FIXTURE_PICTURES - SNAPSHOT_SKIP_FRAMES - 1

    @pytest.mark.asyncio
    async def test_returns_newest_frame_on_timeout(self, mock_hass_with_loop) -> None:
        """Fewer frames than the skip count: the newest one is used."""
        from custom_components.tuya_peephole.snapshot import async_read_frame

        reader = _FakeReader(_depacketize(_packets())[:2])
        frame = await async_read_frame(mock_hass_with_loop, reader, 1, 0.05)
        assert frame is not None

    @pytest.mark.asyncio
    async def test_no_video(self, mock_hass_with_loop) -> None:
        from custom_components.tuya_peephole.snapshot import async_read_frame

        frame = await async_read_frame(mock_hass_with_loop, _FakeReader([]), 0.05, 1)
        assert frame is None

    @pytest.mark.asyncio
    async def test_stream_closed(self, mock_hass_with_loop) -> None:
        from custom_components.tuya_peephole.snapshot import async_read_frame

        reader = _FakeReader([], end=True)
        assert await async_read_frame(mock_hass_with_loop, reader, 1, 1) is None


# ---------------------------------------------------------------------------
# async_capture_snapshot orchestration
# ---------------------------------------------------------------------------


class TestAsyncCaptureSnapshot:
    """Capture flow with the live stream mocked."""

    @pytest.mark.asyncio
    async def test_capture_success(self, mock_recording_coordinator) -> None:
        """Reads the shared live stream, returns a JPEG, closes the reader."""
        from custom_components.tuya_peephole import snapshot

        coord = mock_recording_coordinator
        reader = _FakeReader(_depacketize(_packets()))
        coord.live_stream.open = MagicMock(return_value=reader)

        image = await snapshot.async_capture_snapshot(coord)

        assert image is not None and image.startswith(b"\xff\xd8")
        assert reader.closed

    @pytest.mark.asyncio
    async def test_capture_no_frame(self, mock_recording_coordinator) -> None:
        """No video in time -> None, reader closed."""
        from custom_components.tuya_peephole import snapshot

        coord = mock_recording_coordinator
        reader = _FakeReader([])
        coord.live_stream.open = MagicMock(return_value=reader)
        with patch.object(snapshot, "CONNECT_TIMEOUT", 0.05):
            image = await snapshot.async_capture_snapshot(coord)

        assert image is None
        assert reader.closed

    @pytest.mark.asyncio
    async def test_capture_after_shutdown(self, mock_recording_coordinator) -> None:
        from custom_components.tuya_peephole import snapshot

        coord = mock_recording_coordinator
        coord.live_stream.open = MagicMock(side_effect=StreamClosedError)
        assert await snapshot.async_capture_snapshot(coord) is None

    @pytest.mark.asyncio
    async def test_capture_without_aiortc(self, mock_recording_coordinator) -> None:
        """aiortc missing -> None without opening the stream."""
        from custom_components.tuya_peephole import snapshot

        coord = mock_recording_coordinator
        with patch.object(snapshot, "AIORTC_AVAILABLE", False):
            image = await snapshot.async_capture_snapshot(coord)

        assert image is None
        coord.live_stream.open.assert_not_called()


# ---------------------------------------------------------------------------
# Coordinator snapshot storage
# ---------------------------------------------------------------------------


class TestCoordinatorSnapshot:
    """TuyaPeepholeCoordinator.async_capture_snapshot."""

    def _make_coordinator(self, hass):
        from custom_components.tuya_peephole.coordinator import (
            TuyaPeepholeCoordinator,
        )

        coordinator = TuyaPeepholeCoordinator(
            hass, MagicMock(), "test_device_id_abc123", "testkey123"
        )
        coordinator.async_set_updated_data = MagicMock()
        return coordinator

    @pytest.mark.asyncio
    async def test_stores_snapshot_and_notifies(self, mock_hass_with_loop) -> None:
        """A successful capture is stored and pushed to listeners."""
        coordinator = self._make_coordinator(mock_hass_with_loop)

        with patch(
            "custom_components.tuya_peephole.snapshot.async_capture_snapshot",
            AsyncMock(return_value=b"jpeg"),
        ):
            image = await coordinator.async_capture_snapshot()

        assert image == b"jpeg"
        assert coordinator.last_snapshot == b"jpeg"
        assert coordinator.last_snapshot_time is not None
        coordinator.async_set_updated_data.assert_called_once()

    @pytest.mark.asyncio
    async def test_failed_capture_keeps_previous(self, mock_hass_with_loop) -> None:
        """A failed capture returns None and keeps the previous image."""
        coordinator = self._make_coordinator(mock_hass_with_loop)
        coordinator.last_snapshot = b"old"

        with patch(
            "custom_components.tuya_peephole.snapshot.async_capture_snapshot",
            AsyncMock(return_value=None),
        ):
            image = await coordinator.async_capture_snapshot()

        assert image is None
        assert coordinator.last_snapshot == b"old"
        coordinator.async_set_updated_data.assert_not_called()

    @pytest.mark.asyncio
    async def test_concurrent_presses_share_one_capture(
        self, mock_hass_with_loop
    ) -> None:
        """A second press during a capture waits for it instead of opening a second stream."""
        coordinator = self._make_coordinator(mock_hass_with_loop)
        release = asyncio.Event()

        async def slow_capture(_coord):
            await release.wait()
            return b"jpeg"

        capture = AsyncMock(side_effect=slow_capture)
        with patch(
            "custom_components.tuya_peephole.snapshot.async_capture_snapshot",
            capture,
        ):
            first = asyncio.ensure_future(coordinator.async_capture_snapshot())
            await asyncio.sleep(0)
            second = asyncio.ensure_future(coordinator.async_capture_snapshot())
            await asyncio.sleep(0)
            release.set()
            results = await asyncio.gather(first, second)

        assert results == [b"jpeg", b"jpeg"]
        assert capture.await_count == 1


# ---------------------------------------------------------------------------
# Shared signaling topic in TuyaMQTTClient
# ---------------------------------------------------------------------------


class TestMQTTSharedTopicCallbacks:
    """Several WebRTC sessions share /av/u/{msid}; each keeps its callback."""

    def _make_client(self, hass):
        from custom_components.tuya_peephole.mqtt import TuyaMQTTClient

        client = TuyaMQTTClient(hass)
        client._client = MagicMock()
        return client

    def test_dispatches_to_all_callbacks(self, mock_hass_with_loop) -> None:
        """Both registered callbacks receive a message on the topic."""
        client = self._make_client(mock_hass_with_loop)
        cb_live, cb_snap = MagicMock(), MagicMock()

        client.message_callback_add("/av/u/msid", cb_live)
        client.message_callback_add("/av/u/msid", cb_snap)

        # paho gets a single dispatcher for the topic
        client._client.message_callback_add.assert_called_once()
        dispatch = client._client.message_callback_add.call_args[0][1]
        msg = MagicMock()
        dispatch(None, None, msg)

        cb_live.assert_called_once_with(None, None, msg)
        cb_snap.assert_called_once_with(None, None, msg)

    def test_removing_one_keeps_other_and_subscription(
        self, mock_hass_with_loop
    ) -> None:
        """Ending the snapshot session leaves live view's callback and subscription intact."""
        client = self._make_client(mock_hass_with_loop)
        cb_live, cb_snap = MagicMock(), MagicMock()
        client.message_callback_add("/av/u/msid", cb_live)
        client.message_callback_add("/av/u/msid", cb_snap)

        client.message_callback_remove("/av/u/msid", cb_snap)
        client.unsubscribe("/av/u/msid")

        client._client.message_callback_remove.assert_not_called()
        client._client.unsubscribe.assert_not_called()
        dispatch = client._client.message_callback_add.call_args[0][1]
        dispatch(None, None, MagicMock())
        cb_live.assert_called_once()
        cb_snap.assert_not_called()

    def test_removing_last_unsubscribes(self, mock_hass_with_loop) -> None:
        """Once no callbacks remain, paho's callback and the subscription are removed."""
        client = self._make_client(mock_hass_with_loop)
        cb = MagicMock()
        client.message_callback_add("/av/u/msid", cb)

        client.message_callback_remove("/av/u/msid", cb)
        client.unsubscribe("/av/u/msid")

        client._client.message_callback_remove.assert_called_once_with("/av/u/msid")
        client._client.unsubscribe.assert_called_once_with("/av/u/msid")

    def test_callback_error_does_not_block_others(self, mock_hass_with_loop) -> None:
        """An exception in one session's callback does not stop the next."""
        client = self._make_client(mock_hass_with_loop)
        bad = MagicMock(side_effect=RuntimeError("boom"))
        good = MagicMock()
        client.message_callback_add("/av/u/msid", bad)
        client.message_callback_add("/av/u/msid", good)

        dispatch = client._client.message_callback_add.call_args[0][1]
        dispatch(None, None, MagicMock())

        good.assert_called_once()


# ---------------------------------------------------------------------------
# ICE server normalization for aiortc
# ---------------------------------------------------------------------------


class TestIceServerNormalization:
    """Tuya's p2pConfig.ices adapted to what aiortc accepts."""

    @pytest.mark.parametrize(
        ("uri", "expected"),
        [
            ("stun:203.0.113.5:3478", "stun:203.0.113.5:3478"),
            ("stun:stun.example.com", "stun:stun.example.com"),
            # aiortc ignores TURN without a transport -> default it
            ("turn:203.0.113.5:3478", "turn:203.0.113.5:3478?transport=udp"),
            ("turns:relay.example.com:443", "turns:relay.example.com:443?transport=tcp"),
            ("turn:203.0.113.5:3478?transport=tcp", "turn:203.0.113.5:3478?transport=tcp"),
            # aiortc raises "malformed uri" on IPv6 -> drop it
            ("stun:[2a05:d014:afa:af10:9a8a:756e:9087:735e]:3478", None),
            ("turn:[2001:db8::1]:3478", None),
            ("http://example.com", None),
        ],
    )
    def test_normalize_ice_uri(self, uri, expected) -> None:
        from custom_components.tuya_peephole.webrtc_session import normalize_ice_uri

        assert normalize_ice_uri(uri) == expected

    def test_build_servers_from_tuya_config(self) -> None:
        """The shape the camera returns: IPv4 STUN, IPv6 STUN, TURN with credentials."""
        from types import SimpleNamespace

        from custom_components.tuya_peephole import webrtc_session

        ices = [
            {"urls": "stun:203.0.113.5:3478"},
            {"urls": "stun:[2a05:d014:afa:af10:9a8a:756e:9087:735e]:3478"},
            {
                "urls": "turn:203.0.113.5:3478",
                "username": "user",
                "credential": "pass",
                "ttl": 600,
            },
        ]
        with patch.object(
            webrtc_session, "RTCIceServer", side_effect=lambda **kw: SimpleNamespace(**kw)
        ):
            servers = webrtc_session.build_aiortc_ice_servers(ices)

        assert [s.urls for s in servers] == [
            "stun:203.0.113.5:3478",
            "turn:203.0.113.5:3478?transport=udp",
        ]
        assert servers[1].username == "user"
        assert servers[1].credential == "pass"

    def test_build_servers_keeps_list_urls(self) -> None:
        """A server with a urls list keeps its usable entries as a list."""
        from types import SimpleNamespace

        from custom_components.tuya_peephole import webrtc_session

        ices = [{"urls": ["turn:203.0.113.5:3478", "turn:[2001:db8::1]:3478", "turns:r.example.com:443"]}]
        with patch.object(
            webrtc_session, "RTCIceServer", side_effect=lambda **kw: SimpleNamespace(**kw)
        ):
            servers = webrtc_session.build_aiortc_ice_servers(ices)

        assert servers[0].urls == [
            "turn:203.0.113.5:3478?transport=udp",
            "turns:r.example.com:443?transport=tcp",
        ]


# ---------------------------------------------------------------------------
# Signaling identity
# ---------------------------------------------------------------------------


class TestSessionSignalingMsid:
    """The session must signal with the msid the MQTT connection logged in with."""

    @pytest.mark.asyncio
    async def test_uses_connection_msid_not_fresh_one(
        self, mock_recording_coordinator
    ) -> None:
        """Every /api/jarvis/mqtt call mints a new msid; the broker only
        delivers /av/u/{msid} to client web_{msid}, so a fresh msid would
        never receive the camera's answer."""
        from unittest.mock import AsyncMock, MagicMock, patch

        from custom_components.tuya_peephole import webrtc_session

        coord = mock_recording_coordinator
        coord.api.async_get_mqtt_config = AsyncMock(
            return_value={"msid": "freshly_minted_msid", "password": "x"}
        )
        coord.mqtt_client = MagicMock(is_connected=True)

        pc = MagicMock()
        pc.createOffer = AsyncMock(return_value=MagicMock(sdp="v=0\r\n"))
        pc.setLocalDescription = AsyncMock()
        pc.localDescription.sdp = "v=0\r\n"
        pc.close = AsyncMock()

        session = webrtc_session.ServerWebRTCSession(coord)
        with (
            patch.object(webrtc_session, "AIORTC_AVAILABLE", True),
            patch.object(webrtc_session, "RTCPeerConnection", return_value=pc, create=True),
            patch.object(webrtc_session, "RTCConfiguration", create=True),
            patch.object(webrtc_session, "WEBRTC_SESSION_TIMEOUT", 0.01),
        ):
            started = await session.async_start()

        assert started is False  # no answer in the test
        coord.mqtt_client.subscribe.assert_any_call("/av/u/test_msid_789", qos=1)
        offer = coord.mqtt_client.publish.call_args_list[0].args[1]
        if isinstance(offer, bytes):
            offer = offer.decode()
        assert "test_msid_789" in offer
        assert "freshly_minted_msid" not in offer
        coord.api.async_get_mqtt_config.assert_not_called()
