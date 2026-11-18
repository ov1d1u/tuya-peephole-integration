"""Tests for stream snapshots (snapshot.py), shared MQTT topic callbacks, and
the coordinator's snapshot storage.

Requirements covered: CTRL-02 (snapshot)
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class _FakeTrack:
    """Video track that yields the given frames, then ends."""

    kind = "video"

    def __init__(self, frames, end_exc=Exception("track ended")):
        self._frames = list(frames)
        self._end_exc = end_exc

    async def recv(self):
        if self._frames:
            return self._frames.pop(0)
        raise self._end_exc


class _HangingTrack:
    """Video track that yields frames, then waits forever."""

    kind = "video"

    def __init__(self, frames):
        self._frames = list(frames)

    async def recv(self):
        if self._frames:
            return self._frames.pop(0)
        await asyncio.Event().wait()


# ---------------------------------------------------------------------------
# SnapshotSession frame handling
# ---------------------------------------------------------------------------


class TestSnapshotSessionFrames:
    """Frame selection inside SnapshotSession."""

    @pytest.mark.asyncio
    async def test_skips_settling_frames(self, mock_recording_coordinator) -> None:
        """Returns the first frame after SNAPSHOT_SKIP_FRAMES frames."""
        from custom_components.tuya_peephole.const import SNAPSHOT_SKIP_FRAMES
        from custom_components.tuya_peephole.snapshot import SnapshotSession

        session = SnapshotSession(mock_recording_coordinator)
        frames = [f"frame{i}" for i in range(SNAPSHOT_SKIP_FRAMES + 3)]
        await session._async_on_video_track(_HangingTrack(frames))

        frame = await session.async_wait_frame(timeout=1)

        assert frame == f"frame{SNAPSHOT_SKIP_FRAMES}"
        await session._async_close_media()

    @pytest.mark.asyncio
    async def test_short_stream_returns_last_frame(
        self, mock_recording_coordinator
    ) -> None:
        """If the track ends before settling, the newest frame is used."""
        from custom_components.tuya_peephole.snapshot import SnapshotSession

        session = SnapshotSession(mock_recording_coordinator)
        await session._async_on_video_track(_FakeTrack(["a", "b"]))

        frame = await session.async_wait_frame(timeout=1)

        assert frame == "b"

    @pytest.mark.asyncio
    async def test_timeout_returns_newest_frame(
        self, mock_recording_coordinator
    ) -> None:
        """On timeout, returns whatever frame arrived (stream still open)."""
        from custom_components.tuya_peephole.snapshot import SnapshotSession

        session = SnapshotSession(mock_recording_coordinator)
        await session._async_on_video_track(_HangingTrack(["only"]))

        frame = await session.async_wait_frame(timeout=0.05)

        assert frame == "only"
        await session._async_close_media()
        assert session._reader_task.done()

    @pytest.mark.asyncio
    async def test_no_frames_returns_none(self, mock_recording_coordinator) -> None:
        """No frame at all -> None after the timeout."""
        from custom_components.tuya_peephole.snapshot import SnapshotSession

        session = SnapshotSession(mock_recording_coordinator)
        await session._async_on_video_track(_HangingTrack([]))

        frame = await session.async_wait_frame(timeout=0.05)

        assert frame is None
        await session._async_close_media()


class TestFrameToJpeg:
    """JPEG encoding of a decoded frame."""

    def test_frame_to_jpeg_encodes_image(self) -> None:
        """frame_to_jpeg returns JPEG bytes from frame.to_image()."""
        PIL_Image = pytest.importorskip("PIL.Image")
        from custom_components.tuya_peephole.snapshot import frame_to_jpeg

        frame = MagicMock()
        frame.to_image.return_value = PIL_Image.new("RGB", (32, 24), "red")

        data = frame_to_jpeg(frame)

        assert data[:2] == b"\xff\xd8"  # JPEG SOI marker


# ---------------------------------------------------------------------------
# async_capture_snapshot orchestration
# ---------------------------------------------------------------------------


class TestAsyncCaptureSnapshot:
    """End-to-end capture flow with the session mocked."""

    def _patch_session(self, started=True, frame="frame"):
        session = MagicMock()
        session.async_start = AsyncMock(return_value=started)
        session.async_wait_frame = AsyncMock(return_value=frame)
        session.async_stop = AsyncMock()
        return session, patch(
            "custom_components.tuya_peephole.snapshot.SnapshotSession",
            return_value=session,
        )

    @pytest.mark.asyncio
    async def test_capture_success(self, mock_recording_coordinator) -> None:
        """Wakes with force, refreshes config, encodes the frame, stops session."""
        from custom_components.tuya_peephole import snapshot

        coord = mock_recording_coordinator
        frame = MagicMock(width=640, height=480)
        session, patcher = self._patch_session(frame=frame)

        with patcher, patch.object(
            snapshot, "frame_to_jpeg", return_value=b"\xff\xd8jpeg"
        ):
            image = await snapshot.async_capture_snapshot(coord)

        assert image == b"\xff\xd8jpeg"
        coord.async_wake_camera.assert_awaited_once_with(force=True)
        coord.api.invalidate_cache.assert_called_once()
        session.async_stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_capture_session_fails_to_start(
        self, mock_recording_coordinator
    ) -> None:
        """No stream -> None, session still stopped."""
        from custom_components.tuya_peephole import snapshot

        session, patcher = self._patch_session(started=False)
        with patcher:
            image = await snapshot.async_capture_snapshot(
                mock_recording_coordinator
            )

        assert image is None
        session.async_wait_frame.assert_not_awaited()
        session.async_stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_capture_no_frame(self, mock_recording_coordinator) -> None:
        """Stream opens but no frame arrives -> None."""
        from custom_components.tuya_peephole import snapshot

        session, patcher = self._patch_session(frame=None)
        with patcher:
            image = await snapshot.async_capture_snapshot(
                mock_recording_coordinator
            )

        assert image is None
        session.async_stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_capture_without_aiortc(self, mock_recording_coordinator) -> None:
        """aiortc missing -> None without waking the camera."""
        from custom_components.tuya_peephole import snapshot

        with patch.object(snapshot, "AIORTC_AVAILABLE", False):
            image = await snapshot.async_capture_snapshot(
                mock_recording_coordinator
            )

        assert image is None
        mock_recording_coordinator.async_wake_camera.assert_not_awaited()


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

    @pytest.mark.asyncio
    async def test_waiter_gets_none_when_shared_capture_fails(
        self, mock_hass_with_loop
    ) -> None:
        """A press waiting on a failed capture gets None, not the stale stored image."""
        coordinator = self._make_coordinator(mock_hass_with_loop)
        coordinator.last_snapshot = b"old"
        release = asyncio.Event()

        async def failing_capture(_coord):
            await release.wait()
            return None

        capture = AsyncMock(side_effect=failing_capture)
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

        assert results == [None, None]
        assert capture.await_count == 1
        assert coordinator.last_snapshot == b"old"

    @pytest.mark.asyncio
    async def test_waiter_gets_none_when_shared_capture_raises(
        self, mock_hass_with_loop
    ) -> None:
        """An exception in the capture releases waiters with None and allows a retry."""
        coordinator = self._make_coordinator(mock_hass_with_loop)
        release = asyncio.Event()

        async def raising_capture(_coord):
            await release.wait()
            raise RuntimeError("boom")

        with patch(
            "custom_components.tuya_peephole.snapshot.async_capture_snapshot",
            AsyncMock(side_effect=raising_capture),
        ):
            first = asyncio.ensure_future(coordinator.async_capture_snapshot())
            await asyncio.sleep(0)
            second = asyncio.ensure_future(coordinator.async_capture_snapshot())
            await asyncio.sleep(0)
            release.set()
            with pytest.raises(RuntimeError):
                await first
            assert await second is None

        with patch(
            "custom_components.tuya_peephole.snapshot.async_capture_snapshot",
            AsyncMock(return_value=b"jpeg"),
        ):
            assert await coordinator.async_capture_snapshot() == b"jpeg"


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
        from custom_components.tuya_peephole.snapshot import SnapshotSession

        coord = mock_recording_coordinator
        coord.api.async_get_mqtt_config = AsyncMock(
            return_value={"msid": "freshly_minted_msid", "password": "x"}
        )
        coord.mqtt_client = MagicMock(is_connected=True)

        pc = MagicMock()
        pc.createOffer = AsyncMock(return_value=MagicMock(sdp="v=0\r\n"))
        pc.setLocalDescription = AsyncMock()
        pc.close = AsyncMock()

        session = SnapshotSession(coord)
        session._setup_media = MagicMock()
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
