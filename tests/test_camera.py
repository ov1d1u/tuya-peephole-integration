"""Tests for TuyaPeepholeCamera entity.

Tests entity properties, MQTT-aware availability, the stored snapshot,
and the live stream source (local restream URL).

Requirements covered: STRM-01, STRM-04, REL-04
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

# ---------------------------------------------------------------------------
# Helper: create a mock coordinator specifically for camera tests
# ---------------------------------------------------------------------------


def _make_mock_coordinator(
    device_id="test_device_123",
    msid="abc123def456",
    moto_id="moto_cnpre002",
    auth="U+qtvRP+testauth",
    p2p_ices=None,
    camera_state=None,
    mqtt_connected=True,
):
    """Create a mock coordinator for camera entity tests.

    Args:
        device_id: Device ID for topic routing.
        msid: MQTT session ID.
        moto_id: Moto ID from jarvis/config.
        auth: Auth token for WebRTC offer.
        p2p_ices: ICE server list (default: STUN + TURN).
        camera_state: Camera state (default: AWAKE).
        mqtt_connected: Whether MQTT client reports connected.

    Returns:
        MagicMock coordinator with all expected attributes.
    """
    from custom_components.tuya_peephole.models import CameraState

    if camera_state is None:
        camera_state = CameraState.AWAKE
    if p2p_ices is None:
        p2p_ices = [
            {"urls": "stun:172.81.239.63:3478"},
            {
                "urls": "turn:172.81.239.63:3478",
                "username": "testuser",
                "credential": "testcred",
            },
        ]

    coordinator = MagicMock()
    coordinator.device_id = device_id
    coordinator.camera_state = camera_state
    coordinator.async_wake_camera = AsyncMock(return_value=True)
    # msid the live MQTT connection logged in with (web_{msid})
    coordinator.msid = msid

    # Mock MQTT client
    mqtt_client = MagicMock()
    mqtt_client.is_connected = mqtt_connected
    mqtt_client.subscribe = MagicMock()
    mqtt_client.publish = MagicMock()
    mqtt_client._client = MagicMock()
    mqtt_client._client.message_callback_add = MagicMock()
    mqtt_client._client.message_callback_remove = MagicMock()
    mqtt_client._client.unsubscribe = MagicMock()
    coordinator.mqtt_client = mqtt_client

    # Mock API
    api = AsyncMock()
    api.async_get_webrtc_config = AsyncMock(
        return_value={
            "motoId": moto_id,
            "auth": auth,
            "p2pConfig": {"ices": p2p_ices},
        }
    )
    # Tuya mints a new msid on every /api/jarvis/mqtt call; it must never
    # be used for signaling (the broker won't deliver its /av/u/ topic).
    api.async_get_mqtt_config = AsyncMock(
        return_value={
            "msid": "freshly_minted_msid",
            "password": "testpw",
        }
    )
    coordinator.api = api

    return coordinator


def _make_camera(coordinator=None, options=None):
    """Create a TuyaPeepholeCamera instance with the given coordinator."""
    from custom_components.tuya_peephole.camera import TuyaPeepholeCamera

    if coordinator is None:
        coordinator = _make_mock_coordinator()
    entry = MagicMock()
    entry.options = options or {}
    return TuyaPeepholeCamera(coordinator, entry)


# ---------------------------------------------------------------------------
# Entity properties tests
# ---------------------------------------------------------------------------


class TestCameraEntityProperties:
    """Tests for TuyaPeepholeCamera entity attributes."""

    def test_entity_unique_id(self) -> None:
        """unique_id is {device_id}_camera."""
        camera = _make_camera()
        assert camera._attr_unique_id == "test_device_123_camera"

    def test_entity_supported_features(self) -> None:
        """CameraEntityFeature.STREAM is set."""
        from homeassistant.components.camera import CameraEntityFeature

        camera = _make_camera()
        assert camera._attr_supported_features == CameraEntityFeature.STREAM

    def test_entity_has_entity_name(self) -> None:
        """_attr_has_entity_name is True."""
        camera = _make_camera()
        assert camera._attr_has_entity_name is True

    def test_entity_name(self) -> None:
        """_attr_name is 'Camera'."""
        camera = _make_camera()
        assert camera._attr_name == "Camera"

    def test_entity_device_info(self) -> None:
        """Device identifiers contain (DOMAIN, device_id)."""
        from custom_components.tuya_peephole.const import DOMAIN

        camera = _make_camera()
        info = camera._attr_device_info
        assert (DOMAIN, "test_device_123") in info.identifiers


# ---------------------------------------------------------------------------
# Availability tests
# ---------------------------------------------------------------------------


class TestCameraAvailability:
    """Tests for MQTT-aware camera availability."""

    def test_available_when_mqtt_connected(self) -> None:
        """available is True when mqtt_client.is_connected is True."""
        coordinator = _make_mock_coordinator(mqtt_connected=True)
        camera = _make_camera(coordinator)
        assert camera.available is True

    def test_unavailable_when_mqtt_disconnected(self) -> None:
        """available is False when mqtt_client.is_connected is False."""
        coordinator = _make_mock_coordinator(mqtt_connected=False)
        camera = _make_camera(coordinator)
        assert camera.available is False

    def test_unavailable_when_no_mqtt_client(self) -> None:
        """available is False when mqtt_client is None."""
        coordinator = _make_mock_coordinator()
        coordinator.mqtt_client = None
        camera = _make_camera(coordinator)
        assert camera.available is False


# ---------------------------------------------------------------------------
# Snapshot placeholder test (STRM-04)
# ---------------------------------------------------------------------------


class TestCameraSnapshot:
    """Tests for async_camera_image (serves the stored snapshot)."""

    @pytest.mark.asyncio
    async def test_async_camera_image_returns_none_without_snapshot(self) -> None:
        """No snapshot captured yet -> None, and nothing is woken or fetched."""
        coordinator = _make_mock_coordinator()
        coordinator.last_snapshot = None
        camera = _make_camera(coordinator)
        result = await camera.async_camera_image()
        assert result is None
        coordinator.async_wake_camera.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_async_camera_image_returns_stored_snapshot(self) -> None:
        """Returns the coordinator's last snapshot without opening a stream."""
        coordinator = _make_mock_coordinator()
        coordinator.last_snapshot = b"\xff\xd8jpeg"
        coordinator.async_capture_snapshot = AsyncMock()
        camera = _make_camera(coordinator)
        result = await camera.async_camera_image(width=640, height=480)
        assert result == b"\xff\xd8jpeg"
        coordinator.async_capture_snapshot.assert_not_awaited()
        coordinator.async_wake_camera.assert_not_awaited()


# ---------------------------------------------------------------------------
# Live stream source
# ---------------------------------------------------------------------------


class TestStreamSource:
    """stream_source() points Home Assistant at the local restream."""

    def _restream(self):
        from custom_components.tuya_peephole.restream import RestreamServer

        server = RestreamServer(MagicMock(), MagicMock())
        server._port = 40000
        return server

    @pytest.mark.asyncio
    async def test_defaults_to_h264(self) -> None:
        coordinator = _make_mock_coordinator()
        coordinator.restream = self._restream()
        url = await _make_camera(coordinator).stream_source()
        assert url.startswith("http://127.0.0.1:40000/")
        assert url.endswith("/h264.ts")

    @pytest.mark.asyncio
    async def test_hevc_option(self) -> None:
        coordinator = _make_mock_coordinator()
        coordinator.restream = self._restream()
        camera = _make_camera(coordinator, options={"live_codec": "hevc"})
        assert (await camera.stream_source()).endswith("/hevc.ts")

    @pytest.mark.asyncio
    async def test_no_restream_yet(self) -> None:
        coordinator = _make_mock_coordinator()
        coordinator.restream = None
        assert await _make_camera(coordinator).stream_source() is None

    @pytest.mark.asyncio
    async def test_does_not_wake_camera(self) -> None:
        """Only reading the URL connects the camera, not asking for it."""
        coordinator = _make_mock_coordinator()
        coordinator.restream = self._restream()
        await _make_camera(coordinator).stream_source()
        coordinator.async_wake_camera.assert_not_awaited()

    def test_no_native_webrtc(self) -> None:
        """Native WebRTC would bypass the restream (and send the browser
        H.265 labelled as H.264)."""
        from custom_components.tuya_peephole.camera import TuyaPeepholeCamera

        assert "async_handle_async_webrtc_offer" not in vars(TuyaPeepholeCamera)
