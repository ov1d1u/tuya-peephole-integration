"""Tests for TuyaPeepholeCoordinator (custom_components/tuya_peephole/coordinator.py).

Tests camera wake flow, motion detection, state transitions,
disconnect handling, and teardown.

Requirements covered: MQTT-02 (wake), MQTT-03 (message parse),
MQTT-04 (reconnect), REL-01 (sleeping state)
"""

from __future__ import annotations

import asyncio
import struct
import zlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _make_coordinator(hass, mock_api=None, device_id="test_device_id_abc123",
                      local_key="testkey123"):
    """Create a TuyaPeepholeCoordinator with mocked dependencies."""
    from custom_components.tuya_peephole.coordinator import TuyaPeepholeCoordinator

    api = mock_api or MagicMock()
    coordinator = TuyaPeepholeCoordinator(hass, api, device_id, local_key)
    return coordinator


def _make_mock_mqtt_client(is_connected=True):
    """Create a mock MQTT client."""
    client = MagicMock()
    client.async_connect = AsyncMock()
    client.subscribe = MagicMock()
    client.publish = MagicMock()
    client.async_disconnect = AsyncMock()
    client.is_connected = is_connected
    client.set_message_callback = MagicMock()
    client.set_on_connected_callback = MagicMock()
    client.set_on_disconnected_callback = MagicMock()
    return client


class TestCoordinatorInitialState:
    """Test coordinator initial state."""

    def test_coordinator_initial_state_sleeping(
        self, mock_hass_with_loop
    ) -> None:
        """[REL-01] New coordinator has camera_state=SLEEPING and motion=False."""
        from custom_components.tuya_peephole.models import CameraState

        coordinator = _make_coordinator(mock_hass_with_loop)

        assert coordinator.camera_state == CameraState.SLEEPING
        assert coordinator._motion_detected is False

    def test_coordinator_data_shape(
        self, mock_hass_with_loop
    ) -> None:
        """Coordinator _async_update_data returns expected data shape."""
        coordinator = _make_coordinator(mock_hass_with_loop)

        # Manually call _async_update_data (normally called by DataUpdateCoordinator)
        loop = asyncio.get_event_loop()
        data = loop.run_until_complete(coordinator._async_update_data())

        assert "camera_state" in data
        assert "motion_detected" in data

    def test_coordinator_no_update_interval(
        self, mock_hass_with_loop
    ) -> None:
        """Coordinator is push-only (no update_interval set)."""
        coordinator = _make_coordinator(mock_hass_with_loop)

        assert coordinator.update_interval is None


class TestCoordinatorWake:
    """Test camera wake flow."""

    @pytest.mark.asyncio
    async def test_wake_publishes_crc32_payload(
        self, mock_hass_with_loop
    ) -> None:
        """[MQTT-02] Wake publishes CRC32 of local_key as big-endian 4 bytes to m/w/{device_id}."""
        from custom_components.tuya_peephole.models import CameraState

        coordinator = _make_coordinator(
            mock_hass_with_loop, local_key="testkey123"
        )
        mock_mqtt = _make_mock_mqtt_client()
        coordinator.mqtt_client = mock_mqtt

        # Known CRC32 test vector
        expected_crc = zlib.crc32(b"testkey123") & 0xFFFFFFFF
        expected_payload = struct.pack(">I", expected_crc)
        assert expected_crc == 0x62FBB946

        # Simulate awake response after publish
        async def _simulate_awake():
            await asyncio.sleep(0.01)
            coordinator._awake_event.set()

        task = asyncio.create_task(_simulate_awake())

        with patch(
            "custom_components.tuya_peephole.coordinator.async_call_later",
            return_value=MagicMock(),
        ):
            result = await coordinator.async_wake_camera()

        await task

        assert result is True
        mock_mqtt.publish.assert_called_once_with(
            "m/w/test_device_id_abc123", expected_payload, qos=1
        )

    @pytest.mark.asyncio
    async def test_wake_transitions_to_waking(
        self, mock_hass_with_loop
    ) -> None:
        """[MQTT-02] After wake starts, camera_state transitions to WAKING."""
        from custom_components.tuya_peephole.models import CameraState

        coordinator = _make_coordinator(mock_hass_with_loop)
        mock_mqtt = _make_mock_mqtt_client()
        coordinator.mqtt_client = mock_mqtt

        # Capture state during the wake call by not resolving the awake event
        states_seen = []
        original_publish = mock_mqtt.publish

        def capture_state(*args, **kwargs):
            states_seen.append(coordinator.camera_state)
            original_publish(*args, **kwargs)
            # Set awake event to unblock
            coordinator._awake_event.set()

        mock_mqtt.publish = capture_state

        with patch(
            "custom_components.tuya_peephole.coordinator.async_call_later",
            return_value=MagicMock(),
        ):
            await coordinator.async_wake_camera()

        # At publish time, state should have been WAKING
        assert CameraState.WAKING in states_seen

    @pytest.mark.asyncio
    async def test_wake_returns_true_on_awake_event(
        self, mock_hass_with_loop
    ) -> None:
        """[MQTT-02] Wake returns True when wireless_awake received within timeout."""
        from custom_components.tuya_peephole.models import CameraState

        coordinator = _make_coordinator(mock_hass_with_loop)
        mock_mqtt = _make_mock_mqtt_client()
        coordinator.mqtt_client = mock_mqtt

        # Simulate awake response
        async def _simulate_awake():
            await asyncio.sleep(0.01)
            coordinator._camera_state = CameraState.AWAKE
            coordinator._awake_event.set()

        task = asyncio.create_task(_simulate_awake())

        with patch(
            "custom_components.tuya_peephole.coordinator.async_call_later",
            return_value=MagicMock(),
        ):
            result = await coordinator.async_wake_camera()

        await task

        assert result is True
        assert coordinator.camera_state == CameraState.AWAKE

    @pytest.mark.asyncio
    async def test_wake_returns_false_on_timeout(
        self, mock_hass_with_loop
    ) -> None:
        """[MQTT-02] Wake returns False on timeout, state reverts to SLEEPING."""
        from custom_components.tuya_peephole.models import CameraState

        coordinator = _make_coordinator(mock_hass_with_loop)
        mock_mqtt = _make_mock_mqtt_client()
        coordinator.mqtt_client = mock_mqtt

        # Patch WAKE_TIMEOUT to very short for test speed
        with (
            patch(
                "custom_components.tuya_peephole.coordinator.WAKE_TIMEOUT", 0.01
            ),
            patch(
                "custom_components.tuya_peephole.coordinator.async_call_later",
                return_value=MagicMock(),
            ),
        ):
            result = await coordinator.async_wake_camera()

        assert result is False
        assert coordinator.camera_state == CameraState.SLEEPING

    @pytest.mark.asyncio
    async def test_wake_cooldown_prevents_duplicate(
        self, mock_hass_with_loop
    ) -> None:
        """[MQTT-02] Second wake within cooldown returns without publishing again."""
        from custom_components.tuya_peephole.models import CameraState

        coordinator = _make_coordinator(mock_hass_with_loop)
        mock_mqtt = _make_mock_mqtt_client()
        coordinator.mqtt_client = mock_mqtt

        # Simulate first wake succeeding
        async def _simulate_awake():
            await asyncio.sleep(0.01)
            coordinator._awake_event.set()

        task = asyncio.create_task(_simulate_awake())

        with patch(
            "custom_components.tuya_peephole.coordinator.async_call_later",
            return_value=MagicMock(),
        ):
            await coordinator.async_wake_camera()

        await task

        # Reset publish mock to check second call
        mock_mqtt.publish.reset_mock()

        # Second wake during cooldown
        result = await coordinator.async_wake_camera()

        # Should not publish again
        mock_mqtt.publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_wake_when_already_awake(
        self, mock_hass_with_loop
    ) -> None:
        """Wake returns True immediately when camera is already AWAKE."""
        from custom_components.tuya_peephole.models import CameraState

        coordinator = _make_coordinator(mock_hass_with_loop)
        coordinator._camera_state = CameraState.AWAKE
        mock_mqtt = _make_mock_mqtt_client()
        coordinator.mqtt_client = mock_mqtt

        result = await coordinator.async_wake_camera()

        assert result is True
        mock_mqtt.publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_wake_when_mqtt_disconnected(
        self, mock_hass_with_loop
    ) -> None:
        """Wake returns False when MQTT is not connected."""
        coordinator = _make_coordinator(mock_hass_with_loop)
        mock_mqtt = _make_mock_mqtt_client(is_connected=False)
        coordinator.mqtt_client = mock_mqtt

        result = await coordinator.async_wake_camera()

        assert result is False
        mock_mqtt.publish.assert_not_called()


class TestCoordinatorMotion:
    """Test motion detection flow."""

    def test_motion_detected_from_message(
        self, mock_hass_with_loop
    ) -> None:
        """[MQTT-03] Motion message sets motion_detected=True in coordinator data."""
        from custom_components.tuya_peephole.models import TuyaMQTTMessage

        coordinator = _make_coordinator(mock_hass_with_loop)

        with patch(
            "custom_components.tuya_peephole.coordinator.async_call_later",
            return_value=MagicMock(),
        ):
            message = TuyaMQTTMessage.parse(
                "smart/decrypt/in/test_device",
                b'{"data":{"pir":"1"}}',
            )
            coordinator._handle_mqtt_message(message)

        assert coordinator.data["motion_detected"] is True

    def test_motion_auto_clears_scheduled(
        self, mock_hass_with_loop
    ) -> None:
        """Motion event schedules auto-clear via async_call_later."""
        from custom_components.tuya_peephole.models import TuyaMQTTMessage
        from custom_components.tuya_peephole.const import MOTION_CLEAR_TIMEOUT

        coordinator = _make_coordinator(mock_hass_with_loop)

        with patch(
            "custom_components.tuya_peephole.coordinator.async_call_later",
            return_value=MagicMock(),
        ) as mock_call_later:
            message = TuyaMQTTMessage.parse(
                "smart/decrypt/in/test_device",
                b'{"data":{"pir":"1"}}',
            )
            coordinator._handle_mqtt_message(message)

        mock_call_later.assert_called_once_with(
            mock_hass_with_loop, MOTION_CLEAR_TIMEOUT, coordinator._async_clear_motion
        )

    def test_motion_clear_callback_resets_motion(
        self, mock_hass_with_loop
    ) -> None:
        """_async_clear_motion resets motion_detected to False."""
        coordinator = _make_coordinator(mock_hass_with_loop)
        coordinator._motion_detected = True

        coordinator._async_clear_motion(None)

        assert coordinator._motion_detected is False
        assert coordinator.data["motion_detected"] is False

    def test_wireless_awake_message_sets_awake(
        self, mock_hass_with_loop
    ) -> None:
        """[MQTT-03] wireless_awake message sets camera_state=AWAKE and triggers event."""
        from custom_components.tuya_peephole.models import CameraState, TuyaMQTTMessage

        coordinator = _make_coordinator(mock_hass_with_loop)

        message = TuyaMQTTMessage.parse(
            "smart/decrypt/in/test_device",
            b'{"data":{"wireless_awake":true}}',
        )
        coordinator._handle_mqtt_message(message)

        assert coordinator.camera_state == CameraState.AWAKE
        assert coordinator._awake_event.is_set()


class TestCoordinatorDisconnect:
    """Test MQTT disconnect handling."""

    def test_disconnect_resets_to_sleeping(
        self, mock_hass_with_loop
    ) -> None:
        """[REL-01] MQTT disconnect resets camera_state to SLEEPING and invalidates cache."""
        from custom_components.tuya_peephole.models import CameraState

        mock_api = MagicMock()
        coordinator = _make_coordinator(mock_hass_with_loop, mock_api=mock_api)
        coordinator._camera_state = CameraState.AWAKE

        coordinator._handle_mqtt_disconnect()

        assert coordinator.camera_state == CameraState.SLEEPING
        mock_api.invalidate_cache.assert_called_once()

    def test_disconnect_pushes_state_update(
        self, mock_hass_with_loop
    ) -> None:
        """Disconnect pushes updated data to entities."""
        from custom_components.tuya_peephole.models import CameraState

        coordinator = _make_coordinator(mock_hass_with_loop)
        coordinator._camera_state = CameraState.AWAKE

        coordinator._handle_mqtt_disconnect()

        assert coordinator.data["camera_state"] == CameraState.SLEEPING


class TestCoordinatorTeardown:
    """Test coordinator teardown."""

    @pytest.mark.asyncio
    async def test_teardown_disconnects_mqtt(
        self, mock_hass_with_loop
    ) -> None:
        """Teardown calls mqtt_client.async_disconnect."""
        coordinator = _make_coordinator(mock_hass_with_loop)
        mock_mqtt = _make_mock_mqtt_client()
        coordinator.mqtt_client = mock_mqtt

        await coordinator.async_teardown()

        mock_mqtt.async_disconnect.assert_awaited_once()
        assert coordinator.mqtt_client is None

    @pytest.mark.asyncio
    async def test_teardown_cancels_motion_timer(
        self, mock_hass_with_loop
    ) -> None:
        """Teardown cancels active motion clear timer."""
        coordinator = _make_coordinator(mock_hass_with_loop)
        coordinator.mqtt_client = _make_mock_mqtt_client()

        # Simulate active motion timer
        mock_unsub = MagicMock()
        coordinator._motion_clear_unsub = mock_unsub

        await coordinator.async_teardown()

        mock_unsub.assert_called_once()
        assert coordinator._motion_clear_unsub is None

    @pytest.mark.asyncio
    async def test_teardown_without_mqtt_client(
        self, mock_hass_with_loop
    ) -> None:
        """Teardown works even when mqtt_client is None."""
        coordinator = _make_coordinator(mock_hass_with_loop)
        coordinator.mqtt_client = None

        await coordinator.async_teardown()  # Should not raise


# Messages copied from a real camera log, in arrival order
_SLEEP_DPS = b'{"protocol":4,"t":1794742110,"data":{"dps":{"149":false}}}'
_SLEEP_OFFLINE = (
    b'{"data":{"devId":"test_device","online":false,'
    b'"lastOnlineChangeTime":1794742110388,"gwId":"test_device"},'
    b'"protocol":1,"t":1794742110,"type":"update"}'
)
_WAKE_DPS = b'{"protocol":4,"t":1794740288,"data":{"dps":{"149":true}}}'
_OTHER_DPS = b'{"protocol":4,"t":1794740288,"data":{"dps":{"145":90}}}'
_TOPIC = "smart/decrypt/in/test_device"


class TestSleepSignal:
    """The camera's go-to-sleep messages must never count as a wake."""

    @pytest.mark.parametrize("payload", [_SLEEP_DPS, _SLEEP_OFFLINE])
    def test_sleep_messages_are_not_wake(self, payload) -> None:
        from custom_components.tuya_peephole.models import TuyaMQTTMessage

        msg = TuyaMQTTMessage.parse(_TOPIC, payload)
        assert msg.is_sleep_signal is True
        assert msg.is_wireless_awake is False

    @pytest.mark.parametrize("payload", [_WAKE_DPS, _OTHER_DPS])
    def test_wake_messages(self, payload) -> None:
        from custom_components.tuya_peephole.models import TuyaMQTTMessage

        msg = TuyaMQTTMessage.parse(_TOPIC, payload)
        assert msg.is_sleep_signal is False
        assert msg.is_wireless_awake is True

    def test_sleep_message_sets_sleeping(self, mock_hass_with_loop) -> None:
        from custom_components.tuya_peephole.models import (
            CameraState,
            TuyaMQTTMessage,
        )

        coordinator = _make_coordinator(mock_hass_with_loop)
        coordinator._camera_state = CameraState.AWAKE
        coordinator._awake_event.set()

        coordinator._handle_mqtt_message(TuyaMQTTMessage.parse(_TOPIC, _SLEEP_DPS))

        assert coordinator.camera_state == CameraState.SLEEPING
        assert not coordinator._awake_event.is_set()

    @pytest.mark.asyncio
    async def test_wake_resent_when_camera_falls_asleep(
        self, mock_hass_with_loop
    ) -> None:
        """Wake lands while the camera is shutting down: it reports sleep,
        the wake is sent again, and only the real wake confirms."""
        from custom_components.tuya_peephole.models import TuyaMQTTMessage

        coordinator = _make_coordinator(mock_hass_with_loop)
        coordinator.mqtt_client = _make_mock_mqtt_client()

        async def camera() -> None:
            await asyncio.sleep(0.01)
            for payload in (_SLEEP_DPS, _SLEEP_OFFLINE):
                coordinator._handle_mqtt_message(
                    TuyaMQTTMessage.parse(_TOPIC, payload)
                )
            await asyncio.sleep(0.01)
            # nothing confirmed by the sleep messages
            assert coordinator.mqtt_client.publish.call_count == 2
            coordinator._handle_mqtt_message(TuyaMQTTMessage.parse(_TOPIC, _WAKE_DPS))

        task = asyncio.create_task(camera())
        with patch(
            "custom_components.tuya_peephole.coordinator.async_call_later",
            return_value=MagicMock(),
        ):
            result = await coordinator.async_wake_camera(force=True)
        await task

        assert result is True
        wake_topics = [
            c.args[0] for c in coordinator.mqtt_client.publish.call_args_list
        ]
        assert wake_topics == ["m/w/test_device_id_abc123"] * 2

    @pytest.mark.asyncio
    async def test_forced_wake_of_awake_camera_does_not_wait(
        self, mock_hass_with_loop
    ) -> None:
        from custom_components.tuya_peephole.models import CameraState

        coordinator = _make_coordinator(mock_hass_with_loop)
        coordinator.mqtt_client = _make_mock_mqtt_client()
        coordinator._camera_state = CameraState.AWAKE

        result = await asyncio.wait_for(
            coordinator.async_wake_camera(force=True), timeout=1
        )

        assert result is True
        coordinator.mqtt_client.publish.assert_called_once()


# The camera's cloud link coming back up. Seen alone (without the DPS 149
# false / online false before it) when the camera fell asleep, and also
# just before DPS 149 true when it wakes.
_LINK_UP = (
    b'{"data":{"devId":"test_device","online":true,'
    b'"lastOnlineChangeTime":1794764040743,"gwId":"test_device"},'
    b'"protocol":1,"s":764040904,"t":1794764040,"type":"update"}'
)


class TestLinkChange:
    """A link change means the camera can no longer be assumed awake."""

    def test_link_up_is_a_link_change_not_a_wake(self) -> None:
        from custom_components.tuya_peephole.models import TuyaMQTTMessage

        msg = TuyaMQTTMessage.parse(_TOPIC, _LINK_UP)
        assert msg.is_link_change is True
        assert msg.is_wireless_awake is False
        assert msg.is_sleep_signal is False
        assert TuyaMQTTMessage.parse(_TOPIC, _WAKE_DPS).is_link_change is False

    def test_missed_sleep_signal(self, mock_hass_with_loop) -> None:
        """Only the final link-up arrives: the camera is treated as asleep."""
        from custom_components.tuya_peephole.models import (
            CameraState,
            TuyaMQTTMessage,
        )

        coordinator = _make_coordinator(mock_hass_with_loop)
        coordinator._camera_state = CameraState.AWAKE
        coordinator._awake_event.set()

        coordinator._handle_mqtt_message(TuyaMQTTMessage.parse(_TOPIC, _LINK_UP))

        assert coordinator.camera_state == CameraState.SLEEPING
        assert not coordinator._awake_event.is_set()

    def test_wake_sequence_ends_awake(self, mock_hass_with_loop) -> None:
        from custom_components.tuya_peephole.models import (
            CameraState,
            TuyaMQTTMessage,
        )

        coordinator = _make_coordinator(mock_hass_with_loop)
        for payload in (_LINK_UP, _WAKE_DPS):
            coordinator._handle_mqtt_message(TuyaMQTTMessage.parse(_TOPIC, payload))

        assert coordinator.camera_state == CameraState.AWAKE
        assert coordinator._awake_event.is_set()

    def test_mark_camera_asleep(self, mock_hass_with_loop) -> None:
        from custom_components.tuya_peephole.models import CameraState

        coordinator = _make_coordinator(mock_hass_with_loop)
        coordinator.async_set_updated_data = MagicMock()
        coordinator._camera_state = CameraState.AWAKE
        coordinator.mark_camera_asleep("test")
        assert coordinator.camera_state == CameraState.SLEEPING
        coordinator.async_set_updated_data.assert_called_once()

        coordinator.mark_camera_asleep("again")  # already asleep: no update
        coordinator.async_set_updated_data.assert_called_once()
