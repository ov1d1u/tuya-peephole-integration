"""Tests for tuya_peephole recorder.py (RecordingSession, RecordingManager).

Tests the recording engine: session lifecycle (start/stop), disk space
checks, retention cleanup, motion callback
triggers, charging detection state machine, and continuous mode task
lifecycle.

Requirements covered: REC-01, REC-02, REC-03, REC-04, STRM-05
"""

from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# RecordingSession tests
# ---------------------------------------------------------------------------


class TestRecordingSession:
    """RecordingSession writes the shared live stream to MP4."""

    def _coord(self, coord, reader):
        coord.hass.async_add_executor_job = AsyncMock(
            side_effect=lambda fn, *args: fn(*args)
        )
        coord.live_stream = MagicMock()
        coord.live_stream.open = MagicMock(return_value=reader)
        return coord

    @pytest.mark.asyncio
    async def test_records_h264_mp4(self, mock_recording_coordinator, tmp_path) -> None:
        """[REC-01] Starts once video flows; stop finalizes a playable MP4."""
        import av

        from custom_components.tuya_peephole.recorder import RecordingSession

        from .test_hevc_stream import FIXTURE_PICTURES, _depacketize, _packets
        from .test_snapshot import _FakeReader

        reader = _FakeReader(_depacketize(_packets()))
        coord = self._coord(mock_recording_coordinator, reader)
        path = tmp_path / "clip.mp4.tmp"
        session = RecordingSession(coord, str(path))

        assert await session.async_start() is True
        await asyncio.sleep(0.05)  # let the writer drain the queue
        await session.async_stop()

        assert reader.closed
        with av.open(str(path), format="mp4") as container:
            stream = container.streams.video[0]
            assert stream.codec_context.name == "h264"
            assert len(list(container.decode(stream))) == FIXTURE_PICTURES

    @pytest.mark.asyncio
    async def test_no_video_fails_start(self, mock_recording_coordinator, tmp_path) -> None:
        """No keyframe in time -> False, reader closed, no file."""
        from custom_components.tuya_peephole import recorder

        from .test_snapshot import _FakeReader

        reader = _FakeReader([])
        coord = self._coord(mock_recording_coordinator, reader)
        path = tmp_path / "clip.mp4.tmp"
        session = recorder.RecordingSession(coord, str(path))
        with patch.object(recorder, "RECORDING_START_TIMEOUT", 0.05):
            assert await session.async_start() is False
        assert reader.closed
        assert not path.exists()

    @pytest.mark.asyncio
    async def test_stream_shut_down(self, mock_recording_coordinator, tmp_path) -> None:
        """The live stream shutting down before video -> False."""
        from custom_components.tuya_peephole.live_stream import StreamClosedError
        from custom_components.tuya_peephole.recorder import RecordingSession

        coord = self._coord(mock_recording_coordinator, None)
        coord.live_stream.open.side_effect = StreamClosedError
        session = RecordingSession(coord, str(tmp_path / "x.mp4.tmp"))
        assert await session.async_start() is False

    @pytest.mark.asyncio
    async def test_stop_is_idempotent(self, mock_recording_coordinator, tmp_path) -> None:
        from custom_components.tuya_peephole.recorder import RecordingSession

        session = RecordingSession(mock_recording_coordinator, str(tmp_path / "x"))
        await session.async_stop()
        await session.async_stop()


# ---------------------------------------------------------------------------
# RecordingManager tests
# ---------------------------------------------------------------------------


class TestRecordingManager:
    """Test RecordingManager lifecycle, disk checks, and recording triggers."""

    @pytest.mark.asyncio
    async def test_start_recording_success(
        self, mock_recording_coordinator, tmp_path
    ) -> None:
        """[REC-01] Recording starts when disk is free and camera is awake."""
        from custom_components.tuya_peephole.recorder import RecordingManager

        coord = mock_recording_coordinator
        hass = coord.hass
        hass.config = MagicMock()
        hass.config.path = MagicMock(
            side_effect=lambda *args: str(tmp_path / Path(*args))
        )
        hass.async_create_task = MagicMock(side_effect=lambda coro: asyncio.ensure_future(coro))

        manager = RecordingManager(hass, coord)
        manager._storage_path = tmp_path / "media" / "tuya_peephole" / "test_device_id_abc123"
        manager._storage_path.mkdir(parents=True, exist_ok=True)

        # Mock disk usage returning plenty of free space (1GB)
        mock_usage = MagicMock()
        mock_usage.free = 1024 * 1024 * 1024  # 1GB
        hass.async_add_executor_job = AsyncMock(
            side_effect=lambda fn, *args: fn(*args) if args else fn()
        )
        with patch("shutil.disk_usage", return_value=mock_usage):
            # Mock RecordingSession to avoid real WebRTC
            with patch(
                "custom_components.tuya_peephole.recorder.RecordingSession"
            ) as MockSession:
                mock_session_instance = MagicMock()
                mock_session_instance.async_start = AsyncMock(return_value=True)
                mock_session_instance._output_path = str(tmp_path / "test.mp4.tmp")
                MockSession.return_value = mock_session_instance

                # Mock async_call_later for timers
                with patch(
                    "custom_components.tuya_peephole.recorder.async_call_later",
                    return_value=MagicMock(),
                ):
                    result = await manager.async_start_recording()

        assert result is True
        assert manager._active_session is mock_session_instance

    @pytest.mark.asyncio
    async def test_start_recording_disk_full(
        self, mock_recording_coordinator, tmp_path
    ) -> None:
        """[REC-02] Recording skipped when disk space is insufficient."""
        from custom_components.tuya_peephole.recorder import RecordingManager

        coord = mock_recording_coordinator
        hass = coord.hass

        manager = RecordingManager(hass, coord)
        manager._storage_path = tmp_path / "media"
        manager._storage_path.mkdir(parents=True, exist_ok=True)

        # Mock disk usage returning only 50MB (below MIN_FREE_SPACE_MB=100)
        mock_usage = MagicMock()
        mock_usage.free = 50 * 1024 * 1024  # 50MB
        hass.async_add_executor_job = AsyncMock(
            side_effect=lambda fn, *args: fn(*args) if args else fn()
        )
        with patch("shutil.disk_usage", return_value=mock_usage):
            result = await manager.async_start_recording()

        assert result is False
        assert manager._active_session is None

    @pytest.mark.asyncio
    async def test_start_recording_already_active(
        self, mock_recording_coordinator, tmp_path
    ) -> None:
        """Recording skipped when another session is already active."""
        from custom_components.tuya_peephole.recorder import RecordingManager

        coord = mock_recording_coordinator
        hass = coord.hass

        manager = RecordingManager(hass, coord)
        manager._active_session = MagicMock()  # Already recording

        result = await manager.async_start_recording()

        assert result is False

    @pytest.mark.asyncio
    async def test_start_recording_session_fails(
        self, mock_recording_coordinator, tmp_path
    ) -> None:
        """No video from the camera -> no active recording."""
        from custom_components.tuya_peephole.recorder import RecordingManager

        coord = mock_recording_coordinator
        hass = coord.hass

        manager = RecordingManager(hass, coord)
        manager._storage_path = tmp_path / "media"
        manager._storage_path.mkdir(parents=True, exist_ok=True)

        mock_usage = MagicMock()
        mock_usage.free = 1024 * 1024 * 1024
        hass.async_add_executor_job = AsyncMock(
            side_effect=lambda fn, *args: fn(*args) if args else fn()
        )
        with (
            patch("shutil.disk_usage", return_value=mock_usage),
            patch(
                "custom_components.tuya_peephole.recorder.RecordingSession"
            ) as MockSession,
        ):
            MockSession.return_value.async_start = AsyncMock(return_value=False)
            result = await manager.async_start_recording()

        assert result is False
        assert manager._active_session is None

    @pytest.mark.asyncio
    async def test_cleanup_old_recordings(
        self, mock_recording_coordinator, tmp_path
    ) -> None:
        """[REC-03] Retention cleanup deletes old MP4 files and empty directories."""
        from custom_components.tuya_peephole.recorder import RecordingManager

        coord = mock_recording_coordinator
        hass = coord.hass
        hass.async_add_executor_job = AsyncMock(
            side_effect=lambda fn, *args: fn(*args) if args else fn()
        )

        manager = RecordingManager(hass, coord)
        manager._storage_path = tmp_path
        manager._retention_days = 7

        # Create old files (>7 days old)
        old_dir = tmp_path / "2026-03-01"
        old_dir.mkdir()
        old_file = old_dir / "test_device_20260301_120000.mp4"
        old_file.write_text("old recording")
        # Set mtime to 30 days ago
        old_mtime = time.time() - (30 * 86400)
        os.utime(old_file, (old_mtime, old_mtime))

        # Create recent file (today)
        recent_dir = tmp_path / "2026-04-05"
        recent_dir.mkdir()
        recent_file = recent_dir / "test_device_20260405_120000.mp4"
        recent_file.write_text("recent recording")

        await manager._async_cleanup_recordings()

        # Old file should be deleted
        assert not old_file.exists()
        # Old empty dir should be removed
        assert not old_dir.exists()
        # Recent file should still exist
        assert recent_file.exists()
        assert recent_dir.exists()

    @pytest.mark.asyncio
    async def test_cleanup_no_recordings(
        self, mock_recording_coordinator, tmp_path
    ) -> None:
        """Cleanup runs without error on empty storage directory."""
        from custom_components.tuya_peephole.recorder import RecordingManager

        coord = mock_recording_coordinator
        hass = coord.hass
        hass.async_add_executor_job = AsyncMock(
            side_effect=lambda fn, *args: fn(*args) if args else fn()
        )

        manager = RecordingManager(hass, coord)
        manager._storage_path = tmp_path / "nonexistent"

        # Should not raise any error
        await manager._async_cleanup_recordings()

    @pytest.mark.asyncio
    async def test_update_options(
        self, mock_recording_coordinator, tmp_path
    ) -> None:
        """update_options updates internal state."""
        from custom_components.tuya_peephole.recorder import RecordingManager

        coord = mock_recording_coordinator
        hass = coord.hass

        manager = RecordingManager(hass, coord)

        manager.update_options(retention_days=14, duration=120, enabled=False)

        assert manager._retention_days == 14
        assert manager._duration == 120
        assert manager._recording_enabled is False

    @pytest.mark.asyncio
    async def test_motion_callback_triggers_recording(
        self, mock_recording_coordinator, tmp_path
    ) -> None:
        """[REC-04] Motion event from coordinator triggers recording start."""
        from custom_components.tuya_peephole.recorder import RecordingManager

        coord = mock_recording_coordinator
        hass = coord.hass
        hass.config = MagicMock()
        hass.config.path = MagicMock(
            side_effect=lambda *args: str(tmp_path / Path(*args))
        )

        created_tasks = []
        hass.async_create_task = MagicMock(side_effect=lambda coro: created_tasks.append(coro))

        manager = RecordingManager(hass, coord)
        manager._recording_enabled = True

        # Simulate async_setup registering motion callback
        with patch(
            "custom_components.tuya_peephole.recorder.async_track_time_interval",
            return_value=MagicMock(),
        ):
            hass.async_add_executor_job = AsyncMock(
                side_effect=lambda fn, *args: fn(*args) if args else fn()
            )
            # Patch cleanup to avoid issues
            with patch.object(manager, "_async_cleanup_recordings", new=AsyncMock()):
                await manager.async_setup()

        # The motion callback should have been registered
        assert coord.register_motion_callback.called

        # Fire the motion callback
        manager._on_motion_event()

        # Should have scheduled a recording task
        assert len(created_tasks) > 0


# ---------------------------------------------------------------------------
# Charging detection tests (coordinator)
# ---------------------------------------------------------------------------


class TestChargingDetection:
    """Test coordinator charging detection heuristic."""

    @pytest.mark.asyncio
    async def test_charging_detected_sustained_battery_100(self) -> None:
        """[STRM-05] Battery=100 sustained for CHARGING_STABLE_MINUTES implies charging."""
        from custom_components.tuya_peephole.coordinator import (
            TuyaPeepholeCoordinator,
        )

        coord = TuyaPeepholeCoordinator.__new__(TuyaPeepholeCoordinator)
        coord._battery_percentage = 100
        coord._charging_detected = False
        coord._battery_100_since = None

        # First call: sets the start time
        coord._update_charging_state()
        assert coord._battery_100_since is not None
        assert coord._charging_detected is False

        # Simulate time passing beyond CHARGING_STABLE_MINUTES (5 min)
        coord._battery_100_since = time.monotonic() - (6 * 60)  # 6 minutes ago

        coord._update_charging_state()
        assert coord._charging_detected is True
        assert coord.is_charging is True

    @pytest.mark.asyncio
    async def test_charging_resets_on_battery_drop(self) -> None:
        """[STRM-05] Charging state resets when battery drops below 100."""
        from custom_components.tuya_peephole.coordinator import (
            TuyaPeepholeCoordinator,
        )

        coord = TuyaPeepholeCoordinator.__new__(TuyaPeepholeCoordinator)
        coord._battery_percentage = 100
        coord._charging_detected = True
        coord._battery_100_since = time.monotonic() - 600

        # Battery drops to 99
        coord._battery_percentage = 99
        coord._update_charging_state()

        assert coord._charging_detected is False
        assert coord._battery_100_since is None
        assert coord.is_charging is False

    @pytest.mark.asyncio
    async def test_motion_callback_registration_and_unsubscribe(self) -> None:
        """Motion callback can be registered and unsubscribed."""
        from custom_components.tuya_peephole.coordinator import (
            TuyaPeepholeCoordinator,
        )

        coord = TuyaPeepholeCoordinator.__new__(TuyaPeepholeCoordinator)
        coord._on_motion_callbacks = []

        callback_fired = False

        def test_callback():
            nonlocal callback_fired
            callback_fired = True

        unsub = coord.register_motion_callback(test_callback)

        # Callback should be in list
        assert test_callback in coord._on_motion_callbacks

        # Unsubscribe
        unsub()
        assert test_callback not in coord._on_motion_callbacks


# ---------------------------------------------------------------------------
# Continuous mode tests
# ---------------------------------------------------------------------------


class TestContinuousMode:
    """Test continuous recording mode lifecycle."""

    @pytest.mark.asyncio
    async def test_start_continuous_creates_task(
        self, mock_recording_coordinator, tmp_path
    ) -> None:
        """[STRM-05] async_start_continuous creates a background task."""
        from custom_components.tuya_peephole.recorder import RecordingManager

        coord = mock_recording_coordinator
        hass = coord.hass
        hass.config = MagicMock()
        hass.config.path = MagicMock(
            side_effect=lambda *args: str(tmp_path / Path(*args))
        )

        # Track task creation
        created_tasks = []
        hass.async_create_task = MagicMock(
            side_effect=lambda coro: created_tasks.append(coro) or MagicMock()
        )

        manager = RecordingManager(hass, coord)
        await manager.async_start_continuous()

        assert manager._continuous_task is not None

    @pytest.mark.asyncio
    async def test_stop_continuous_cancels_task(
        self, mock_recording_coordinator, tmp_path
    ) -> None:
        """[STRM-05] async_stop_continuous cancels the background task."""
        from custom_components.tuya_peephole.recorder import RecordingManager

        coord = mock_recording_coordinator
        hass = coord.hass
        hass.config = MagicMock()
        hass.config.path = MagicMock(
            side_effect=lambda *args: str(tmp_path / Path(*args))
        )

        manager = RecordingManager(hass, coord)

        # Create a real asyncio task that we can cancel
        async def _dummy_loop():
            try:
                while True:
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                pass

        task = asyncio.ensure_future(_dummy_loop())
        manager._continuous_task = task

        await manager.async_stop_continuous()

        assert task.cancelled() or task.done()
        assert manager._continuous_task is None

    @pytest.mark.asyncio
    async def test_start_continuous_noop_when_already_running(
        self, mock_recording_coordinator, tmp_path
    ) -> None:
        """Calling start_continuous when already running is a no-op."""
        from custom_components.tuya_peephole.recorder import RecordingManager

        coord = mock_recording_coordinator
        hass = coord.hass
        hass.config = MagicMock()
        hass.config.path = MagicMock(
            side_effect=lambda *args: str(tmp_path / Path(*args))
        )

        manager = RecordingManager(hass, coord)
        manager._continuous_task = MagicMock()  # Already running

        original_task = manager._continuous_task
        await manager.async_start_continuous()

        # Task should not change
        assert manager._continuous_task is original_task

    @pytest.mark.asyncio
    async def test_teardown_cleans_all_resources(
        self, mock_recording_coordinator, tmp_path
    ) -> None:
        """async_teardown cleans up continuous task, session, timers, and callbacks."""
        from custom_components.tuya_peephole.recorder import RecordingManager

        coord = mock_recording_coordinator
        hass = coord.hass

        manager = RecordingManager(hass, coord)

        # Set up mock resources
        mock_cleanup_unsub = MagicMock()
        mock_motion_unsub = MagicMock()
        mock_watchdog_unsub = MagicMock()
        mock_stop_timer_unsub = MagicMock()

        manager._cleanup_unsub = mock_cleanup_unsub
        manager._motion_unsub = mock_motion_unsub
        manager._watchdog_unsub = mock_watchdog_unsub
        manager._stop_timer_unsub = mock_stop_timer_unsub
        manager._continuous_task = None
        manager._active_session = None

        await manager.async_teardown()

        mock_cleanup_unsub.assert_called_once()
        mock_motion_unsub.assert_called_once()
        mock_watchdog_unsub.assert_called_once()
        mock_stop_timer_unsub.assert_called_once()
        assert manager._cleanup_unsub is None
        assert manager._motion_unsub is None
