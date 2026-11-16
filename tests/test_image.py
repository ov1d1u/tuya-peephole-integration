"""Tests for the snapshot image entity."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from custom_components.tuya_peephole.image import TuyaSnapshotImage


def _make_image(mock_coordinator) -> TuyaSnapshotImage:
    mock_coordinator.last_snapshot = None
    mock_coordinator.last_snapshot_time = None
    return TuyaSnapshotImage(mock_coordinator)


class TestSnapshotImage:
    """Tests for TuyaSnapshotImage."""

    def test_unique_id_and_name(self, mock_coordinator) -> None:
        image = _make_image(mock_coordinator)
        assert image._attr_unique_id == "test_device_id_abc123_snapshot_image"
        assert image._attr_name == "Snapshot"
        assert image._attr_content_type == "image/jpeg"
        assert image.hass is mock_coordinator.hass

    @pytest.mark.asyncio
    async def test_async_image_none_without_snapshot(
        self, mock_coordinator
    ) -> None:
        image = _make_image(mock_coordinator)
        assert await image.async_image() is None
        assert image.image_last_updated is None

    @pytest.mark.asyncio
    async def test_async_image_returns_stored_snapshot(
        self, mock_coordinator
    ) -> None:
        image = _make_image(mock_coordinator)
        mock_coordinator.last_snapshot = b"\xff\xd8jpeg"
        mock_coordinator.last_snapshot_time = 1_700_000_000.0
        assert await image.async_image() == b"\xff\xd8jpeg"
        assert image.image_last_updated == datetime(
            2023, 11, 14, 22, 13, 20, tzinfo=UTC
        )
        mock_coordinator.async_wake_camera.assert_not_awaited()

    def test_available_when_mqtt_disconnected(self, mock_coordinator) -> None:
        image = _make_image(mock_coordinator)
        mock_coordinator.mqtt_client = None
        assert image.available is True
