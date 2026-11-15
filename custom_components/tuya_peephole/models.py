"""Data models for the Tuya Peephole Camera integration.

Contains CameraState enum for camera lifecycle tracking and
TuyaMQTTMessage parser for incoming MQTT payloads.
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
from enum import Enum
from typing import Any

_LOGGER = logging.getLogger(__name__)

# DPS the camera sets true on waking and false on going to sleep
DPS_AWAKE = "149"

# DPS that carry event notifications as base64-encoded JSON, e.g.
# {"cmd":"ipc_motion","type":"image","alarm":true,...}. Peepholes use 212
# (initiative_message); other Tuya cameras use 115 (movement_detect_pic)
# and 185 (alarm_message).
DPS_MOTION_EVENTS = ("212", "115", "185")

# Substrings of an event's "cmd" that mean motion or a person at the door
MOTION_EVENT_CMDS = ("motion", "pir", "human", "door_lock_video")


def _is_motion_event_value(value: Any) -> bool:
    """Check whether a DPS event value describes a motion event.

    Values are usually base64-encoded JSON; plain JSON and plain
    strings are accepted too.
    """
    if not isinstance(value, str) or not value:
        return False
    text = value
    if not value.lstrip().startswith("{"):
        try:
            text = base64.b64decode(value, validate=True).decode("utf-8")
        except (binascii.Error, ValueError):
            text = value
    try:
        event = json.loads(text)
    except ValueError:
        return any(cmd in text.lower() for cmd in MOTION_EVENT_CMDS)
    if not isinstance(event, dict):
        return False
    cmd = str(event.get("cmd", "")).lower()
    return any(key in cmd for key in MOTION_EVENT_CMDS)


class CameraState(Enum):
    """Camera lifecycle state (3-state model).

    SLEEPING: Camera is in low-power mode, not streaming.
    WAKING: Wake command sent, waiting for wireless_awake confirmation.
    AWAKE: Camera confirmed awake, ready for WebRTC streaming.
    """

    SLEEPING = "sleeping"
    WAKING = "waking"
    AWAKE = "awake"


class TuyaMQTTMessage:
    """Parser for incoming Tuya MQTT messages.

    Handles both JSON and binary payloads from the Tuya MQTT broker.
    Messages arrive on topic smart/decrypt/in/{device_id} and may
    contain wake confirmations, motion events, or WebRTC signaling.
    """

    def __init__(self, topic: str, payload: bytes) -> None:
        """Initialize message parser.

        Args:
            topic: MQTT topic the message was received on.
            payload: Raw message payload bytes.
        """
        self.topic = topic
        self.raw = payload
        self._text: str = payload.decode("utf-8", errors="replace")
        self._json: dict | None = None

        try:
            self._json = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

    @classmethod
    def parse(cls, topic: str, payload: bytes) -> TuyaMQTTMessage:
        """Create a TuyaMQTTMessage from raw MQTT data.

        Args:
            topic: MQTT topic string.
            payload: Raw payload bytes.

        Returns:
            Parsed TuyaMQTTMessage instance.
        """
        return cls(topic, payload)

    @property
    def is_wireless_awake(self) -> bool:
        """Check if this message indicates the camera is awake.

        Some cameras send 'wireless_awake' explicitly.
        Others just send DPS updates (protocol 4) or events (protocol 56)
        when they're active — any such message means the camera is awake.
        """
        # The camera announcing it is going to sleep is not a wake
        if self.is_sleep_signal:
            return False
        # Explicit wake confirmation
        if "wireless_awake" in self._text:
            return True
        # DPS update = camera is active and responding
        if self._json is not None:
            proto = self._json.get("protocol")
            if proto in (4, 56):
                return True
        return False

    @property
    def is_sleep_signal(self) -> bool:
        """Check if this message says the camera is going to sleep.

        The camera reports DPS 149 = true when it wakes and false when it
        goes back to sleep, and the cloud reports it "online": false as it
        drops to its low-power link. A camera in that state still answers
        WebRTC signaling but never sends video.
        """
        if self._json is None:
            return False
        data = self._json.get("data")
        if not isinstance(data, dict):
            return False
        if self._json.get("protocol") == 4:
            dps = data.get("dps")
            if isinstance(dps, dict) and dps.get(DPS_AWAKE) is False:
                return True
        return data.get("online") is False

    @property
    def is_motion(self) -> bool:
        """Check if this message indicates a motion/PIR event.

        Detects motion via:
        - Protocol 56 with warnLevel (Tuya alarm/motion event)
        - DPS 212/115/185 event payloads (base64 JSON such as
          {"cmd":"ipc_motion","alarm":true,...})
        - PIR/alarm keys in the data dict, or keywords in non-JSON text
        """
        if self._json is not None:
            # Protocol 56 = alarm/motion event from Tuya
            if self._json.get("protocol") == 56:
                data = self._json.get("data", {})
                if isinstance(data, dict) and data.get("warnLevel"):
                    return True

            data = self._json.get("data", {})
            if isinstance(data, dict):
                dps = data.get("dps", {})
                if isinstance(dps, dict):
                    for dp in DPS_MOTION_EVENTS:
                        if _is_motion_event_value(dps.get(dp)):
                            return True
                # Check data dict for motion-related keys
                for key in data:
                    if key in ("pir", "alarm_message", "movement_detect_pic"):
                        return True
            # JSON payloads carry base64 values that can contain these
            # keywords by chance, so the raw-text fallback is not used
            return False

        # Fallback for non-JSON payloads: substring match in raw text
        text_lower = self._text.lower()
        return any(
            keyword in text_lower
            for keyword in ("pir", "alarm_message", "movement_detect_pic")
        )

    @property
    def battery_percentage(self) -> int | None:
        """Extract battery percentage from message data.

        Checks 'battery_percentage' and 'residual_electricity' (Tuya alternate key).
        Returns None if not present or not parseable. Clamped to 0-100.
        """
        if self._json is None:
            return None
        data = self._json.get("data")
        if not isinstance(data, dict):
            return None
        raw = data.get("battery_percentage")
        if raw is None:
            raw = data.get("residual_electricity")
        if raw is None:
            return None
        try:
            value = int(raw)
        except (ValueError, TypeError):
            return None
        return max(0, min(100, value))

    @property
    def signal_strength(self) -> int | None:
        """Extract signal strength (RSSI) from message data.

        Checks 'signal_strength' and 'wifi_signal' (Tuya alternate key).
        Returns None if not present or not parseable. No clamping (RSSI is negative).
        """
        if self._json is None:
            return None
        data = self._json.get("data")
        if not isinstance(data, dict):
            return None
        raw = data.get("signal_strength")
        if raw is None:
            raw = data.get("wifi_signal")
        if raw is None:
            return None
        try:
            return int(raw)
        except (ValueError, TypeError):
            return None

    @property
    def json_data(self) -> dict | None:
        """Return parsed JSON data or None if payload is not JSON."""
        return self._json

    @property
    def is_protocol_302(self) -> bool:
        """Check if this message is a WebRTC signaling message (protocol 302)."""
        if self._json is None:
            return False
        return self._json.get("protocol") == 302

    @property
    def text(self) -> str:
        """Return decoded text representation of the payload."""
        return self._text

    def __repr__(self) -> str:
        """Return debug representation."""
        return (
            f"TuyaMQTTMessage(topic={self.topic!r}, "
            f"awake={self.is_wireless_awake}, "
            f"motion={self.is_motion}, "
            f"len={len(self.raw)})"
        )
