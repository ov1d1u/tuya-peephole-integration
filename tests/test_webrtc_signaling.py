"""Tests for WebRTC signaling helpers (protocol 302 message format, SDP/ICE manipulation).

Covers all 6 exported functions from webrtc_signaling.py:
- generate_session_id
- strip_sdp_extmap
- build_protocol_302_message
- build_offer_payload
- build_disconnect_payload
- parse_protocol_302_message

Requirements covered: STRM-01, STRM-02, STRM-03
"""

from __future__ import annotations

import json
import string



# ---------------------------------------------------------------------------
# generate_session_id tests
# ---------------------------------------------------------------------------


class TestGenerateSessionId:
    """Tests for 6-char alphanumeric session ID generator."""

    def test_session_id_length(self) -> None:
        """Session ID is exactly 6 characters."""
        from custom_components.tuya_peephole.webrtc_signaling import (
            generate_session_id,
        )

        sid = generate_session_id()
        assert len(sid) == 6

    def test_session_id_alphanumeric(self) -> None:
        """Session ID contains only ASCII letters and digits (base-62)."""
        from custom_components.tuya_peephole.webrtc_signaling import (
            generate_session_id,
        )

        allowed = set(string.ascii_letters + string.digits)
        sid = generate_session_id()
        assert all(c in allowed for c in sid)

    def test_session_id_uniqueness(self) -> None:
        """100 generated IDs are not all identical (statistical uniqueness)."""
        from custom_components.tuya_peephole.webrtc_signaling import (
            generate_session_id,
        )

        ids = {generate_session_id() for _ in range(100)}
        # With 62^6 possible values, 100 samples should produce many distinct IDs
        assert len(ids) > 1


# ---------------------------------------------------------------------------
# strip_sdp_extmap tests
# ---------------------------------------------------------------------------


class TestStripSdpExtmap:
    """Tests for SDP extmap line removal (8KB payload limit workaround)."""

    def test_strip_extmap_removes_lines(self, sample_sdp_offer: str) -> None:
        """All a=extmap: lines are removed from the SDP."""
        from custom_components.tuya_peephole.webrtc_signaling import (
            strip_sdp_extmap,
        )

        result = strip_sdp_extmap(sample_sdp_offer)
        # No "a=extmap:" lines should remain
        for line in result.split("\r\n"):
            assert not line.startswith("a=extmap:")

    def test_strip_extmap_preserves_other_lines(
        self, sample_sdp_offer: str
    ) -> None:
        """Non-extmap SDP lines (m=audio, m=video, a=rtpmap) are preserved."""
        from custom_components.tuya_peephole.webrtc_signaling import (
            strip_sdp_extmap,
        )

        result = strip_sdp_extmap(sample_sdp_offer)
        assert "m=audio" in result
        assert "m=video" in result
        assert "a=rtpmap:111 opus/48000/2" in result
        assert "a=rtpmap:96 H264/90000" in result
        assert "a=group:BUNDLE 0 1" in result

    def test_strip_extmap_no_extmap(self) -> None:
        """SDP without extmap lines is returned unchanged."""
        from custom_components.tuya_peephole.webrtc_signaling import (
            strip_sdp_extmap,
        )

        sdp = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=rtpmap:111 opus/48000/2\r\n"
        result = strip_sdp_extmap(sdp)
        assert result == sdp

    def test_strip_extmap_also_strips_extmap_allow_mixed(
        self, sample_sdp_offer: str
    ) -> None:
        """The regex also strips a=extmap-allow-mixed (matches go2rtc behavior).

        The regex r'\\r\\na=extmap[^\\r\\n]*' matches 'a=extmap-allow-mixed'
        because it starts with 'a=extmap'. This matches go2rtc behavior.
        """
        from custom_components.tuya_peephole.webrtc_signaling import (
            strip_sdp_extmap,
        )

        result = strip_sdp_extmap(sample_sdp_offer)
        assert "extmap-allow-mixed" not in result


# ---------------------------------------------------------------------------
# build_protocol_302_message tests
# ---------------------------------------------------------------------------


class TestBuildProtocol302Message:
    """Tests for the full MQTT protocol 302 JSON envelope builder."""

    def _build(self, msg_type: str = "offer", payload: dict | None = None) -> bytes:
        """Helper to build a message with standard parameters."""
        from custom_components.tuya_peephole.webrtc_signaling import (
            build_protocol_302_message,
        )

        return build_protocol_302_message(
            msg_type=msg_type,
            uid="test_uid_msid",
            device_id="test_device_123",
            session_id="AbC12x",
            moto_id="moto_cnpre002",
            msg_payload=payload or {"mode": "webrtc", "sdp": "v=0..."},
        )

    def test_build_offer_message(self) -> None:
        """Offer message has correct protocol, pv, and header fields."""
        raw = self._build("offer", {"mode": "webrtc", "sdp": "v=0..."})
        msg = json.loads(raw)

        assert msg["protocol"] == 302
        assert msg["pv"] == "2.2"
        assert isinstance(msg["t"], int)

        header = msg["data"]["header"]
        assert header["type"] == "offer"
        assert header["from"] == "test_uid_msid"
        assert header["to"] == "test_device_123"
        assert header["sessionid"] == "AbC12x"
        assert header["moto_id"] == "moto_cnpre002"

        assert msg["data"]["msg"] == {"mode": "webrtc", "sdp": "v=0..."}

    def test_build_candidate_message(self) -> None:
        """Candidate message has correct type and candidate payload."""
        raw = self._build(
            "candidate", {"mode": "webrtc", "candidate": "a=candidate:1 1 udp"}
        )
        msg = json.loads(raw)

        assert msg["data"]["header"]["type"] == "candidate"
        assert msg["data"]["msg"]["candidate"] == "a=candidate:1 1 udp"

    def test_build_disconnect_message(self) -> None:
        """Disconnect message has correct type."""
        raw = self._build("disconnect", {"mode": "webrtc"})
        msg = json.loads(raw)

        assert msg["data"]["header"]["type"] == "disconnect"

    def test_message_is_compact_json(self) -> None:
        """Output uses compact JSON (no spaces in separators)."""
        raw = self._build()
        text = raw.decode("utf-8")
        # Compact JSON uses ": " -> ":" and ", " -> ","
        assert ": " not in text
        assert ", " not in text

    def test_message_is_bytes(self) -> None:
        """Output is bytes, not str."""
        raw = self._build()
        assert isinstance(raw, bytes)


# ---------------------------------------------------------------------------
# build_offer_payload tests
# ---------------------------------------------------------------------------


class TestBuildOfferPayload:
    """Tests for the protocol 302 offer payload builder."""

    def test_offer_payload_fields(self) -> None:
        """Offer payload has all required fields with correct defaults."""
        from custom_components.tuya_peephole.webrtc_signaling import (
            build_offer_payload,
        )

        ice_servers = [{"urls": "stun:example.com:3478"}]
        payload = build_offer_payload(
            sdp="v=0\r\ntest",
            auth="testauthtoken",
            ice_servers=ice_servers,
        )

        assert payload["mode"] == "webrtc"
        assert payload["sdp"] == "v=0\r\ntest"
        assert payload["stream_type"] == 0  # HD default
        assert payload["auth"] == "testauthtoken"
        assert payload["datachannel_enable"] is False
        assert payload["token"] == ice_servers

    def test_offer_payload_sd_stream(self) -> None:
        """Offer payload with stream_type=1 for SD."""
        from custom_components.tuya_peephole.webrtc_signaling import (
            build_offer_payload,
        )

        payload = build_offer_payload(
            sdp="v=0",
            auth="auth",
            ice_servers=[],
            stream_type=1,
        )
        assert payload["stream_type"] == 1


# ---------------------------------------------------------------------------
# build_disconnect_payload tests
# ---------------------------------------------------------------------------


class TestBuildDisconnectPayload:
    """Tests for the protocol 302 disconnect payload builder."""

    def test_disconnect_payload(self) -> None:
        """Disconnect payload has mode field only."""
        from custom_components.tuya_peephole.webrtc_signaling import (
            build_disconnect_payload,
        )

        payload = build_disconnect_payload()
        assert payload == {"mode": "webrtc"}


# ---------------------------------------------------------------------------
# parse_protocol_302_message tests
# ---------------------------------------------------------------------------


class TestParseProtocol302Message:
    """Tests for the protocol 302 incoming message parser."""

    def _make_message(
        self,
        msg_type: str = "answer",
        session_id: str = "test01",
        msg_payload: dict | str | None = None,
        protocol: int = 302,
    ) -> bytes:
        """Build a raw protocol 302 message for parsing tests."""
        if msg_payload is None:
            msg_payload = {"sdp": "v=0\r\nanswer"}
        message = {
            "protocol": protocol,
            "pv": "2.2",
            "t": 1700000000,
            "data": {
                "header": {
                    "type": msg_type,
                    "from": "device_xyz",
                    "to": "uid_abc",
                    "sessionid": session_id,
                    "moto_id": "moto_test",
                },
                "msg": msg_payload,
            },
        }
        return json.dumps(message, separators=(",", ":")).encode("utf-8")

    def test_parse_answer(self) -> None:
        """Answer message is parsed with correct type and msg."""
        from custom_components.tuya_peephole.webrtc_signaling import (
            parse_protocol_302_message,
        )

        raw = self._make_message("answer", "sess01", {"sdp": "v=0\r\nanswer_sdp"})
        result = parse_protocol_302_message(raw, "sess01")

        assert result is not None
        assert result["type"] == "answer"
        assert result["msg"]["sdp"] == "v=0\r\nanswer_sdp"

    def test_parse_candidate(self) -> None:
        """Candidate message is parsed correctly."""
        from custom_components.tuya_peephole.webrtc_signaling import (
            parse_protocol_302_message,
        )

        raw = self._make_message(
            "candidate", "sess01", {"candidate": "a=candidate:1 1 udp"}
        )
        result = parse_protocol_302_message(raw, "sess01")

        assert result is not None
        assert result["type"] == "candidate"
        assert result["msg"]["candidate"] == "a=candidate:1 1 udp"

    def test_parse_wrong_session(self) -> None:
        """Message with non-matching session ID returns None."""
        from custom_components.tuya_peephole.webrtc_signaling import (
            parse_protocol_302_message,
        )

        raw = self._make_message("answer", "other_session")
        result = parse_protocol_302_message(raw, "my_session")
        assert result is None

    def test_parse_not_302(self) -> None:
        """Non-302 protocol message returns None."""
        from custom_components.tuya_peephole.webrtc_signaling import (
            parse_protocol_302_message,
        )

        raw = self._make_message("answer", "sess01", protocol=312)
        result = parse_protocol_302_message(raw, "sess01")
        assert result is None

    def test_parse_invalid_json(self) -> None:
        """Garbage bytes return None (no crash)."""
        from custom_components.tuya_peephole.webrtc_signaling import (
            parse_protocol_302_message,
        )

        result = parse_protocol_302_message(b"\x00\x01\x02garbage", "sess01")
        assert result is None

    def test_parse_msg_as_string(self) -> None:
        """Camera may send msg as JSON string instead of dict -- still parsed."""
        from custom_components.tuya_peephole.webrtc_signaling import (
            parse_protocol_302_message,
        )

        # Camera quirk: msg field is a JSON-encoded string, not a dict
        inner_json = json.dumps({"sdp": "v=0\r\nstring_encoded"})
        raw = self._make_message("answer", "sess01", msg_payload=inner_json)
        result = parse_protocol_302_message(raw, "sess01")

        assert result is not None
        assert result["type"] == "answer"
        assert result["msg"]["sdp"] == "v=0\r\nstring_encoded"

    def test_parse_empty_data(self) -> None:
        """Message with no data field returns None."""
        from custom_components.tuya_peephole.webrtc_signaling import (
            parse_protocol_302_message,
        )

        msg = json.dumps({"protocol": 302, "pv": "2.2", "t": 1}).encode()
        result = parse_protocol_302_message(msg, "sess01")
        assert result is None
