"""Constants for the Tuya Peephole Camera integration."""

DOMAIN = "tuya_peephole"

# Config entry keys
CONF_EMAIL = "email"
CONF_PASSWORD = "password"
CONF_DEVICE_ID = "device_id"
CONF_REGION = "region"

# Region-to-host mapping for Tuya Smart App API
REGIONS: dict[str, str] = {
    "eu": "protect-eu.ismartlife.me",
    "eu_east": "protect-we.ismartlife.me",
    "us": "protect-us.ismartlife.me",
    "us_east": "protect-ue.ismartlife.me",
    "cn": "protect.ismartlife.me",
    "in": "protect-in.ismartlife.me",
}

# Friendly names for region selector in config flow
REGION_NAMES: dict[str, str] = {
    "eu": "Europe / CIS / Kazakhstan",
    "eu_east": "Europe (East)",
    "us": "Americas (West)",
    "us_east": "Americas (East)",
    "cn": "China",
    "in": "India",
}

# API settings
API_TIMEOUT = 15  # seconds per HTTP request
TOKEN_REFRESH_HOURS = 6  # proactive refresh before expiry
MQTT_WEBRTC_CACHE_TTL = 300  # 5-minute cache for MQTT/WebRTC config

# MQTT settings
CONF_LOCAL_KEY = "local_key"
MQTT_PORT = 8883
MQTT_KEEPALIVE = 60
MQTT_CONNECT_TIMEOUT = 15  # seconds
MQTT_RECONNECT_MIN_DELAY = 5  # seconds before the first reconnect attempt
MQTT_RECONNECT_MAX_DELAY = 300  # cap for the exponential reconnect backoff
WAKE_TIMEOUT = 20  # seconds to wait for wireless_awake
WAKE_COOLDOWN = 5  # seconds between wake requests
# The official app resends the wake packet every second until the camera
# wakes, unless the cloud supplies its own schedule (p2pConfig.wakeScheduleMs)
WAKE_RESEND_INTERVAL = 1  # seconds
MOTION_CLEAR_TIMEOUT = 30  # seconds before motion auto-clears

# WebRTC signaling settings
WEBRTC_SESSION_TIMEOUT = 30  # seconds to wait for SDP answer from camera
WEBRTC_PUBLISH_TOPIC_TEMPLATE = "/av/moto/{moto_id}/u/{device_id}"
WEBRTC_SUBSCRIBE_TOPIC_TEMPLATE = "/av/u/{msid}"
WEBRTC_STREAM_TYPE_HD = 0

# Snapshot settings
SNAPSHOT_TIMEOUT = 15  # seconds to wait for snapshot frame
SNAPSHOT_SKIP_FRAMES = 5  # decoded frames to skip while the stream settles
