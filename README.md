# Tuya Peephole Camera

Custom Home Assistant integration for battery-powered Wi-Fi peephole cameras based on Tuya Smart platform.

## Features

- **Live Video Stream** — plays in every browser through Home Assistant (H.264, or the camera's H.265 passed through); the camera is connected only while someone watches
- **Camera Wake** — Wake sleeping battery camera on demand
- **Motion Detection** — Real-time motion alerts via MQTT push (no polling)
- **Snapshot** — On-demand image grabbed from the live WebRTC stream; the camera entity shows the last one (no automatic captures, to save battery)
- **Battery & Signal Sensors** — Monitor charge level and Wi-Fi strength
- **Motion Recording** — Automatic 60-second MP4 recording on motion events
- **Recording Browser** — Browse and play recordings via HA media browser
- **24/7 Stream** — Continuous streaming when camera is on charger
- **Event History** — View past events from Tuya Message Center
- **Re-auth** — Update credentials without removing the integration

## Why This Integration?

- **go2rtc cannot authenticate** with Tuya Smart API for certain regions (Kazakhstan bug)
- **Tuya IoT Platform API quota exhausted** — this uses the consumer Smart App API (no limits)
- **Battery camera support** — proper wake/sleep lifecycle management

## Installation

### HACS (Recommended)

1. Open HACS in Home Assistant
2. Click **Integrations** > **3 dots menu** > **Custom repositories**
3. Add `https://github.com/daurentakibaev/tuya-peephole-integration` as **Integration**
4. Install **Tuya Peephole Camera**
5. Restart Home Assistant

### Manual

Copy `custom_components/tuya_peephole/` to your HA `config/custom_components/` directory.

## Configuration

1. Go to **Settings** > **Devices & Services** > **Add Integration**
2. Search for **Tuya Peephole Camera**
3. Enter:
   - **Email** — your Tuya Smart / Smart Life app email
   - **Password** — your app password
   - **Device ID** — camera device ID (from Tuya app or API)
   - **Local Key** — device local key (from Tuya API)
   - **Region** — your Tuya region (EU, US, CN, IN, etc.)

## Entities

| Entity | Type | Description |
|--------|------|-------------|
| `camera.tuya_peephole` | Camera | Live video stream |
| `binary_sensor.tuya_peephole_motion` | Binary Sensor | Motion detection |
| `sensor.tuya_peephole_battery` | Sensor | Battery percentage |
| `sensor.tuya_peephole_signal_strength` | Sensor | Wi-Fi RSSI (disabled by default) |
| `button.tuya_peephole_wake` | Button | Manual camera wake |
| `button.tuya_peephole_snapshot` | Button | Wake the camera and grab one frame from the stream as the camera image |

## Technical Details

- Uses **Tuya Smart App API** (`protect-eu.ismartlife.me`) — no IoT Platform quota limits
- **MQTT** for real-time events and WebRTC signaling (push-only, no polling)
- **Datachannel media** — the camera's regular WebRTC tracks carry H.265 labelled as H.264, which browsers cannot play. HA connects with **aiortc** and asks the camera for its datachannel stream instead, which carries standard H.265 RTP
- **One shared stream** — live view, recordings and snapshots read the same camera session, which starts on demand and disconnects 10 s after the last viewer
- **Local restream** — the camera entity's stream source is an MPEG-TS stream served on `127.0.0.1`, played by HA's go2rtc (or its HLS player). Choose the codec in the integration options: `h264` (default, plays everywhere, uses some CPU) or `hevc` (no transcoding, needs an H.265-capable browser)
- **paho-mqtt 2.1** with AsyncioHelper pattern for HA event loop integration

## Requirements

- Home Assistant 2024.1+
- Tuya Smart / Smart Life account with a peephole camera
- Camera device ID and local key

## License

MIT
