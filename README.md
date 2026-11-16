# Tuya Peephole Camera

Custom Home Assistant integration for battery-powered Wi-Fi peephole cameras based on Tuya Smart platform.

## Features

- **Motion Detection** — Real-time motion alerts via MQTT push (no polling)
- **Snapshot** — On-demand image: a button wakes the camera, grabs one frame from its WebRTC stream and shows it in an `image` entity (no automatic captures, to save battery)
- **Re-auth** — Update credentials without removing the integration

Live video, motion recording, the recordings browser, the doorbell event and the battery/signal sensors were removed in 0.5.0. That version lives on in the [`video`](../../tree/video) branch.

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
| `binary_sensor.tuya_peephole_motion` | Binary Sensor | Motion detection |
| `button.tuya_peephole_snapshot` | Button | Wake the camera and grab one frame from the stream into the snapshot image |
| `image.tuya_peephole_snapshot` | Image | Last snapshot; its state is the capture time |

Upgrading from an earlier version removes the entities this version no longer provides (camera, doorbell, battery, signal strength, camera awake, wake button) from the entity registry.

## Technical Details

- Uses **Tuya Smart App API** (`protect-eu.ismartlife.me`) — no IoT Platform quota limits
- **MQTT** for real-time events, camera wake and WebRTC signaling (push-only, no polling)
- **aiortc** for the short server-side WebRTC session that grabs a snapshot frame
- **paho-mqtt 2.1** with AsyncioHelper pattern for HA event loop integration

## Requirements

- Home Assistant 2024.1+
- Tuya Smart / Smart Life account with a peephole camera
- Camera device ID and local key

## License

MIT
