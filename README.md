# Tuya Peephole Camera

Custom Home Assistant integration for battery-powered Wi-Fi peephole cameras based on Tuya Smart platform.

## Features

- **Live Video Stream** — WebRTC video directly in HA dashboard via Tuya MQTT signaling
- **Camera Wake** — Wake sleeping battery camera on demand, resending the wake packet every second (or on the cloud's wake schedule) until it answers, as the official app does
- **Doorbell** — Doorbell presses (MQTT protocol 43) as an `event` entity and a `tuya_peephole_doorbell` HA event
- **Motion Detection** — Real-time motion alerts via MQTT push (no polling)
- **Snapshot** — On-demand image grabbed from the live WebRTC stream; shown by a separate `image` entity, while the camera entity is live video only (no automatic captures, to save battery)
- **Battery & Signal Sensors** — Monitor charge level and Wi-Fi strength
- **Motion Recording** — Automatic 60-second MP4 recording on motion events (the camera's H.265 stream is saved as is, without re-encoding; playback needs a browser with HEVC support)
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
| `camera.tuya_peephole` | Camera | Live WebRTC video stream |
| `binary_sensor.tuya_peephole_motion` | Binary Sensor | Motion detection |
| `binary_sensor.tuya_peephole_camera_awake` | Binary Sensor | On while the camera is awake, off while sleeping (`camera_state` attribute shows sleeping/waking/awake) |
| `sensor.tuya_peephole_battery` | Sensor | Battery percentage |
| `sensor.tuya_peephole_signal_strength` | Sensor | Wi-Fi RSSI (disabled by default) |
| `event.tuya_peephole_doorbell` | Event | Fires `ring` on each doorbell press (attributes: `message_id`, `call_type`) |
| `button.tuya_peephole_wake` | Button | Manual camera wake |
| `button.tuya_peephole_snapshot` | Button | Wake the camera and grab one frame from the stream into the snapshot image |
| `image.tuya_peephole_snapshot` | Image | Last snapshot; its state is the capture time |

## Technical Details

- Uses **Tuya Smart App API** (`protect-eu.ismartlife.me`) — no IoT Platform quota limits
- **MQTT** for real-time events and WebRTC signaling (push-only, no polling)
- **WebRTC SDP proxy** — browser connects directly to camera, HA relays signaling
- **aiortc** for server-side recording (motion-triggered MP4 capture)
- **paho-mqtt 2.1** with AsyncioHelper pattern for HA event loop integration

## Requirements

- Home Assistant 2024.1+
- Tuya Smart / Smart Life account with a peephole camera
- Camera device ID and local key

## License

MIT
