"""Tuya Peephole Camera integration for Home Assistant.

Sets up the coordinator, forwards the entity platforms (motion sensor,
snapshot button and snapshot image), and manages token refresh and clean
teardown.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.aiohttp_client import async_create_clientsession
from homeassistant.helpers.event import async_track_time_interval

from .api import TuyaSmartAPI
from .const import (
    CONF_DEVICE_ID,
    CONF_EMAIL,
    CONF_LOCAL_KEY,
    CONF_PASSWORD,
    CONF_REGION,
    DOMAIN,
    REGIONS,
    TOKEN_REFRESH_HOURS,
)
from .coordinator import TuyaPeepholeCoordinator
from .exceptions import TuyaApiError, TuyaAuthError

_LOGGER = logging.getLogger(__name__)

TOKEN_REFRESH_INTERVAL = timedelta(hours=TOKEN_REFRESH_HOURS)

PLATFORMS = [
    Platform.BINARY_SENSOR,
    Platform.BUTTON,
    Platform.IMAGE,
]

# Unique id suffixes of entities that earlier versions created (live view,
# doorbell, battery/signal sensors, awake sensor, wake button). Their
# registry entries are removed so they don't linger as unavailable.
REMOVED_ENTITY_KEYS = (
    "camera",
    "doorbell",
    "battery",
    "signal_strength",
    "camera_awake",
    "wake_camera",
)


def _async_remove_stale_entities(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Delete registry entries of entities this version no longer provides."""
    registry = er.async_get(hass)
    stale = {
        f"{entry.data[CONF_DEVICE_ID]}_{key}" for key in REMOVED_ENTITY_KEYS
    }
    for entity in er.async_entries_for_config_entry(registry, entry.entry_id):
        if entity.unique_id in stale:
            _LOGGER.info("Removing entity no longer provided: %s", entity.entity_id)
            registry.async_remove(entity.entity_id)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Tuya Peephole Camera from a config entry.

    Creates an API client, performs a fresh login, and registers
    a 6-hour token refresh interval to keep the session alive.
    """
    session = async_create_clientsession(hass)
    region = entry.data[CONF_REGION]

    api = TuyaSmartAPI(
        session=session,
        host=REGIONS[region],
        email=entry.data[CONF_EMAIL],
        password=entry.data[CONF_PASSWORD],
        country_code=region.upper(),
    )

    # Fresh login on startup (don't persist sid/cookies across restarts)
    try:
        await api.async_login()
    except TuyaAuthError as err:
        raise ConfigEntryAuthFailed(f"Authentication failed: {err}") from err
    except TuyaApiError as err:
        raise ConfigEntryNotReady(f"Cannot connect to Tuya API: {err}") from err

    # Create coordinator (owns MQTT client lifecycle)
    coordinator = TuyaPeepholeCoordinator(
        hass, api, entry.data[CONF_DEVICE_ID], entry.data[CONF_LOCAL_KEY]
    )

    # Connect MQTT explicitly (don't rely on _async_setup being called by DUC)
    try:
        await coordinator.async_connect_mqtt()
        _LOGGER.info("MQTT connected for device %s", entry.data[CONF_DEVICE_ID])
    except Exception as err:
        _LOGGER.error("MQTT connection failed: %s", err)
        raise ConfigEntryNotReady(f"MQTT connection failed: {err}") from err

    await coordinator.async_config_entry_first_refresh()

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = coordinator

    _async_remove_stale_entities(hass, entry)
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    # Proactive token refresh every 6 hours (before session expiry)
    async def _async_refresh_token(_now) -> None:
        """Re-login to refresh the Tuya session token."""
        try:
            await api.async_login()
            _LOGGER.debug("Tuya session refreshed successfully")
        except TuyaAuthError:
            _LOGGER.error(
                "Token refresh failed: authentication error, starting reauth"
            )
            entry.async_start_reauth(hass)
        except TuyaApiError as err:
            _LOGGER.warning(
                "Token refresh failed, will retry next interval: %s", err
            )

    unsub = async_track_time_interval(
        hass, _async_refresh_token, TOKEN_REFRESH_INTERVAL
    )
    entry.async_on_unload(unsub)

    _LOGGER.info(
        "Tuya Peephole integration set up for device %s",
        entry.data[CONF_DEVICE_ID],
    )
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a Tuya Peephole Camera config entry.

    Unloads entity platforms, tears down the coordinator (disconnects MQTT),
    and removes stored data. The token refresh timer is cleaned up
    automatically via entry.async_on_unload.
    """
    unload_ok = await hass.config_entries.async_unload_platforms(
        entry, PLATFORMS
    )
    if unload_ok:
        coordinator: TuyaPeepholeCoordinator = hass.data[DOMAIN].pop(
            entry.entry_id
        )
        await coordinator.async_teardown()
    return unload_ok
