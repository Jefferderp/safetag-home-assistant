"""Initial snapshot and cloud push, with bounded reconnect backoff."""

import asyncio
import logging
import random
from contextlib import suppress
from time import monotonic

from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import SafetagAuthError, SafetagRateLimitError, TrackerStore
from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)
SNAPSHOT_MIN_INTERVAL = 60


class SafetagCoordinator(DataUpdateCoordinator):
    """Own the account snapshot and stream lifetime without routine polling."""

    def __init__(self, hass, entry, client):
        super().__init__(hass, _LOGGER, name=DOMAIN, config_entry=entry, update_interval=None)
        self.client = client
        self.entry = entry
        self.store = TrackerStore()
        self.connected = False
        self.stream_task = None
        self._last_snapshot = float("-inf")
        self._reauth_started = False

    async def _async_update_data(self):
        self._last_snapshot = monotonic()
        try:
            self.store.replace(await self.client.get_trackers())
            data = self.store.trackers
        except SafetagAuthError:
            self.connected = False
            raise ConfigEntryAuthFailed("Authentication failed") from None
        except SafetagRateLimitError as err:
            self.connected = False
            error = UpdateFailed("Safetag rate limit reached")
            error.retry_after = err.retry_after
            raise error from None
        except Exception:
            self.connected = False
            # Do not pass raw errors to HA's logger or diagnostics.
            raise UpdateFailed("Unable to retrieve Safetag trackers") from None
        self.connected = True
        return data

    def start(self):
        self.stream_task = self.hass.async_create_background_task(self._stream(), "Safetag stream")

    async def _wait_to_reconnect(self, delay):
        await asyncio.sleep(delay)

    def _auth_failed(self):
        self.connected = False
        self.async_set_update_error(UpdateFailed("Authentication failed"))
        if not self._reauth_started:
            self._reauth_started = True
            self.entry.async_start_reauth(self.hass)

    async def _stream(self):
        attempt = 0
        while True:
            retry_after = 0
            try:
                # Reconcile membership only on reconnect, at most once per minute.
                if attempt and monotonic() - self._last_snapshot >= SNAPSHOT_MIN_INTERVAL:
                    self.async_set_updated_data(await self._async_update_data())
                async for updates in self.client.stream():
                    self.store.update(updates)
                    self.connected = True
                    self.async_set_updated_data(self.store.trackers)
                    attempt = 0
            except (SafetagAuthError, ConfigEntryAuthFailed):
                self._auth_failed()
                return
            except SafetagRateLimitError as err:
                retry_after = err.retry_after
            except UpdateFailed as err:
                retry_after = getattr(err, "retry_after", 0)
            except Exception:
                # Transport, schema, and unexpected failures are all sanitized.
                pass
            self.connected = False
            self.async_set_update_error(UpdateFailed("Safetag stream disconnected"))
            delay = min(120, (2 ** min(attempt, 7)) * random.uniform(0.8, 1.2))
            attempt += 1
            await self._wait_to_reconnect(max(delay, retry_after))

    async def stop(self):
        if self.stream_task is not None:
            self.stream_task.cancel()
            with suppress(asyncio.CancelledError):
                await self.stream_task
