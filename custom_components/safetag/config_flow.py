"""Safetag configuration flows."""

from hashlib import sha256

import voluptuous as vol
from homeassistant.config_entries import ConfigFlow
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.selector import TextSelector, TextSelectorConfig, TextSelectorType

from .api import (
    SafetagAuthError,
    SafetagClient,
    SafetagConnectionError,
    SafetagRateLimitError,
    SafetagResponseError,
    TrackerStore,
)
from .const import CONF_API_KEY, CONF_USERNAME, DOMAIN

SCHEMA = vol.Schema(
    {
        vol.Required(CONF_USERNAME): TextSelector(TextSelectorConfig(type=TextSelectorType.EMAIL)),
        vol.Required(CONF_API_KEY): TextSelector(
            TextSelectorConfig(type=TextSelectorType.PASSWORD)
        ),
    }
)


class SafetagConfigFlow(ConfigFlow, domain=DOMAIN):
    """Configure a Safetag account."""

    VERSION = 1

    async def _validate(self, user_input):
        username = user_input[CONF_USERNAME].strip().lower()
        try:
            client = SafetagClient(
                async_get_clientsession(self.hass), username, user_input[CONF_API_KEY]
            )
            store = TrackerStore()
            store.replace(await client.get_trackers())
            # Evaluate normalization too: a valid envelope alone is insufficient.
            store.trackers
        except SafetagAuthError:
            return "invalid_auth"
        except (SafetagConnectionError, SafetagRateLimitError, TimeoutError):
            return "cannot_connect"
        except SafetagResponseError:
            return "invalid_response"
        except Exception:
            # Never log upstream errors: they may contain credentials or location.
            return "unknown"
        return None

    async def async_step_user(self, user_input=None):
        errors = {}
        if user_input is not None:
            error = await self._validate(user_input)
            if error:
                errors["base"] = error
            else:
                username = user_input[CONF_USERNAME].strip().lower()
                await self.async_set_unique_id(sha256(username.encode()).hexdigest())
                self._abort_if_unique_id_configured()
                return self.async_create_entry(
                    title="Safetag", data={**user_input, CONF_USERNAME: username}
                )
        return self.async_show_form(step_id="user", data_schema=SCHEMA, errors=errors)

    async def async_step_reauth(self, entry_data):
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(self, user_input=None):
        return await self._rotate_key("reauth_confirm", self._get_reauth_entry(), user_input)

    async def async_step_reconfigure(self, user_input=None):
        return await self._rotate_key("reconfigure", self._get_reconfigure_entry(), user_input)

    async def _rotate_key(self, step, entry, user_input):
        errors = {}
        if user_input is not None:
            # Account identity cannot change when rotating credentials.
            data = {**entry.data, CONF_API_KEY: user_input[CONF_API_KEY]}
            error = await self._validate(data)
            if error:
                errors["base"] = error
            else:
                return self.async_update_reload_and_abort(
                    entry,
                    data_updates=data,
                    reason="reauth_successful"
                    if step == "reauth_confirm"
                    else "reconfigure_successful",
                )
        return self.async_show_form(
            step_id=step,
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_API_KEY): TextSelector(
                        TextSelectorConfig(type=TextSelectorType.PASSWORD)
                    )
                }
            ),
            errors=errors,
        )
