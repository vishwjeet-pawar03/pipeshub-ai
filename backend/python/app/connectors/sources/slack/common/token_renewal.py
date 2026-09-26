"""Keep a Slack connector working when its app has token rotation turned on.

With rotation, Slack access tokens expire after about 12 hours. Each comes with a
single-use refresh token, and every refresh returns a new pair. The shared
``TokenRefreshService`` renews them on a schedule; this module covers the cases
the schedule can miss: a token found about to expire when the connector is about
to use it, and a token Slack has already refused. All renewals go through
``TokenRefreshService.refresh_now``, which lets one refresh run at a time per
connector and saves the new access and refresh token together.
"""

import inspect
import logging
import time
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

from app.config.configuration_service import ConfigurationService
from app.connectors.core.base.token_service.oauth_service import (
    OAuthToken,
    RefreshTokenInvalidError,
)
from app.sources.client.slack.slack import SlackClient
from app.sources.external.slack.slack import SlackDataSource

# What Slack answers when an access token has expired or was replaced.
RENEWABLE_ERRORS = frozenset({"token_expired", "invalid_auth"})

# Shorter than the refresh service's own 10-minute lead, so the schedule normally
# wins and this only runs when the schedule was missed.
RENEW_BEFORE_EXPIRY = timedelta(minutes=5)

# If Slack calls a token invalid this soon after issuing it, a new one won't fare
# better, and renewing on every refused call would hammer oauth.v2.access.
RENEWAL_COOLDOWN_SECONDS = 60

_ROTATING_TOKEN_PREFIX = "xoxe."

RECONNECT_MESSAGE = (
    "Slack no longer accepts PipesHub's saved sign-in for this connector, so its access "
    "could not be renewed. The sign-in was revoked, or the Slack app was reinstalled or "
    "removed. Reconnect Slack from the connector's settings to resume syncing."
)
PASTED_ROTATING_TOKEN_MESSAGE = (
    "The Slack token pasted into this connector has expired. It comes from a Slack app "
    "with token rotation turned on, and a pasted token can't be renewed automatically. "
    "Turn off token rotation in the Slack app's OAuth & Permissions page and paste the new "
    "token, or connect with Slack sign-in (OAuth) where the connector offers it."
)

TokenSource = Callable[[], Awaitable[tuple[dict[str, Any], str]]]


def _datasource_for(external_client: SlackClient, token: str) -> SlackDataSource:
    client = external_client.get_client()
    if getattr(client, "get_token", lambda: None)() != token and hasattr(client, "set_token"):
        client.set_token(token)
    return SlackDataSource(external_client)


class RenewingSlackDataSource:
    """A ``SlackDataSource`` that renews the token and retries once when Slack refuses it."""

    def __init__(
        self,
        datasource: SlackDataSource,
        token: str,
        renewal: "SlackTokenRenewal",
        rebuild: Callable[[], Awaitable[SlackDataSource]],
    ) -> None:
        self._datasource = datasource
        self._token = token
        self._renewal = renewal
        self._rebuild = rebuild

    def __getattr__(self, name: str) -> Any:  # noqa: ANN401 - proxies any datasource member
        member = getattr(self._datasource, name)
        if not inspect.iscoroutinefunction(member):
            return member

        async def call(*args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
            response = await member(*args, **kwargs)
            if getattr(response, "success", True) is not False:
                return response
            error = getattr(response, "error", None)
            if error not in RENEWABLE_ERRORS or not await self._renewal.renew(self._token, error):
                return response
            fresh = await self._rebuild()
            return await getattr(fresh, name)(*args, **kwargs)

        return call


class SlackTokenRenewal:
    """Renews one Slack connector's rotating token through the shared refresh service."""

    def __init__(
        self,
        connector_id: str,
        connector_type: str,
        config_service: ConfigurationService,
        logger: logging.Logger,
    ) -> None:
        self._connector_id = connector_id
        self._connector_type = connector_type
        self._config_service = config_service
        self._logger = logger
        # A refresh token Slack has refused is not sent again, so a sync with
        # hundreds of calls makes one attempt rather than one per call.
        self._rejected_refresh_token: str | None = None
        self._explained_token: str | None = None
        self._last_renewed_at: float | None = None

    async def datasource(self, external_client: SlackClient, current_token: TokenSource) -> RenewingSlackDataSource:
        """A datasource for the connector's current token, renewed first if it is about to expire."""
        config, token = await current_token()
        if self._expiring(config, token) and await self.renew(token, "token_expiring"):
            config, token = await current_token()

        async def rebuild() -> SlackDataSource:
            _, fresh_token = await current_token()
            return _datasource_for(external_client, fresh_token)

        return RenewingSlackDataSource(_datasource_for(external_client, token), token, self, rebuild)

    @staticmethod
    def _expiring(config: dict[str, Any], token: str) -> bool:
        credentials = config.get("credentials") or {}
        if token != credentials.get("access_token") or not credentials.get("refresh_token"):
            return False
        try:
            expires_at = OAuthToken.from_dict(credentials).expires_at_epoch
        except (TypeError, ValueError):
            return False
        if expires_at is None:
            return False
        return time.time() + RENEW_BEFORE_EXPIRY.total_seconds() >= expires_at

    async def renew(self, token_in_use: str, reason: str) -> bool:
        """Get a newer access token than ``token_in_use``. True when there is one to use."""
        if not token_in_use.startswith(_ROTATING_TOKEN_PREFIX):
            return False
        config = await self._config_service.get_config(
            f"/services/connectors/{self._connector_id}/config"
        ) or {}
        auth = config.get("auth") or {}
        credentials = config.get("credentials") or {}
        refresh_token = credentials.get("refresh_token")

        pasted = token_in_use in (auth.get("apiToken"), auth.get("accessToken"))
        if pasted or not refresh_token:
            self._explain_unrenewable(token_in_use, reason)
            return False

        stored_token = credentials.get("access_token")
        if stored_token and stored_token != token_in_use:
            return True
        if refresh_token == self._rejected_refresh_token:
            return False
        if reason == "invalid_auth" and self._renewed_recently():
            return False

        from app.connectors.core.base.token_service.startup_service import (
            startup_service,
        )
        refresh_service = startup_service.get_token_refresh_service()
        if refresh_service is None:
            self._logger.warning("Slack token refresh service is not running; cannot renew the Slack token")
            return False

        try:
            await refresh_service.refresh_now(self._connector_id, self._connector_type, refresh_token)
        except RefreshTokenInvalidError:
            self._rejected_refresh_token = refresh_token
            self._logger.error(RECONNECT_MESSAGE)
            return False
        except Exception as exc:
            self._logger.warning("Could not renew the Slack token (%s); the next call will try again", exc)
            return False
        self._last_renewed_at = time.monotonic()
        self._logger.info("Renewed the Slack token for connector %s (%s)", self._connector_id, reason)
        return True

    def _renewed_recently(self) -> bool:
        return (
            self._last_renewed_at is not None
            and time.monotonic() - self._last_renewed_at < RENEWAL_COOLDOWN_SECONDS
        )

    def _explain_unrenewable(self, token: str, reason: str) -> None:
        if reason != "token_expired":
            return
        if token == self._explained_token:
            return
        self._explained_token = token
        self._logger.error(PASTED_ROTATING_TOKEN_MESSAGE)
