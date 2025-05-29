import logging
from typing import Optional, Dict
import aiohttp
import jwt
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from app.config.configuration_service import (
    DefaultEndpoints,
    config_node_constants,
    TokenScopes,
)

class SlackTokenHandler:
    def __init__(self, logger, config_service):
        self.logger = logger
        self.config_service = config_service

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, Exception)),
        reraise=True,
    )
    async def get_bot_config(self, org_id=None, user_id=None):
        """
        Fetch Slack bot token and signing secret from Node.js config manager using JWT auth.
        If org_id/user_id are provided, can be extended for per-org/user configs.
        """
        try:
            # Prepare payload for credentials API (always include scopes)
            payload = {"scopes": [TokenScopes.FETCH_CONFIG.value]}
            if org_id:
                payload["orgId"] = org_id
            if user_id:
                payload["userId"] = user_id

            # JWT signing
            secret_keys = await self.config_service.get_config(
                config_node_constants.SECRET_KEYS.value
            )
            scoped_jwt_secret = secret_keys.get("scopedJwtSecret")
            jwt_token = jwt.encode(payload, scoped_jwt_secret, algorithm="HS256")
            headers = {"Authorization": f"Bearer {jwt_token}"}

            # Get endpoints
            endpoints = await self.config_service.get_config(
                config_node_constants.ENDPOINTS.value
            )
            nodejs_endpoint = endpoints.get("cm", {}).get("endpoint", DefaultEndpoints.NODEJS_ENDPOINT.value)

            # The route for Slack credentials (customize as needed)
            slack_route = "/api/v1/configurationManager/internal/connectors/slackCredentials"

            # Fetch credentials from API
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{nodejs_endpoint}{slack_route}",
                    json=payload,
                    headers=headers,
                ) as response:
                    if response.status != 200:
                        error_json = await response.json()
                        self.logger.error(f"Slack bot credential fetch failed: {error_json}")
                        raise Exception(
                            f"Failed to fetch Slack bot credentials: {error_json}"
                        )
                    creds_data = await response.json()
                    self.logger.info("🚀 Fetched Slack bot config: %s", creds_data)

            bot_token = creds_data.get("botToken")
            signing_secret = creds_data.get("signingSecret")
            if not bot_token or not signing_secret:
                self.logger.error(f"Slack bot token or signing secret missing in response: {creds_data}")
                raise Exception(f"Slack bot token or signing secret missing in response: {creds_data}")

            return {
                "bot_token": bot_token,
                "signing_secret": signing_secret,
            }
        except Exception as e:
            self.logger.error(f"❌ Failed to fetch Slack bot config: {str(e)}")
            return None

    async def get_bot_token(self, org_id=None, user_id=None):
        config = await self.get_bot_config(org_id, user_id)
        return config["bot_token"] if config else None

    async def get_signing_secret(self, org_id=None, user_id=None):
        config = await self.get_bot_config(org_id, user_id)
        return config["signing_secret"] if config else None 