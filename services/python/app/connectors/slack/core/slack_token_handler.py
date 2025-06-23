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
            # Log attempt
            self.logger.info("🔄 Attempting to fetch Slack bot config...")
            if org_id:
                self.logger.info("🔍 Using org_id: %s", org_id)
            if user_id:
                self.logger.info("🔍 Using user_id: %s", user_id)

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
            if not scoped_jwt_secret:
                self.logger.error("❌ JWT secret not found in configuration")
                raise Exception("JWT secret not found in configuration")
                
            jwt_token = jwt.encode(payload, scoped_jwt_secret, algorithm="HS256")
            headers = {"Authorization": f"Bearer {jwt_token}"}

            # Get endpoints
            endpoints = await self.config_service.get_config(
                config_node_constants.ENDPOINTS.value
            )
            nodejs_endpoint = endpoints.get("cm", {}).get("endpoint", DefaultEndpoints.NODEJS_ENDPOINT.value)
            if not nodejs_endpoint:
                self.logger.error("❌ Node.js endpoint not found in configuration")
                raise Exception("Node.js endpoint not found in configuration")

            # The route for Slack credentials
            slack_route = "/api/v1/configurationManager/internal/connectors/slackCredentials"
            
            # Build query params
            params = {"scopes": TokenScopes.FETCH_CONFIG.value}
            if org_id:
                params["orgId"] = org_id
            if user_id:
                params["userId"] = user_id

            # Log request details
            self.logger.info("📤 Making request to: %s%s", nodejs_endpoint, slack_route)
            self.logger.debug("Request params: %s", params)

            # Fetch credentials from API
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{nodejs_endpoint}{slack_route}",
                    params=params,
                    headers=headers,
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        self.logger.error("❌ Slack bot credential fetch failed with status %d: %s", response.status, error_text)
                        raise Exception(f"Failed to fetch Slack bot credentials: {error_text}")
                        
                    creds_data = await response.json()
                    self.logger.debug("📥 Raw credentials response: %s", creds_data)

                    # Extract credentials from response
                    bot_token = creds_data.get("botToken") or creds_data.get("bot_token")
                    signing_secret = creds_data.get("signingSecret") or creds_data.get("signing_secret")

                    if not bot_token or not signing_secret:
                        if not creds_data:
                            self.logger.error("❌ Empty credentials response received")
                            raise Exception("Empty credentials response received")
                        else:
                            self.logger.error("❌ Missing required credentials in response: %s", creds_data)
                            raise Exception(f"Missing required credentials in response: {creds_data}")

                    self.logger.info("✅ Successfully fetched Slack bot credentials")
                    return {
                        "bot_token": bot_token,
                        "signing_secret": signing_secret,
                    }

        except Exception as e:
            self.logger.error("❌ Failed to fetch Slack bot config: %s", str(e))
            raise  # Re-raise the exception instead of returning None

    async def get_bot_token(self, org_id=None, user_id=None):
        config = await self.get_bot_config(org_id, user_id)
        return config["bot_token"] if config else None

    async def get_signing_secret(self, org_id=None, user_id=None):
        config = await self.get_bot_config(org_id, user_id)
        return config["signing_secret"] if config else None 