"""Slack webhook handler for processing Slack events"""

import logging
import asyncio
import json
import hmac
import hashlib
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Set
from datetime import datetime

from app.config.configuration_service import ConfigurationService, WebhookConfig
from app.config.utils.named_constants.arangodb_constants import (
    CollectionNames,
    RecordTypes,
    ProgressStatus,
    EntityType,
    AccountType
)
from app.connectors.slack.core.slack_token_handler import SlackTokenHandler
from app.connectors.slack.handlers.slack_change_handler import SlackChangeHandler

logger = logging.getLogger(__name__)

class AbstractSlackWebhookHandler(ABC):
    """Abstract base class for Slack webhook handlers"""

    def __init__(
        self,
        logger,
        config: ConfigurationService,
        arango_service,
        change_handler: SlackChangeHandler
    ):
        self.logger = logger
        self.config = config
        self.arango_service = arango_service
        self.change_handler = change_handler
        self.signing_secret = None
        self.initialized = False
        self.org_id = None
        self.workspace_id = None
        self.token_handler = SlackTokenHandler(logger, config)
        self.processing_lock = asyncio.Lock()
        self.scheduled_task = None

    async def _ensure_signing_secret(self, org_id: Optional[str] = None, user_id: Optional[str] = None) -> bool:
        """Ensure signing secret is available."""
        try:
            if not self.signing_secret:
                self.logger.info("🔄 Fetching signing secret...")
                self.signing_secret = await self.token_handler.get_signing_secret(org_id, user_id)
                if self.signing_secret:
                    self.logger.info("✅ Successfully fetched signing secret")
                else:
                    self.logger.error("❌ Failed to get signing secret - returned None")
            return bool(self.signing_secret)
        except Exception as e:
            self.logger.error(f"❌ Error getting signing secret: {str(e)}")
            return False

    async def initialize(self, org_id: Optional[str] = None, user_id: Optional[str] = None) -> bool:
        """Initialize the webhook handler."""
        try:
            self.logger.info("🚀 Initializing Slack webhook handler")
            
            if self.initialized:
                self.logger.info("✅ Webhook handler already initialized")
                return True
                
            if not org_id:
                self.logger.warning("⚠️ No org_id provided for initialization")
                return False
                
            # Ensure we have the signing secret
            if not await self._ensure_signing_secret(org_id, user_id):
                self.logger.error("❌ Failed to get signing secret")
                return False
                
            # Get workspace info
            workspace = await self.arango_service.get_workspace_by_org_id(org_id)
            if not workspace:
                self.logger.error("❌ No workspace found for org %s", org_id)
                return False
                
            self.org_id = org_id
            self.workspace_id = workspace.get("externalId")
            self.initialized = True
            
            self.logger.info("✅ Successfully initialized Slack webhook handler")
            return True
            
        except Exception as e:
            self.logger.error("❌ Failed to initialize webhook handler: %s", str(e))
            self.initialized = False
            return False

    async def verify_slack_request(self, request_body: str, timestamp: str, signature: str, org_id: Optional[str] = None, user_id: Optional[str] = None) -> bool:
        """Verify Slack request signature."""
        try:
            if not self.initialized:
                self.logger.error("❌ Cannot verify request - handler not initialized")
                return False

            # Ensure we have signing secret
            if not await self._ensure_signing_secret(org_id, user_id):
                self.logger.error("❌ Cannot verify request - no signing secret available")
                return False

            # Create signature
            sig_basestring = f"v0:{timestamp}:{request_body}"
            my_signature = 'v0=' + hmac.new(
                self.signing_secret.encode(),
                sig_basestring.encode(),
                hashlib.sha256
            ).hexdigest()

            # Compare signatures
            result = hmac.compare_digest(my_signature, signature)
            if result:
                self.logger.info("✅ Request signature verified successfully")
            else:
                self.logger.error("❌ Request signature verification failed")
            return result
        except Exception as e:
            self.logger.error(f"❌ Error verifying request signature: {str(e)}")
            return False

    async def _log_headers(self, headers: Dict) -> Dict:
        """Log and extract important headers."""
        try:
            # Get important headers
            timestamp = headers.get("x-slack-request-timestamp", "")
            signature = headers.get("x-slack-signature", "")
            team_id = headers.get("x-slack-team-id", "")

            # Log headers for debugging
            self.logger.debug("📥 Received headers: %s", headers)

            # Validate timestamp
            if not timestamp:
                self.logger.error("❌ Missing timestamp header")
                return {}

            # Check timestamp freshness (within 5 minutes)
            ts = int(timestamp)
            if abs(datetime.now().timestamp() - ts) > 300:
                self.logger.error("❌ Request too old (>5 minutes)")
                return {}

            return {
                "timestamp": timestamp,
                "signature": signature,
                "team_id": team_id
            }
        except Exception as e:
            self.logger.error(f"❌ Error processing headers: {str(e)}")
            return {}

    @abstractmethod
    async def process_notification(self, headers: Dict, body: Dict) -> bool:
        """Process incoming webhook notification."""
        pass

    @abstractmethod
    async def _delayed_process_notifications(self):
        """Process notifications after coalescing delay."""
        pass

class IndividualSlackWebhookHandler(AbstractSlackWebhookHandler):
    """Handles webhooks for individual workspace accounts"""

    def __init__(
        self,
        logger,
        config: ConfigurationService,
        arango_service,
        change_handler
    ):
        super().__init__(logger, config, arango_service, change_handler)
        self.pending_notifications: Set[str] = set()

    async def process_notification(self, headers: Dict, body: Dict) -> bool:
        """Process incoming webhook notification for individual workspace"""
        try:
            # Verify handler is initialized
            if not self.signing_secret:
                self.logger.error("Webhook handler not initialized")
                return False

            important_headers = await self._log_headers(headers)
            if not important_headers:
                return True

            # Verify request signature using raw body string
            raw_body = headers.get("raw_body", "")  # Get raw body from headers
            if not raw_body:
                raw_body = json.dumps(body)  # Fallback to JSON string if raw body not provided

            # Verify request signature
            if not await self.verify_slack_request(
                raw_body,
                important_headers["timestamp"],
                important_headers["signature"]
            ):
                self.logger.error("Invalid request signature")
                return False

            # Add to pending notifications
            self.pending_notifications.add(json.dumps({
                "headers": important_headers,
                "body": body
            }))

            # Schedule delayed processing
            if self.scheduled_task and not self.scheduled_task.done():
                self.scheduled_task.cancel()
            self.scheduled_task = asyncio.create_task(
                self._delayed_process_notifications()
            )

            return True

        except Exception as e:
            self.logger.error(f"Error processing individual notification: {str(e)}")
            return False

    async def _delayed_process_notifications(self):
        """Process notifications after coalescing delay"""
        try:
            coalesce_delay = WebhookConfig.COALESCEDELAY.value
            await asyncio.sleep(coalesce_delay)

            async with self.processing_lock:
                if not self.pending_notifications:
                    return

                # Process notifications
                for notification_json in self.pending_notifications:
                    notification = json.loads(notification_json)
                    await self._process_event(
                        notification["headers"],
                        notification["body"]
                    )

                # Clear processed notifications
                self.pending_notifications.clear()
                self.logger.info("✅ Cleared processed notifications")

        except asyncio.CancelledError:
            self.logger.info("Processing delayed")
        except Exception as e:
            self.logger.error(f"Error processing notifications: {str(e)}")

    async def _process_event(self, headers: Dict, body: Dict):
        """Process a single Slack event"""
        try:
            event_type = body.get("type")
            
            # Handle URL verification
            if event_type == "url_verification":
                return {"challenge": body.get("challenge")}

            # Handle events
            if event_type == "event_callback":
                event = body.get("event", {})
                team_id = body.get("team_id")
                
                # Get org_id from workspace
                workspace = await self.arango_service.get_workspace_by_team_id(team_id)
                if not workspace:
                    self.logger.error(f"Workspace not found for team_id: {team_id}")
                    return
                    
                org_id = workspace.get("orgId")
                if not org_id:
                    self.logger.error(f"No orgId found for workspace with team_id: {team_id}")
                    return
                    
                self.logger.info(f"Processing event for org_id: {org_id}, event type: {event.get('type')}")
                await self.change_handler.handle_event(event, org_id)

        except Exception as e:
            self.logger.error(f"Error processing event: {str(e)}")

class EnterpriseSlackWebhookHandler(AbstractSlackWebhookHandler):
    """Handles webhooks for enterprise grid workspaces"""

    def __init__(
        self,
        logger,
        config: ConfigurationService,
        arango_service,
        change_handler : SlackChangeHandler,
        slack_admin_service
    ):
        super().__init__(logger, config, arango_service, change_handler)
        self.slack_admin_service = slack_admin_service
        self.pending_notifications: Set[str] = set()

    async def initialize(self, org_id: Optional[str] = None) -> bool:
        """Initialize enterprise webhook handler."""
        try:
            # Initialize base handler first with org context
            if not await super().initialize(org_id):
                return False
                
            # Verify admin service is initialized
            if not self.slack_admin_service:
                self.logger.error("Slack admin service not properly initialized")
                return False
                
            return True
        except Exception as e:
            self.logger.error(f"Error initializing enterprise webhook handler: {str(e)}")
            return False

    async def process_notification(self, headers: Dict, body: Dict) -> bool:
        """Process incoming webhook notification for enterprise grid"""
        try:
            # Verify handler is initialized
            if not self.signing_secret:
                self.logger.error("Webhook handler not initialized")
                return False

            important_headers = await self._log_headers(headers)
            if not important_headers:
                return True

            # Verify request signature using raw body string
            raw_body = headers.get("raw_body", "")  # Get raw body from headers
            if not raw_body:
                raw_body = json.dumps(body)  # Fallback to JSON string if raw body not provided

            # Verify request signature
            if not await self.verify_slack_request(
                raw_body,
                important_headers["timestamp"],
                important_headers["signature"]
            ):
                self.logger.error("Invalid request signature")
                return False

            # Add to pending notifications
            self.pending_notifications.add(json.dumps({
                "headers": important_headers,
                "body": body
            }))

            # Schedule delayed processing
            if self.scheduled_task and not self.scheduled_task.done():
                self.scheduled_task.cancel()
            self.scheduled_task = asyncio.create_task(
                self._delayed_process_notifications()
            )

            return True

        except Exception as e:
            self.logger.error(f"Error processing enterprise notification: {str(e)}")
            return False

    async def _delayed_process_notifications(self):
        """Process notifications after coalescing delay"""
        try:
            coalesce_delay = WebhookConfig.COALESCEDELAY.value
            await asyncio.sleep(coalesce_delay)

            async with self.processing_lock:
                if not self.pending_notifications:
                    return

                # Group notifications by team
                team_notifications = {}
                for notification_json in self.pending_notifications:
                    notification = json.loads(notification_json)
                    team_id = notification["headers"]["team_id"]
                    if team_id not in team_notifications:
                        team_notifications[team_id] = []
                    team_notifications[team_id].append(notification)

                # Process notifications by team
                for team_id, notifications in team_notifications.items():
                    await self._process_team_events(team_id, notifications)

                # Clear processed notifications
                self.pending_notifications.clear()
                self.logger.info("✅ Cleared processed notifications")

        except asyncio.CancelledError:
            self.logger.info("Processing delayed")
        except Exception as e:
            self.logger.error(f"Error processing notifications: {str(e)}")

    async def _process_team_events(self, team_id: str, notifications: list):
        """Process events for a specific team"""
        try:
            self.logger.info(f"Processing {len(notifications)} events for team_id: {team_id}")
            
            for notification in notifications:
                headers = notification["headers"]
                body = notification["body"]
                
                # Process each event using the common _process_event method
                await self._process_event(headers, body)

        except Exception as e:
            self.logger.error(f"Error processing team events: {str(e)}")

    async def _process_event(self, headers: Dict, body: Dict):
        """Process a single Slack event"""
        try:
            event_type = body.get("type")
            self.logger.info(f"Processing event of type: {event_type}")
            
            # Handle URL verification
            if event_type == "url_verification":
                self.logger.info("Handling URL verification challenge")
                return {"challenge": body.get("challenge")}

            # Handle events
            if event_type == "event_callback":
                event = body.get("event", {})
                team_id = body.get("team_id")
                
                if not team_id:
                    self.logger.error("No team_id found in event data")
                    return
                
                # Get org_id from workspace
                workspace = await self.arango_service.get_workspace_by_team_id(team_id)
                if not workspace:
                    self.logger.error(f"Workspace not found for team_id: {team_id}")
                    return
                    
                org_id = workspace.get("orgId")
                if not org_id:
                    self.logger.error(f"No orgId found for workspace with team_id: {team_id}")
                    return
                    
                self.logger.info(f"Processing event for org_id: {org_id}, team_id: {team_id}, event type: {event.get('type')}")
                await self.change_handler.handle_event(event, org_id)
                self.logger.info(f"Successfully processed event for org_id: {org_id}, event type: {event.get('type')}")

        except Exception as e:
            self.logger.error(f"Error processing event: {str(e)}", exc_info=True) 