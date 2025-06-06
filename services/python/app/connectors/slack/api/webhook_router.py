"""Slack webhook router for handling incoming Slack events"""

from fastapi import APIRouter, Request, HTTPException, BackgroundTasks
from app.connectors.api.middleware import WebhookAuthVerifier
from app.config.utils.named_constants.arangodb_constants import AccountType
from app.utils.logger import create_logger
import json

logger = create_logger("slack_webhook_router")
router = APIRouter()

async def get_slack_webhook_handler(request: Request):
    """Get appropriate Slack webhook handler based on account type"""
    try:
        container = request.app.container
        config_service = container.config_service()
        
        # Get account type from config
        account_type = await config_service.get_account_type()
        
        # Get org_id and user_id from config
        org_id = await config_service.get_org_id()
        user_id = await config_service.get_user_id() if account_type != AccountType.ENTERPRISE.value else None
        
        # Get appropriate handler
        if account_type == AccountType.ENTERPRISE.value:
            handler = container.enterprise_slack_webhook_handler()
        else:
            handler = container.individual_slack_webhook_handler()
            
        # Ensure handler is initialized
        if not handler:
            logger.error("Failed to get Slack webhook handler")
            return None
            
        # Initialize handler if not already initialized
        if not handler.signing_secret:
            if account_type == AccountType.ENTERPRISE.value:
                if not await handler.initialize(org_id=org_id):
                    logger.error("Failed to initialize enterprise Slack webhook handler")
                    return None
            else:
                if not await handler.initialize(org_id=org_id, user_id=user_id):
                    logger.error("Failed to initialize individual Slack webhook handler")
                    return None
            
        return handler
            
    except Exception as e:
        logger.error(f"Failed to get Slack webhook handler: {str(e)}")
        return None

@router.post("/slack/webhook")
async def handle_slack_webhook(request: Request, background_tasks: BackgroundTasks):
    """Handle incoming webhook notifications from Slack"""
    try:
        # Get webhook handler
        handler = await get_slack_webhook_handler(request)
        if not handler:
            raise HTTPException(
                status_code=500,
                detail="Webhook handler not available"
            )

        # Get request body
        body = await request.json()
        
        # Process notification
        success = await handler.process_notification(
            dict(request.headers),
            body
        )
        
        if not success:
            raise HTTPException(
                status_code=400,
                detail="Failed to process webhook notification"
            )
            
        return {"status": "accepted"}
        
    except json.JSONDecodeError:
        logger.error("Invalid JSON in request body")
        raise HTTPException(status_code=400, detail="Invalid JSON")
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 