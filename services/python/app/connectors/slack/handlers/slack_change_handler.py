import logging
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime
import uuid

from app.config.configuration_service import ConfigurationService
from app.config.utils.named_constants.arangodb_constants import (
    CollectionNames,
    RecordTypes,
    MessageType,
    EntityType,
    AccountType,
    ProgressStatus,
    RecordRelations,
    OriginTypes,
    Connectors,
    GroupType
)
from app.utils.time_conversion import get_epoch_timestamp_in_ms

logger = logging.getLogger(__name__)

class SlackChangeHandler:
    """Handler for processing Slack events and changes"""

    def __init__(
        self,
        logger,
        config_service: ConfigurationService,
        arango_service
    ):
        """Initialize the Slack change handler.

        Args:
            logger: Logger instance
            config_service: Configuration service instance
            arango_service: ArangoDB service instance
        """
        self.logger = logger
        self.config_service = config_service
        self.arango_service = arango_service

    async def handle_event(self, event: Dict[str, Any], org_id: str = None) -> bool:
        """Handle different types of Slack events.
        
        Args:
            event: The Slack event to process
            org_id: Optional organization ID
            
        Returns:
            bool: True if event was handled successfully, False otherwise
        """
        try:
            if not org_id:
                self.logger.error("No org_id provided for event handling")
                return False
                
            event_type = event.get('type')
            self.logger.info(f"Handling Slack event type: {event_type} for org_id: {org_id}")
            
            # Handle URL verification
            if event_type == 'url_verification':
                self.logger.info("Processing URL verification challenge")
                return {"challenge": event.get("challenge")}
                
            # Handle message events and their subtypes
            if event_type == 'message':
                self.logger.info(f"Processing message event for org_id: {org_id}")
                return await self._handle_message_event(event, org_id)
                
            # Handle channel-related events
            elif event_type in [
                'channel_created', 'channel_rename', 'channel_archive',
                'channel_unarchive', 'channel_deleted', 'channel_shared',
                'channel_unshared'
            ]:
                self.logger.info(f"Processing channel event: {event_type} for org_id: {org_id}")
                return await self._handle_channel_event(event, org_id)
                
            # Handle membership events
            elif event_type in [
                'member_joined_channel', 'member_left_channel',
                'group_joined', 'group_left'
            ]:
                self.logger.info(f"Processing membership event: {event_type} for org_id: {org_id}")
                return await self._handle_channel_membership_event(event, org_id)
                
            # Handle user events
            elif event_type in [
                'user_change', 'team_join', 'user_profile_changed',
                'user_status_changed'
            ]:
                self.logger.info(f"Processing user event: {event_type} for org_id: {org_id}")
                return await self._handle_user_event(event, org_id)
                
            # Handle file events
            elif event_type in [
                'file_created', 'file_shared', 'file_unshared',
                'file_public', 'file_deleted'
            ]:
                self.logger.info(f"Processing file event: {event_type} for org_id: {org_id}")
                return await self._handle_file_event(event, org_id)

            self.logger.warning(f"Unhandled event type: {event_type} for org_id: {org_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error handling Slack event for org_id {org_id}: {str(e)}")
            return False

    async def _handle_message_event(self, event: Dict[str, Any], org_id: str) -> bool:
        """Handle message events including thread messages"""
        try:
            # Skip message_changed and message_deleted events for now
            if event.get('subtype') in ['message_changed', 'message_deleted']:
                return True

            # Begin transaction
            txn = self.arango_service.db.begin_transaction(
                write=[
                    CollectionNames.RECORDS.value,
                    CollectionNames.SLACK_MESSAGE_METADATA.value,
                    CollectionNames.BELONGS_TO_SLACK_CHANNEL.value,
                    CollectionNames.RECORD_RELATIONS.value,
                    CollectionNames.SLACK_FILE_TO_ATTACHMENT_METADATA.value,
                    CollectionNames.SLACK_ATTACHMENT_METADATA.value
                ]
            )

            try:
                timestamp = get_epoch_timestamp_in_ms()
                message_key = str(uuid.uuid4())

                # Get channel info
                channel_id = event.get('channel')
                channel = await self.arango_service.get_channel_by_slack_id(channel_id, org_id, txn)
                if not channel:
                    # Channel doesn't exist, create it
                    workspace = await self.arango_service.get_workspace_by_org_id(org_id)
                    if not workspace:
                        self.logger.error(f"Workspace not found for org_id: {org_id}")
                        return False
                        
                    channel_key = str(uuid.uuid4())
                    channel_record = {
                        "_key": channel_key,
                        "orgId": org_id,
                        "groupName": "New Channel",  # Will be updated when we get channel details
                        "externalGroupId": channel_id,
                        "groupType": GroupType.SLACK_CHANNEL.value,
                        "connectorName": Connectors.SLACK.value,
                        "createdAtTimestamp": timestamp,
                        "updatedAtTimestamp": timestamp,
                        "isDeletedAtSource": False,
                        "sourceCreatedAtTimestamp": timestamp,
                        "sourceLastModifiedTimestamp": timestamp,
                        "syncState": ProgressStatus.NOT_STARTED.value,
                    }
                    
                    await self.arango_service.batch_upsert_nodes(
                        [channel_record],
                        CollectionNames.RECORD_GROUPS.value,
                        transaction=txn
                    )
                    
                    # Create workspace relationship
                    workspace_edge = {
                        "_from": f"{CollectionNames.RECORD_GROUPS.value}/{channel_key}",
                        "_to": f"{CollectionNames.SLACK_WORKSPACES.value}/{workspace['_key']}",
                        "entityType": "WORKSPACE",
                        "createdAtTimestamp": timestamp,
                        "updatedAtTimestamp": timestamp
                    }
                    await self.arango_service.batch_create_edges(
                        [workspace_edge],
                        CollectionNames.BELONGS_TO_SLACK_WORKSPACE.value,
                        transaction=txn
                    )
                else:
                    channel_key = channel['_key']

                # Create message record
                message_record = {
                    "_key": message_key,
                    "orgId": org_id,
                    "recordName": f"Slack_Message_{event.get('ts', '')}",
                    "externalRecordId": event.get('ts'),
                    "recordType": RecordTypes.MESSAGE.value,
                    "version": 0,
                    "origin": OriginTypes.CONNECTOR.value,
                    "connectorName": Connectors.SLACK.value,
                    "virtualRecordId": None,
                    "isLatestVersion": True,
                    "isDirty": False,
                    "createdAtTimestamp": timestamp,
                    "updatedAtTimestamp": timestamp,
                    "lastSyncTimestamp": timestamp,
                    "sourceCreatedAtTimestamp": int(float(event.get('ts', 0)) * 1000),
                    "sourceLastModifiedTimestamp": int(float(event.get('edited', {}).get('ts', 0)) * 1000) if event.get('edited') else int(float(event.get('ts', 0)) * 1000),
                    "isDeleted": False,
                    "isArchived": False,
                    "reason": None,
                    "lastIndexTimestamp": None,
                    "lastExtractionTimestamp": None,
                    "indexingStatus": ProgressStatus.NOT_STARTED.value,
                    "extractionStatus": ProgressStatus.NOT_STARTED.value,
                }

                # Create message metadata
                message_metadata = {
                    "_key": message_key,
                    "orgId": org_id,
                    "slackTs": event.get('ts'),
                    "threadTs": event.get('thread_ts'),
                    "channelId": channel_id,
                    "userId": event.get('user'),
                    "messageType": MessageType.THREAD_MESSAGE.value if event.get('thread_ts') else MessageType.ROOT_MESSAGE.value,
                    "text": event.get('text', ''),
                    "replyCount": event.get('reply_count', 0),
                    "replyUsersCount": event.get('reply_users_count', 0),
                    "replyUsers": event.get('reply_users', []),
                    "hasFiles": bool(event.get('files')),
                    "fileCount": len(event.get('files', [])),
                    "botId": event.get('bot_id'),
                    "mentionedUsers": self._extract_mentions(event),
                    "links": self._extract_links(event)
                }

                # Store message record and metadata
                await self.arango_service.batch_upsert_nodes(
                    [message_record],
                    CollectionNames.RECORDS.value,
                    transaction=txn
                )
                await self.arango_service.batch_upsert_nodes(
                    [message_metadata],
                    CollectionNames.SLACK_MESSAGE_METADATA.value,
                    transaction=txn
                )

                # Create edges list for batch creation
                edges = []

                # Message to channel edge
                edges.append({
                    "_from": f"{CollectionNames.RECORDS.value}/{message_key}",
                    "_to": f"{CollectionNames.RECORD_GROUPS.value}/{channel_key}",
                    "entityType": EntityType.SLACK_CHANNEL.value,
                    "createdAtTimestamp": timestamp,
                    "updatedAtTimestamp": timestamp
                })

                # Handle thread relationships
                thread_ts = event.get('thread_ts')
                if thread_ts:
                    # This is a thread reply, find parent message
                    parent_key = await self._find_or_fetch_parent_message(
                        thread_ts,
                        channel_id,
                        org_id,
                        txn
                    )
                    if parent_key:
                        # Create parent-child relationship
                        edges.append({
                            "_from": f"{CollectionNames.RECORDS.value}/{parent_key}",
                            "_to": f"{CollectionNames.RECORDS.value}/{message_key}",
                            "relationshipType": RecordRelations.PARENT_CHILD.value,
                            "createdAtTimestamp": timestamp,
                            "updatedAtTimestamp": timestamp
                        })

                # Create all edges
                if edges:
                    await self.arango_service.batch_create_edges(
                        edges,
                        CollectionNames.RECORD_RELATIONS.value,
                        transaction=txn
                    )

                # Process files if any
                if event.get('files'):
                    await self._process_message_files(
                        event.get('files'),
                        message_key,
                        channel_key,
                        org_id,
                        txn
                    )

                # Commit transaction
                await asyncio.to_thread(lambda: txn.commit_transaction())
                return True

            except Exception as e:
                if hasattr(txn, 'abort_transaction'):
                    await asyncio.to_thread(lambda: txn.abort_transaction())
                raise e

        except Exception as e:
            self.logger.error(f"Error handling message event: {str(e)}")
            return False

    async def _process_message_files(
        self,
        files: List[Dict[str, Any]],
        message_key: str,
        channel_key: str,
        org_id: str,
        txn
    ) -> List[Dict[str, Any]]:
        """Process files attached to a message and create proper relationships"""
        try:
            processed_files = []
            timestamp = get_epoch_timestamp_in_ms()

            for file in files:
                if not file:
                    continue

                file_key = str(uuid.uuid4())

                # Create file record
                file_record = {
                    "_key": file_key,
                    "orgId": org_id,
                    "recordName": file.get('name', 'Untitled'),
                    "externalRecordId": file.get('id'),
                    "recordType": RecordTypes.FILE.value,
                    "version": 0,
                    "origin": OriginTypes.CONNECTOR.value,
                    "connectorName": Connectors.SLACK.value,
                    "virtualRecordId": None,
                    "isLatestVersion": True,
                    "isDirty": False,
                    "createdAtTimestamp": timestamp,
                    "updatedAtTimestamp": timestamp,
                    "lastSyncTimestamp": timestamp,
                    "sourceCreatedAtTimestamp": int(float(file.get('created', 0)) * 1000),
                    "sourceLastModifiedTimestamp": int(float(file.get('updated', 0)) * 1000),
                    "isDeleted": False,
                    "isArchived": False,
                    "reason": None,
                    "lastIndexTimestamp": None,
                    "lastExtractionTimestamp": None,
                    "indexingStatus": ProgressStatus.NOT_STARTED.value,
                    "extractionStatus": ProgressStatus.NOT_STARTED.value,
                }

                # Create file metadata
                file_metadata = {
                    "_key": file_key,
                    "orgId": org_id,
                    "slackFileId": file.get('id'),
                    "name": file.get('name', ''),
                    "extension": file.get('filetype'),
                    "mimeType": file.get('mimetype'),
                    "sizeInBytes": file.get('size', 0),
                    "channelId": channel_key,
                    "uploadedBy": file.get('user'),
                    "sourceUrls": {
                        "privateUrl": file.get('url_private'),
                        "downloadUrl": file.get('url_private_download'),
                        "publicPermalink": file.get('permalink_public')
                    },
                    "isPublic": file.get('is_public', False),
                    "isEditable": file.get('is_editable', False),
                    "permalink": file.get('permalink'),
                    "richPreview": file.get('has_rich_preview', False)
                }

                # Store records
                await self.arango_service.batch_upsert_nodes(
                    [file_record],
                    CollectionNames.RECORDS.value,
                    transaction=txn
                )

                await self.arango_service.batch_upsert_nodes(
                    [file_metadata],
                    CollectionNames.SLACK_ATTACHMENT_METADATA.value,
                    transaction=txn
                )

                # Create edges
                edges = []

                # File to channel edge
                edges.append({
                    "_from": f"{CollectionNames.RECORDS.value}/{file_key}",
                    "_to": f"{CollectionNames.RECORD_GROUPS.value}/{channel_key}",
                    "entityType": EntityType.SLACK_CHANNEL.value,
                    "createdAtTimestamp": timestamp,
                    "updatedAtTimestamp": timestamp
                })

                # File to metadata edge
                edges.append({
                    "_from": f"{CollectionNames.RECORDS.value}/{file_key}",
                    "_to": f"{CollectionNames.SLACK_ATTACHMENT_METADATA.value}/{file_key}",
                    "createdAtTimestamp": timestamp,
                    "updatedAtTimestamp": timestamp
                })

                # Message to file edge (attachment relationship)
                edges.append({
                    "_from": f"{CollectionNames.RECORDS.value}/{message_key}",
                    "_to": f"{CollectionNames.RECORDS.value}/{file_key}",
                    "relationshipType": RecordRelations.ATTACHMENT.value,
                    "createdAtTimestamp": timestamp,
                    "updatedAtTimestamp": timestamp
                })

                # Create all edges
                await self.arango_service.batch_create_edges(
                    edges,
                    CollectionNames.RECORD_RELATIONS.value,
                    transaction=txn
                )

                processed_files.append(file_record)

            return processed_files

        except Exception as e:
            self.logger.error(f"Error processing message files: {str(e)}")
            if txn:
                raise
            return []

    async def _find_or_fetch_parent_message(
        self,
        thread_ts: str,
        channel_id: str,
        org_id: str,
        txn
    ) -> Optional[str]:
        """Find existing parent message or fetch and store it"""
        try:
            # Check if parent exists in database
            parent = await self.arango_service.get_message_by_slack_ts(thread_ts, org_id, txn)
            if parent:
                return parent['_key']
            return None

        except Exception as e:
            self.logger.error(f"Error finding parent message: {str(e)}")
            return None

    def _extract_mentions(self, message: Dict[str, Any]) -> List[str]:
        """Extract user mentions from message"""
        mentions = []
        text = message.get('text', '')
        
        import re
        user_mentions = re.findall(r'<@([A-Z0-9]+)>', text)
        mentions.extend(user_mentions)
        
        return mentions

    def _extract_links(self, message: Dict[str, Any]) -> List[str]:
        """Extract links from message"""
        links = []
        text = message.get('text', '')
        
        import re
        url_pattern = r'<(https?://[^>]+)(?:\|[^>]+)?>'
        urls = re.findall(url_pattern, text)
        links.extend(urls)
        
        return links

    async def _handle_channel_event(self, event: Dict[str, Any], org_id: str) -> bool:
        """Handle channel-related events"""
        try:
            event_type = event.get('type')
            channel_data = event.get('channel', {})
            channel_id = channel_data.get('id')
            
            if not channel_id:
                self.logger.error("Channel ID not found in event data")
                return False

            timestamp = get_epoch_timestamp_in_ms()
            
            # Get workspace info for the org
            workspace = await self.arango_service.get_workspace_by_org_id(org_id)
            if not workspace:
                self.logger.error(f"Workspace not found for org_id: {org_id}")
                return False
                
            # Start transaction
            txn = self.arango_service.db.begin_transaction(
                write=[
                    CollectionNames.RECORD_GROUPS.value,
                    CollectionNames.BELONGS_TO_SLACK_WORKSPACE.value,
                    CollectionNames.BELONGS_TO_SLACK_CHANNEL.value,
                    CollectionNames.RECORDS.value
                ]
            )

            try:
                # Check if channel already exists
                existing_channel = await self.arango_service.get_channel_by_slack_id(channel_id, org_id, txn)
                
                if event_type == 'channel_deleted':
                    if existing_channel:
                        # Mark channel as deleted
                        update_fields = {
                            "isDeletedAtSource": True,
                            "updatedAtTimestamp": timestamp,
                            "sourceLastModifiedTimestamp": timestamp
                        }
                        await self.arango_service.update_channel(existing_channel['_key'], update_fields, txn)
                        
                        # Remove all member relationships
                        query = f"""
                        FOR edge IN {CollectionNames.BELONGS_TO_SLACK_CHANNEL.value}
                            FILTER edge._to == @channel_id
                            REMOVE edge IN {CollectionNames.BELONGS_TO_SLACK_CHANNEL.value}
                        """
                        bind_vars = {"channel_id": f"{CollectionNames.RECORD_GROUPS.value}/{existing_channel['_key']}"}
                        await asyncio.to_thread(lambda: txn.execute(query, bind_vars=bind_vars))

                elif event_type == 'channel_archive':
                    if existing_channel:
                        update_fields = {
                            "isArchived": True,
                            "updatedAtTimestamp": timestamp,
                            "sourceLastModifiedTimestamp": timestamp
                        }
                        await self.arango_service.update_channel(existing_channel['_key'], update_fields, txn)

                elif event_type == 'channel_unarchive':
                    if existing_channel:
                        update_fields = {
                            "isArchived": False,
                            "updatedAtTimestamp": timestamp,
                            "sourceLastModifiedTimestamp": timestamp
                        }
                        await self.arango_service.update_channel(existing_channel['_key'], update_fields, txn)

                elif event_type in ['channel_created', 'channel_rename']:
                    channel_key = existing_channel['_key'] if existing_channel else str(uuid.uuid4())
                    
                    # Create or update channel record
                    channel_record = {
                        "_key": channel_key,
                        "orgId": org_id,
                        "groupName": channel_data.get('name', 'Unnamed Channel'),
                        "externalGroupId": channel_id,
                        "groupType": GroupType.SLACK_CHANNEL.value,
                        "connectorName": Connectors.SLACK.value,
                        "createdAtTimestamp": existing_channel['createdAtTimestamp'] if existing_channel else timestamp,
                        "updatedAtTimestamp": timestamp,
                        "isDeletedAtSource": False,
                        "isArchived": channel_data.get('is_archived', False),
                        "sourceCreatedAtTimestamp": channel_data.get('created', 0) * 1000,
                        "sourceLastModifiedTimestamp": timestamp,
                        "syncState": existing_channel.get('syncState', ProgressStatus.NOT_STARTED.value) if existing_channel else ProgressStatus.NOT_STARTED.value,
                    }

                    # Store channel
                    await self.arango_service.batch_upsert_nodes(
                        [channel_record],
                        CollectionNames.RECORD_GROUPS.value,
                        transaction=txn
                    )

                    # If new channel, create workspace relationship
                    if not existing_channel:
                        workspace_edge = {
                            "_from": f"{CollectionNames.RECORD_GROUPS.value}/{channel_key}",
                            "_to": f"{CollectionNames.SLACK_WORKSPACES.value}/{workspace['_key']}",
                            "entityType": EntityType.WORKSPACE.value,
                            "createdAtTimestamp": timestamp,
                            "updatedAtTimestamp": timestamp
                        }
                        await self.arango_service.batch_create_edges(
                            [workspace_edge],
                            CollectionNames.BELONGS_TO_SLACK_WORKSPACE.value,
                            transaction=txn
                        )

                # Commit transaction
                await asyncio.to_thread(lambda: txn.commit_transaction())
                return True

            except Exception as e:
                if hasattr(txn, 'abort_transaction'):
                    await asyncio.to_thread(lambda: txn.abort_transaction())
                raise e

        except Exception as e:
            self.logger.error(f"Error handling channel event: {str(e)}")
            return False

    async def _handle_channel_membership_event(self, event: Dict[str, Any], org_id: str) -> bool:
        """Handle channel membership changes"""
        try:
            event_type = event.get('type')
            channel_id = event.get('channel')
            user_id = event.get('user')
            inviter_id = event.get('inviter')  # For invited members
            
            if not channel_id or not user_id:
                self.logger.error("Missing channel_id or user_id in event data")
                return False

            timestamp = get_epoch_timestamp_in_ms()

            # Start transaction
            txn = self.arango_service.db.begin_transaction(
                write=[
                    CollectionNames.BELONGS_TO_SLACK_CHANNEL.value,
                    CollectionNames.RECORD_GROUPS.value,
                    CollectionNames.USERS.value
                ]
            )

            try:
                # Get channel and user records
                channel = await self.arango_service.get_channel_by_slack_id(channel_id, org_id, txn)
                user = await self.arango_service.get_user_by_slack_id(user_id, org_id, txn)

                if not channel or not user:
                    self.logger.error("Channel or user not found")
                    return False

                if event_type in ['member_joined_channel', 'group_joined']:
                    # Check if membership already exists
                    edge_exists = await self.arango_service.check_edge_exists(
                        f"{CollectionNames.USERS.value}/{user['_key']}",
                        f"{CollectionNames.RECORD_GROUPS.value}/{channel['_key']}",
                        CollectionNames.BELONGS_TO_SLACK_CHANNEL.value
                    )

                    if not edge_exists:
                        # Create membership edge
                        edge = {
                            "_from": f"{CollectionNames.USERS.value}/{user['_key']}",
                            "_to": f"{CollectionNames.RECORD_GROUPS.value}/{channel['_key']}",
                            "entityType": EntityType.SLACK_CHANNEL.value,
                            "createdAtTimestamp": timestamp,
                            "updatedAtTimestamp": timestamp,
                            "invitedBy": inviter_id
                        }

                        await self.arango_service.batch_create_edges(
                            [edge],
                            CollectionNames.BELONGS_TO_SLACK_CHANNEL.value,
                            transaction=txn
                        )

                        update_fields = {
                            "updatedAtTimestamp": timestamp
                        }
                        await self.arango_service.update_channel(channel['_key'], update_fields, txn)

                elif event_type in ['member_left_channel', 'group_left']:
                    # Remove membership edge
                    query = f"""
                    FOR edge IN {CollectionNames.BELONGS_TO_SLACK_CHANNEL.value}
                        FILTER edge._from == @from AND edge._to == @to
                        REMOVE edge IN {CollectionNames.BELONGS_TO_SLACK_CHANNEL.value}
                    """
                    bind_vars = {
                        "from": f"{CollectionNames.USERS.value}/{user['_key']}",
                        "to": f"{CollectionNames.RECORD_GROUPS.value}/{channel['_key']}"
                    }
                    await asyncio.to_thread(lambda: txn.execute(query, bind_vars=bind_vars))

                    update_fields = {
                        "updatedAtTimestamp": timestamp
                    }
                    await self.arango_service.update_channel(channel['_key'], update_fields, txn)

                # Commit transaction
                await asyncio.to_thread(lambda: txn.commit_transaction())
                return True

            except Exception as e:
                if hasattr(txn, 'abort_transaction'):
                    await asyncio.to_thread(lambda: txn.abort_transaction())
                raise e

        except Exception as e:
            self.logger.error(f"Error handling channel membership event: {str(e)}")
            return False

    async def _handle_user_event(self, event: Dict[str, Any], org_id: str) -> bool:
        """Handle user events (join team or profile update)"""
        try:
            user_data = event.get('user')
            if not user_data:
                self.logger.error("User data not found in event")
                return False

            timestamp = get_epoch_timestamp_in_ms()
            
            # Get workspace info
            workspace = await self.arango_service.get_workspace_by_org_id(org_id)
            if not workspace:
                self.logger.error(f"Workspace not found for org_id: {org_id}")
                return False

            # Start transaction with all necessary collections
            txn = self.arango_service.db.begin_transaction(
                write=[
                    CollectionNames.USERS.value,
                    CollectionNames.BELONGS_TO.value,
                    CollectionNames.BELONGS_TO_SLACK_CHANNEL.value
                ]
            )

            try:
                # Check if user exists
                existing_user = await self.arango_service.get_user_by_slack_id(user_data.get('id'), org_id, txn)
                
                if existing_user:
                    # Update existing user
                    update_fields = {
                        "email": user_data.get('profile', {}).get('email'),
                        "fullName": user_data.get('profile', {}).get('real_name') or user_data.get('name'),
                        "isActive": not user_data.get('deleted', False),
                        "updatedAtTimestamp": timestamp
                    }
                    await self.arango_service.update_user(existing_user['_key'], update_fields, txn)
                else:
                    # Create new user
                    user_key = str(uuid.uuid4())
                    user_record = {
                        "_key": user_key,
                        "orgId": org_id,
                        "userId": user_data.get('id'),
                        "firstName": user_data.get('profile', {}).get('first_name'),
                        "lastName": user_data.get('profile', {}).get('last_name'),
                        "fullName": user_data.get('profile', {}).get('real_name') or user_data.get('name'),
                        "email": user_data.get('profile', {}).get('email'),
                        "designation": (
                            "Primary Owner" if user_data.get('is_primary_owner') else
                            "Owner" if user_data.get('is_owner') else
                            "Admin" if user_data.get('is_admin') else
                            "Bot" if user_data.get('is_bot') else
                            "Member"
                        ),
                        "isActive": not user_data.get('deleted', False),
                        "createdAtTimestamp": timestamp,
                        "updatedAtTimestamp": timestamp,
                    }

                    # Store user
                    await self.arango_service.batch_upsert_nodes(
                        [user_record],
                        CollectionNames.USERS.value,
                        transaction=txn
                    )

                    # Create workspace membership edge
                    workspace_edge = {
                        "_from": f"{CollectionNames.USERS.value}/{user_key}",
                        "_to": f"{CollectionNames.SLACK_WORKSPACES.value}/{workspace['_key']}",
                        "entityType": EntityType.WORKSPACE.value,
                        "createdAtTimestamp": timestamp,
                        "updatedAtTimestamp": timestamp
                    }
                    
                    await self.arango_service.batch_create_edges(
                        [workspace_edge],
                        CollectionNames.BELONGS_TO.value,
                        transaction=txn
                    )

                # Commit transaction
                await asyncio.to_thread(lambda: txn.commit_transaction())
                return True

            except Exception as e:
                if hasattr(txn, 'abort_transaction'):
                    await asyncio.to_thread(lambda: txn.abort_transaction())
                raise e

        except Exception as e:
            self.logger.error(f"Error handling user event: {str(e)}")
            return False

    async def _handle_file_event(self, event: Dict[str, Any], org_id: str) -> bool:
        """Handle file events"""
        try:
            file_data = event.get('file')
            if not file_data:
                self.logger.error("File data not found in event")
                return False

            timestamp = get_epoch_timestamp_in_ms()
            
            # Get channel info if available
            channel_id = event.get('channel_id')
            channel = None
            if channel_id:
                channel = await self.arango_service.get_channel_by_slack_id(channel_id, org_id)

            # Start transaction with all necessary collections
            txn = self.arango_service.db.begin_transaction(
                write=[
                    CollectionNames.RECORDS.value,
                    CollectionNames.SLACK_ATTACHMENT_METADATA.value,
                    CollectionNames.SLACK_FILE_TO_ATTACHMENT_METADATA.value,
                    CollectionNames.BELONGS_TO_SLACK_CHANNEL.value,
                    CollectionNames.RECORD_RELATIONS.value
                ]
            )

            try:
                # Check if file exists
                existing_file_key = await self.arango_service.get_key_by_external_file_id(file_data.get('id'), txn)
                
                if existing_file_key:
                    # Update existing file record
                    file_update = {
                        "updatedAtTimestamp": timestamp,
                        "lastSyncTimestamp": timestamp,
                        "sourceLastModifiedTimestamp": int(float(file_data.get('updated', 0)) * 1000),
                        "isDeleted": event.get('type') == 'file_deleted',
                        "isDirty": True  # Mark for reindexing since content might have changed
                    }
                    
                    # Update file record
                    await self.arango_service.batch_upsert_nodes(
                        [{
                            "_key": existing_file_key,
                            **file_update
                        }],
                        CollectionNames.RECORDS.value,
                        transaction=txn
                    )
                    
                    # Update metadata
                    metadata_update = {
                        "name": file_data.get('name', ''),
                        "extension": file_data.get('filetype'),
                        "mimeType": file_data.get('mimetype'),
                        "sizeInBytes": file_data.get('size', 0),
                        "uploadedBy": file_data.get('user'),
                        "sourceUrls": {
                            "privateUrl": file_data.get('url_private'),
                            "downloadUrl": file_data.get('url_private_download'),
                            "publicPermalink": file_data.get('permalink_public')
                        }
                    }
                    
                    await self.arango_service.batch_upsert_nodes(
                        [{
                            "_key": existing_file_key,
                            **metadata_update
                        }],
                        CollectionNames.SLACK_ATTACHMENT_METADATA.value,
                        transaction=txn
                    )
                    
                else:
                    # Create new file record
                    file_key = str(uuid.uuid4())
                    file_record = {
                        "_key": file_key,
                        "orgId": org_id,
                        "recordName": file_data.get('name', ''),
                        "externalRecordId": file_data.get('id'),
                        "recordType": RecordTypes.FILE.value,
                        "version": 0,
                        "origin": OriginTypes.CONNECTOR.value,
                        "connectorName": Connectors.SLACK.value,
                        "virtualRecordId": None,
                        "isLatestVersion": True,
                        "isDirty": False,
                        "createdAtTimestamp": timestamp,
                        "updatedAtTimestamp": timestamp,
                        "lastSyncTimestamp": timestamp,
                        "sourceCreatedAtTimestamp": int(float(file_data.get('created', 0)) * 1000),
                        "sourceLastModifiedTimestamp": int(float(file_data.get('updated', 0)) * 1000),
                        "isDeleted": event.get('type') == 'file_deleted',
                        "isArchived": False,
                        "reason": None,
                        "lastIndexTimestamp": None,
                        "lastExtractionTimestamp": None,
                        "indexingStatus": ProgressStatus.NOT_STARTED.value,
                        "extractionStatus": ProgressStatus.NOT_STARTED.value
                    }

                    # Create file metadata
                    file_metadata = {
                        "_key": file_key,
                        "orgId": org_id,
                        "slackFileId": file_data.get('id'),
                        "name": file_data.get('name', ''),
                        "extension": file_data.get('filetype'),
                        "mimeType": file_data.get('mimetype'),
                        "sizeInBytes": file_data.get('size', 0),
                        "channelId": event.get('channel_id'),
                        "uploadedBy": file_data.get('user'),
                        "sourceUrls": {
                            "privateUrl": file_data.get('url_private'),
                            "downloadUrl": file_data.get('url_private_download'),
                            "publicPermalink": file_data.get('permalink_public')
                        },
                        "isPublic": file_data.get('is_public', False),
                        "isEditable": file_data.get('is_editable', False),
                        "permalink": file_data.get('permalink'),
                        "richPreview": file_data.get('has_rich_preview', False)
                    }

                    # Store records
                    await self.arango_service.batch_upsert_nodes(
                        [file_record],
                        CollectionNames.RECORDS.value,
                        transaction=txn
                    )

                    await self.arango_service.batch_upsert_nodes(
                        [file_metadata],
                        CollectionNames.SLACK_ATTACHMENT_METADATA.value,
                        transaction=txn
                    )

                    # Create edges list
                    edges = []

                    # File to metadata edge
                    edges.append({
                        "_from": f"{CollectionNames.RECORDS.value}/{file_key}",
                        "_to": f"{CollectionNames.SLACK_ATTACHMENT_METADATA.value}/{file_key}",
                        "createdAtTimestamp": timestamp,
                        "updatedAtTimestamp": timestamp
                    })

                    # If file is in a channel, create channel relationship
                    if channel:
                        channel_edge = {
                            "_from": f"{CollectionNames.RECORDS.value}/{file_key}",
                            "_to": f"{CollectionNames.RECORD_GROUPS.value}/{channel['_key']}",
                            "entityType": EntityType.SLACK_CHANNEL.value,
                            "createdAtTimestamp": timestamp,
                            "updatedAtTimestamp": timestamp
                        }
                        await self.arango_service.batch_create_edges(
                            [channel_edge],
                            CollectionNames.BELONGS_TO_SLACK_CHANNEL.value,
                            transaction=txn
                        )

                    # Create file to metadata edge
                    await self.arango_service.batch_create_edges(
                        edges,
                        CollectionNames.SLACK_FILE_TO_ATTACHMENT_METADATA.value,
                        transaction=txn
                    )

                # Commit transaction
                await asyncio.to_thread(lambda: txn.commit_transaction())
                return True

            except Exception as e:
                if hasattr(txn, 'abort_transaction'):
                    await asyncio.to_thread(lambda: txn.abort_transaction())
                raise e

        except Exception as e:
            self.logger.error(f"Error handling file event: {str(e)}")
            return False