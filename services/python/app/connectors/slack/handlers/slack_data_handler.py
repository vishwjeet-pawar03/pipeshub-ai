"""Updated Slack Data Handler with corrected thread and message relationships"""

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any

from app.connectors.google.core.arango_service import ArangoService
from app.connectors.core.kafka_service import KafkaService
from app.config.configuration_service import ConfigurationService
from app.config.utils.named_constants.arangodb_constants import (
    CollectionNames,
    Connectors,
    EntityType,
    GroupType,
    ProgressStatus,
    RecordRelations,
    RecordTypes,
    OriginTypes,
    PermissionType,
    MessageType
)
from app.utils.time_conversion import get_epoch_timestamp_in_ms


class SlackDataHandler:
    """Business logic layer for Slack data processing with corrected thread handling"""

    def __init__(
        self,
        logger,
        arango_service: ArangoService,
        slack_client,
        kafka_service: KafkaService,
        config: ConfigurationService,
    ):
        self.logger = logger
        self.arango_service = arango_service
        self.slack_client = slack_client
        self.kafka_service = kafka_service
        self.config = config
        self.batch_size = 100
        self.rate_limit_delay = 1.0

    # ==================== WORKSPACE SYNC METHODS ====================

    async def sync_workspace_data(
        self,
        workspace_id: str,
        org_id: str
    ) -> Dict[str, Any]:
        """Sync complete workspace data including users, channels, and messages"""
        try:
            self.logger.info("🚀 Starting workspace sync for: %s", workspace_id)
            
            sync_stats = {
                "users_processed": 0,
                "channels_processed": 0,
                "messages_processed": 0,
                "files_processed": 0,
                "errors": []
            }

            # Begin transaction for workspace setup
            txn = self.arango_service.db.begin_transaction(
                read=[
                    CollectionNames.USERS.value,
                    CollectionNames.RECORD_GROUPS.value, 
                    CollectionNames.RECORDS.value,
                    CollectionNames.SLACK_WORKSPACES.value,
                    CollectionNames.SLACK_MESSAGE_METADATA.value,
                    CollectionNames.SLACK_ATTACHMENT_METADATA.value,
                ],
                write=[
                    CollectionNames.USERS.value,
                    CollectionNames.RECORD_GROUPS.value,
                    CollectionNames.RECORDS.value,
                    CollectionNames.SLACK_MESSAGE_METADATA.value,
                    CollectionNames.SLACK_ATTACHMENT_METADATA.value,
                    CollectionNames.PERMISSIONS.value,
                    CollectionNames.RECORD_RELATIONS.value,
                    CollectionNames.IS_OF_TYPE.value,
                    CollectionNames.SLACK_WORKSPACES.value,
                    CollectionNames.BELONGS_TO_SLACK_WORKSPACE.value,
                    CollectionNames.BELONGS_TO_SLACK_CHANNEL.value,
                    CollectionNames.BELONGS_TO.value,
                    CollectionNames.SLACK_MESSAGE_TO_METADATA.value,
                    CollectionNames.SLACK_FILE_TO_ATTACHMENT_METADATA.value,
                ]
            )

            try:
                # 1. Sync workspace info
                workspace_info = await self.slack_client.team_info()
                if workspace_info.get('ok'):
                    team = workspace_info['team']
                    existing_workspace = await self.arango_service.get_workspace_by_org_id(org_id, txn)
                    if not existing_workspace:
                        query = """
                        FOR ws IN slackWorkspaces FILTER ws.externalId == @externalId RETURN ws
                        """
                        cursor = self.arango_service.db.aql.execute(query, bind_vars={"externalId": team.get('id')})
                        existing_workspace = next(cursor, None)
                    timestamp = get_epoch_timestamp_in_ms()
                    if existing_workspace:
                        workspace_id = existing_workspace['_key']
                        self.logger.info(f"Updating existing workspace: {workspace_id}")
                        await self.arango_service.update_workspace(
                            workspace_id,
                            {
                                "syncState": "NOT_STARTED",
                                "updatedAtTimestamp": timestamp
                            },
                            txn
                        )
                    else:
                        workspace_id = str(uuid.uuid4())
                        self.logger.info(f"Creating new workspace: {workspace_id}")
                        workspace_doc = {
                            "_key": workspace_id,
                            "orgId": org_id,
                            "externalId": team.get('id'),
                            "name": team.get('name'),
                            "domain": team.get('domain'),
                            "emailDomain": team.get('email_domain'),
                            "url": team.get('url'),
                            "isActive": True,
                            "syncState": "NOT_STARTED",
                            "createdAtTimestamp": timestamp,
                            "updatedAtTimestamp": timestamp,
                        }
                        await self.store_workspace_data(workspace_doc, org_id, txn)
                # 2. Sync users
                users_stats = await self.sync_workspace_users(workspace_id, org_id, txn)
                sync_stats["users_processed"] = users_stats["processed"]
                sync_stats["errors"].extend(users_stats["errors"])

                # 3. Sync channels
                channels_stats = await self.sync_workspace_channels(workspace_id, org_id, txn)
                sync_stats["channels_processed"] = channels_stats["processed"]
                sync_stats["errors"].extend(channels_stats["errors"])

                # Commit workspace setup
                txn.commit_transaction()

                # 4. After users and channels are present, create user-channel permissions edges
                all_channels = await self.arango_service.get_workspace_channels(org_id)
                channel_docs = [c['channel'] for c in all_channels if 'channel' in c]
                await self.sync_channels_members_batch(channel_docs, org_id)

                # 5. Sync messages (separate process due to volume)
                messages_stats = await self.sync_workspace_messages(workspace_id, org_id)
                sync_stats["messages_processed"] = messages_stats["processed"]
                sync_stats["files_processed"] = messages_stats["files_processed"]
                sync_stats["errors"].extend(messages_stats["errors"])

                self.logger.info("✅ Workspace sync completed: %s", sync_stats)
                return sync_stats

            except Exception as e:
                if hasattr(txn, 'abort_transaction'):
                    txn.abort_transaction()
                raise

        except Exception as e:
            self.logger.error("❌ Workspace sync failed: %s", str(e))
            sync_stats["errors"].append(str(e))
            return sync_stats

    async def sync_workspace_users(
        self,
        workspace_id: str,
        org_id: str,
        transaction=None
    ) -> Dict[str, Any]:
        """Sync all users in workspace using batch operations"""
        try:
            self.logger.info("🚀 Syncing workspace users")
            stats = {"processed": 0, "errors": []}
            cursor = None
            while True:
                params = {"limit": 100}
                if cursor:
                    params["cursor"] = cursor
                response = await self.slack_client.users_list(**params)
                if not response.get('ok'):
                    stats["errors"].append(f"Failed to get users: {response.get('error')}")
                    break
                users = response.get('members', [])
                self.logger.info("Processing %d users", len(users))
                new_user_docs = []
                timestamp = get_epoch_timestamp_in_ms()
                for user in users:
                    email = user.get('profile', {}).get('email')
                    if not email or not isinstance(email, str) or not email.strip():
                        self.logger.warning(f"Skipping user {user.get('id')} due to missing/invalid email.")
                        continue
                    existing_user = await self.arango_service.get_user_by_slack_id(user.get('id'), org_id, transaction)
                    if existing_user:
                        user_id = existing_user['_key']
                        self.logger.info(f"Updating existing user: {user_id}")
                        update_fields = {
                            "isActive": not user.get('deleted', False),
                            "updatedAtTimestamp": timestamp,
                            "designation": (
                                "Primary Owner" if user.get('is_primary_owner') else
                                "Owner" if user.get('is_owner') else
                                "Admin" if user.get('is_admin') else
                                "Bot" if user.get('is_bot') else
                                "Member"
                            ),
                        }
                        await self.arango_service.update_user(user_id, update_fields, transaction)
                    else:
                        user_id = str(uuid.uuid4())
                        self.logger.info(f"Creating new user: {user_id}")
                        user_doc = {
                            "_key": user_id,
                            "userId": user.get('id'),
                            "orgId": org_id,
                            "firstName": user.get('profile', {}).get('first_name'),
                            "lastName": user.get('profile', {}).get('last_name'),
                            "fullName": user.get('profile', {}).get('real_name') or user.get('name'),
                            "email": email,
                            "designation": (
                                "Primary Owner" if user.get('is_primary_owner') else
                                "Owner" if user.get('is_owner') else
                                "Admin" if user.get('is_admin') else
                                "Bot" if user.get('is_bot') else
                                "Member"
                            ),
                            "isActive": not user.get('deleted', False),
                            "createdAtTimestamp": timestamp,
                            "updatedAtTimestamp": timestamp,
                        }
                        new_user_docs.append(user_doc)
                if new_user_docs:
                    success = await self.arango_service.batch_upsert_nodes(new_user_docs, CollectionNames.USERS.value, transaction)
                    if success:
                        stats["processed"] += len(new_user_docs)
                    else:
                        stats["errors"].append(f"Failed to store user batch of {len(new_user_docs)} users")
                    await asyncio.sleep(self.rate_limit_delay)
                cursor = response.get('response_metadata', {}).get('next_cursor')
                if not cursor:
                    break
                await asyncio.sleep(self.rate_limit_delay)
            self.logger.info("✅ Users sync completed: %d processed", stats["processed"])
            return stats
        except Exception as e:
            self.logger.error("❌ Users sync failed: %s", str(e))
            stats["errors"].append(str(e))
            return stats

    async def sync_workspace_channels(
        self,
        workspace_id: str,
        org_id: str,
        transaction=None
    ) -> Dict[str, Any]:
        """Sync all channels in workspace using batch operations"""
        try:
            self.logger.info("🚀 Syncing workspace channels")
            stats = {"processed": 0, "errors": []}
            channel_types = ['public_channel', 'private_channel', 'mpim', 'im']
            for channel_type in channel_types:
                try:
                    type_stats = await self.sync_channels_by_type(workspace_id, org_id, channel_type, transaction)
                    stats["processed"] += type_stats["processed"]
                    stats["errors"].extend(type_stats["errors"])
                except Exception as e:
                    self.logger.error("❌ Error syncing %s channels: %s", channel_type, str(e))
                    stats["errors"].append(f"{channel_type}: {str(e)}")
            self.logger.info("✅ Channels sync completed: %d processed", stats["processed"])
            return stats
        except Exception as e:
            self.logger.error("❌ Channels sync failed: %s", str(e))
            stats["errors"].append(str(e))
            return stats

    async def sync_channels_by_type(
        self,
        workspace_id: str,
        org_id: str,
        channel_type: str,
        transaction=None
    ) -> Dict[str, Any]:
        """Sync channels of a specific type and create proper relationships"""
        try:
            stats = {"processed": 0, "errors": []}
            cursor = None
            while True:
                params = {
                    "types": channel_type,
                    "limit": 100,
                    "exclude_archived": False
                }
                if cursor:
                    params["cursor"] = cursor
                response = await self.slack_client.conversations_list(**params)
                if not response.get('ok'):
                    stats["errors"].append(f"Failed to get {channel_type} channels: {response.get('error')}")
                    break
                channels = response.get('channels', [])
                self.logger.info("Processing %d %s channels", len(channels), channel_type)
                new_channel_docs = []
                belongs_to_edges = []
                timestamp = get_epoch_timestamp_in_ms()
                workspace = await self.arango_service.get_workspace_by_org_id(org_id, transaction)
                if not workspace:
                    self.logger.error(f"❌ Workspace not found for org_id: {org_id}")
                    stats["errors"].append(f"Workspace not found for org_id: {org_id}")
                    break
                workspace_key = workspace.get("_key")
                for channel in channels:
                    existing_channel = await self.arango_service.get_channel_by_slack_id(channel.get('id'), org_id, transaction)
                    if existing_channel:
                        channel_id = existing_channel['_key']
                        self.logger.info(f"Updating existing channel: {channel_id}")
                        update_fields = {
                            "isDeletedAtSource": channel.get('is_archived', False),
                            "updatedAtTimestamp": timestamp,
                            "syncState": ProgressStatus.NOT_STARTED.value,
                            "sourceLastModifiedTimestamp": channel.get('updated'),
                        }
                        await self.arango_service.update_channel(channel_id, update_fields, transaction)
                    else:
                        channel_id = str(uuid.uuid4())
                        self.logger.info(f"Creating new channel: {channel_id}")
                        group_record = {
                            "_key": channel_id,
                            "orgId": org_id,
                            "groupName": channel.get('name', 'Unnamed Channel'),
                            "externalGroupId": channel.get('id'),
                            "groupType": GroupType.SLACK_CHANNEL.value,
                            "connectorName": Connectors.SLACK.value,
                            "createdAtTimestamp": timestamp,
                            "updatedAtTimestamp": timestamp,
                            "isDeletedAtSource": channel.get('is_archived', False),
                            "sourceCreatedAtTimestamp": channel.get('created'),
                            "sourceLastModifiedTimestamp": channel.get('updated'),
                            "syncState": ProgressStatus.NOT_STARTED.value,
                        }
                        new_channel_docs.append(group_record)
                        belongs_to_edges.append({
                            "_from": f"{CollectionNames.RECORD_GROUPS.value}/{channel_id}",
                            "_to": f"{CollectionNames.SLACK_WORKSPACES.value}/{workspace_key}",
                            "entityType": "WORKSPACE",
                            "createdAtTimestamp": timestamp,
                            "updatedAtTimestamp": timestamp,
                        })
                if new_channel_docs:
                    success = await self.arango_service.batch_upsert_nodes(new_channel_docs, CollectionNames.RECORD_GROUPS.value, transaction)
                    if success and belongs_to_edges:
                        await self.arango_service.batch_create_edges(belongs_to_edges, CollectionNames.BELONGS_TO_SLACK_WORKSPACE.value, transaction)
                        self.logger.info("✅ Batch stored %d Slack channels and created belongsTo edges successfully", len(new_channel_docs))
                        stats["processed"] += len(new_channel_docs)
                    else:
                        stats["errors"].append(f"Failed to store {channel_type} batch of {len(new_channel_docs)} channels")
                    await asyncio.sleep(self.rate_limit_delay)
                cursor = response.get('response_metadata', {}).get('next_cursor')
                if not cursor:
                    break
                await asyncio.sleep(self.rate_limit_delay)
            return stats
        except Exception as e:
            self.logger.error("❌ Channel type sync failed for %s: %s", channel_type, str(e))
            stats["errors"].append(str(e))
            return stats

    async def sync_workspace_messages(
        self,
        workspace_id: str,
        org_id: str
    ) -> Dict[str, Any]:
        """Sync messages across all channels using batch operations with corrected threading"""
        try:
            self.logger.info("🚀 Syncing workspace messages")
            
            stats = {"processed": 0, "files_processed": 0, "errors": []}
            
            # Get all channels from database
            channels = await self.arango_service.get_workspace_channels(org_id)
            
            for channel_data in channels:
                try:
                    channel_info = channel_data['channel']
                    
                    # Skip archived channels if needed
                    if channel_info.get('isDeletedAtSource'):
                        continue
                    
                    # Update channel sync state
                    await self.arango_service.update_channel_sync_state(
                        channel_info['_key'], "IN_PROGRESS"
                    )
                    
                    # Sync channel messages with corrected threading
                    channel_stats = await self.sync_channel_messages_with_threads(
                        channel_info['externalGroupId'],
                        channel_info['_key'],
                        org_id
                    )
                    
                    stats["processed"] += channel_stats["processed"]
                    stats["files_processed"] += channel_stats["files_processed"]
                    stats["errors"].extend(channel_stats["errors"])
                    
                    # Update channel sync state
                    final_state = "COMPLETED" if not channel_stats["errors"] else "FAILED"
                    await self.arango_service.update_channel_sync_state(
                        channel_info['_key'], final_state
                    )
                    
                    # Rate limiting between channels
                    await asyncio.sleep(self.rate_limit_delay)
                    
                except Exception as e:
                    self.logger.error("❌ Error syncing messages for channel: %s", str(e))
                    stats["errors"].append(str(e))
                    
                    # Mark channel as failed
                    if 'channel_info' in locals():
                        await self.arango_service.update_channel_sync_state(
                            channel_info['_key'], "FAILED"
                        )

            self.logger.info("✅ Messages sync completed: %s", stats)
            return stats

        except Exception as e:
            self.logger.error("❌ Messages sync failed: %s", str(e))
            stats["errors"].append(str(e))
            return stats

    # ==================== CHANNEL-SPECIFIC SYNC METHODS ====================

    async def sync_channel_messages_with_threads(
        self,
        channel_id: str,
        channel_key: str,
        org_id: str,
        oldest: Optional[str] = None,
        latest: Optional[str] = None
    ) -> Dict[str, Any]:
        """Sync all messages in a channel with proper thread handling"""
        try:
            self.logger.info("🚀 Syncing messages with threads for channel: %s", channel_id)
            
            stats = {"processed": 0, "files_processed": 0, "errors": []}
            
            # Check if bot needs to join the channel first
            join_result = await self.ensure_bot_channel_access(channel_id)
            if not join_result["success"]:
                stats["errors"].append(f"Failed to ensure bot access to channel: {join_result.get('error')}")
                return stats
            
            has_more = True
            cursor = None
            processed_threads = set()  # Track processed threads to avoid duplicates
            
            while has_more:
                # Get messages batch from Slack API
                params = {
                    "channel": channel_id,
                    "limit": 100,
                    "inclusive": True
                }
                if oldest:
                    params["oldest"] = oldest
                if latest:
                    params["latest"] = latest
                if cursor:
                    params["cursor"] = cursor

                response = await self.slack_client.conversations_history(**params)
                
                if not response.get('ok'):
                    error_msg = response.get('error')
                    if error_msg == 'not_in_channel':
                        # Try to rejoin
                        rejoin_result = await self.join_channel(channel_id, force=True)
                        if rejoin_result["success"]:
                            response = await self.slack_client.conversations_history(**params)
                            if not response.get('ok'):
                                stats["errors"].append(f"Failed to get messages after rejoin: {response.get('error')}")
                                break
                        else:
                            stats["errors"].append(f"Failed to rejoin channel: {rejoin_result.get('error')}")
                            break
                    else:
                        stats["errors"].append(f"Failed to get messages: {error_msg}")
                        break

                messages = response.get('messages', [])
                self.logger.info("Processing %d messages", len(messages))

                # Process messages with corrected thread handling
                if messages:
                    batch_stats = await self.process_messages_batch_with_threads(
                        messages, channel_id, channel_key, org_id, processed_threads
                    )
                    
                    stats["processed"] += batch_stats["processed"]
                    stats["files_processed"] += batch_stats["files_processed"]
                    stats["errors"].extend(batch_stats["errors"])

                # Check for more messages
                has_more = response.get('has_more', False)
                cursor = response.get('response_metadata', {}).get('next_cursor')
                
                await asyncio.sleep(self.rate_limit_delay)

            self.logger.info("✅ Channel messages with threads sync completed: %s", stats)
            return stats

        except Exception as e:
            self.logger.error("❌ Channel messages sync failed: %s", str(e))
            stats["errors"].append(str(e))
            return stats

    async def process_messages_batch_with_threads(
        self,
        messages: List[Dict[str, Any]],
        channel_id: str,
        channel_key: str,
        org_id: str,
        processed_threads: set
    ) -> Dict[str, Any]:
        """Process a batch of messages and their threads with proper relationships"""
        stats = {"processed": 0, "files_processed": 0, "errors": []}
        
        try:
            timestamp = get_epoch_timestamp_in_ms()
            
            # Containers for batch operations
            message_records = []
            message_metadata = []
            all_edges = {
                CollectionNames.BELONGS_TO_SLACK_CHANNEL.value: [],  # For message/file -> channel relationships
                CollectionNames.SLACK_MESSAGE_TO_METADATA.value: [], # For message -> metadata relationships
                CollectionNames.SLACK_FILE_TO_ATTACHMENT_METADATA.value: [], # For file -> metadata relationships
                CollectionNames.RECORD_RELATIONS.value: [],  # For message -> file (attachments) and thread relationships
            }
            file_records = []
            file_metadata = []
            
            # Process each message
            for message in messages:
                if not message:
                    continue
                    
                message_key = str(uuid.uuid4())
                
                # Create message record
                message_record = {
                    "_key": message_key,
                    "orgId": org_id,
                    "recordName": f"Slack_Message_{message.get('ts', '')}",
                    "externalRecordId": message.get('ts'),
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
                    "sourceCreatedAtTimestamp": int(float(message.get('ts', 0)) * 1000),
                    "sourceLastModifiedTimestamp": int(float(message.get('edited', {}).get('ts', 0)) * 1000) if message.get('edited') else int(float(message.get('ts', 0)) * 1000),
                    "isDeleted": False,
                    "isArchived": False,
                    "reason": None,
                    "lastIndexTimestamp": None,
                    "lastExtractionTimestamp": None,
                    "indexingStatus": ProgressStatus.NOT_STARTED.value,
                    "extractionStatus": ProgressStatus.NOT_STARTED.value,
                }
                message_records.append(message_record)
                
                # Create message metadata
                message_metadata_doc = {
                    "_key": message_key,
                    "orgId": org_id,
                    "slackTs": message.get('ts'),
                    "threadTs": message.get('thread_ts'),
                    "channelId": channel_id,
                    "userId": message.get('user'),
                    "messageType": MessageType.THREAD_MESSAGE.value if message.get('thread_ts') else MessageType.ROOT_MESSAGE.value,
                    "text": message.get('text', ''),
                    "replyCount": message.get('reply_count', 0),
                    "replyUsersCount": message.get('reply_users_count', 0),
                    "replyUsers": message.get('reply_users', []),
                    "hasFiles": bool(message.get('files')),
                    "fileCount": len(message.get('files', [])),
                    "botId": message.get('bot_id'),
                    "mentionedUsers": self._extract_mentions(message),
                    "links": self._extract_links(message)
                }
                message_metadata.append(message_metadata_doc)
                
                # Add message edges
                # Message to channel edge
                all_edges[CollectionNames.BELONGS_TO_SLACK_CHANNEL.value].append({
                    "_from": f"{CollectionNames.RECORDS.value}/{message_key}",
                    "_to": f"{CollectionNames.RECORD_GROUPS.value}/{channel_key}",
                    "entityType": EntityType.SLACK_CHANNEL.value,
                    "createdAtTimestamp": timestamp,
                    "updatedAtTimestamp": timestamp
                })
                
                # Message to metadata edge
                all_edges[CollectionNames.SLACK_MESSAGE_TO_METADATA.value].append({
                    "_from": f"{CollectionNames.RECORDS.value}/{message_key}",
                    "_to": f"{CollectionNames.SLACK_MESSAGE_METADATA.value}/{message_key}",
                    "createdAtTimestamp": timestamp,
                    "updatedAtTimestamp": timestamp
                })
                
                # Process files if any
                if message.get('files'):
                    for file in message.get('files', []):
                        if not file:
                            continue
                            
                        file_key = str(uuid.uuid4())
                        
                        # Create file record
                        file_record = {
                            "_key": file_key,
                            "orgId": org_id,
                            "recordName": file.get('name', ''),
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
                        file_records.append(file_record)
                        
                        # Create file metadata
                        file_metadata_doc = {
                            "_key": file_key,
                            "orgId": org_id,
                            "slackFileId": file.get('id'),
                            "name": file.get('name', ''),
                            "extension": file.get('filetype'),
                            "mimeType": file.get('mimetype'),
                            "sizeInBytes": file.get('size', 0),
                            "channelId": channel_id,
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
                        file_metadata.append(file_metadata_doc)
                        
                        # Add file edges
                        # File to channel edge
                        all_edges[CollectionNames.BELONGS_TO_SLACK_CHANNEL.value].append({
                            "_from": f"{CollectionNames.RECORDS.value}/{file_key}",
                            "_to": f"{CollectionNames.RECORD_GROUPS.value}/{channel_key}",
                            "entityType": EntityType.SLACK_CHANNEL.value,
                            "createdAtTimestamp": timestamp,
                            "updatedAtTimestamp": timestamp
                        })
                        
                        # File to metadata edge
                        all_edges[CollectionNames.SLACK_FILE_TO_ATTACHMENT_METADATA.value].append({
                            "_from": f"{CollectionNames.RECORDS.value}/{file_key}",
                            "_to": f"{CollectionNames.SLACK_ATTACHMENT_METADATA.value}/{file_key}",
                            "createdAtTimestamp": timestamp,
                            "updatedAtTimestamp": timestamp
                        })
                        
                        # Message to file edge
                        all_edges[CollectionNames.RECORD_RELATIONS.value].append({
                            "_from": f"{CollectionNames.RECORDS.value}/{message_key}",
                            "_to": f"{CollectionNames.RECORDS.value}/{file_key}",
                            "relationshipType": RecordRelations.ATTACHMENT.value,
                            "createdAtTimestamp": timestamp,
                            "updatedAtTimestamp": timestamp
                        })
                
                # Process thread replies if this is a parent message
                if message.get('thread_ts') == message.get('ts') and message.get('ts') not in processed_threads:
                    try:
                        thread_response = await self.slack_client.conversations_replies(
                            channel=channel_id,
                            ts=message.get('ts'),
                            inclusive=False  # Exclude parent message
                        )
                        
                        if thread_response and thread_response.get('ok') and thread_response.get('messages'):
                            thread_replies = thread_response.get('messages', [])
                            
                            # Add thread relationship edges
                            for reply in thread_replies:
                                if not reply or reply.get('ts') == message.get('ts'):
                                    continue
                                    
                                reply_key = str(uuid.uuid4())
                                
                                # Create reply record
                                reply_record = {
                                    "_key": reply_key,
                                    "orgId": org_id,
                                    "recordName": f"Slack_Message_{reply.get('ts', '')}",
                                    "externalRecordId": reply.get('ts'),
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
                                    "sourceCreatedAtTimestamp": int(float(reply.get('ts', 0)) * 1000),
                                    "sourceLastModifiedTimestamp": int(float(reply.get('edited', {}).get('ts', 0)) * 1000) if reply.get('edited') else int(float(reply.get('ts', 0)) * 1000),
                                    "isDeleted": False,
                                    "isArchived": False,
                                    "reason": None,
                                    "lastIndexTimestamp": None,
                                    "lastExtractionTimestamp": None,
                                    "indexingStatus": ProgressStatus.NOT_STARTED.value,
                                    "extractionStatus": ProgressStatus.NOT_STARTED.value,
                                }
                                message_records.append(reply_record)
                                
                                # Create reply metadata
                                reply_metadata = {
                                    "_key": reply_key,
                                    "orgId": org_id,
                                    "slackTs": reply.get('ts'),
                                    "threadTs": reply.get('thread_ts'),
                                    "channelId": channel_id,
                                    "userId": reply.get('user'),
                                    "messageType": MessageType.THREAD_MESSAGE.value,
                                    "text": reply.get('text', ''),
                                    "replyCount": 0,
                                    "replyUsersCount": 0,
                                    "replyUsers": [],
                                    "hasFiles": bool(reply.get('files')),
                                    "fileCount": len(reply.get('files', [])),
                                    "botId": reply.get('bot_id'),
                                    "mentionedUsers": self._extract_mentions(reply),
                                    "links": self._extract_links(reply)
                                }
                                message_metadata.append(reply_metadata)
                                
                                # Add reply edges
                                # Reply to channel edge
                                all_edges[CollectionNames.BELONGS_TO_SLACK_CHANNEL.value].append({
                                    "_from": f"{CollectionNames.RECORDS.value}/{reply_key}",
                                    "_to": f"{CollectionNames.RECORD_GROUPS.value}/{channel_key}",
                                    "entityType": EntityType.SLACK_CHANNEL.value,
                                    "createdAtTimestamp": timestamp,
                                    "updatedAtTimestamp": timestamp
                                })
                                
                                # Reply to metadata edge
                                all_edges[CollectionNames.SLACK_MESSAGE_TO_METADATA.value].append({
                                    "_from": f"{CollectionNames.RECORDS.value}/{reply_key}",
                                    "_to": f"{CollectionNames.SLACK_MESSAGE_METADATA.value}/{reply_key}",
                                    "createdAtTimestamp": timestamp,
                                    "updatedAtTimestamp": timestamp
                                })
                                
                                # Thread relationship edge
                                all_edges[CollectionNames.RECORD_RELATIONS.value].append({
                                    "_from": f"{CollectionNames.RECORDS.value}/{message_key}",
                                    "_to": f"{CollectionNames.RECORDS.value}/{reply_key}",
                                    "relationshipType": RecordRelations.PARENT_CHILD.value,
                                    "createdAtTimestamp": timestamp,
                                    "updatedAtTimestamp": timestamp
                                })
                                
                                # Process reply files if any
                                if reply.get('files'):
                                    for file in reply.get('files', []):
                                        if not file:
                                            continue
                                            
                                        file_key = str(uuid.uuid4())
                                        
                                        # Create file record
                                        file_record = {
                                            "_key": file_key,
                                            "orgId": org_id,
                                            "recordName": file.get('name', ''),
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
                                        file_records.append(file_record)
                                        
                                        # Create file metadata
                                        file_metadata_doc = {
                                            "_key": file_key,
                                            "orgId": org_id,
                                            "slackFileId": file.get('id'),
                                            "name": file.get('name', ''),
                                            "extension": file.get('filetype'),
                                            "mimeType": file.get('mimetype'),
                                            "sizeInBytes": file.get('size', 0),
                                            "channelId": channel_id,
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
                                        file_metadata.append(file_metadata_doc)
                                        
                                        # Add file edges
                                        # File to channel edge
                                        all_edges[CollectionNames.BELONGS_TO_SLACK_CHANNEL.value].append({
                                            "_from": f"{CollectionNames.RECORDS.value}/{file_key}",
                                            "_to": f"{CollectionNames.RECORD_GROUPS.value}/{channel_key}",
                                            "entityType": EntityType.SLACK_CHANNEL.value,
                                            "createdAtTimestamp": timestamp,
                                            "updatedAtTimestamp": timestamp
                                        })
                                        
                                        # File to metadata edge
                                        all_edges[CollectionNames.SLACK_FILE_TO_ATTACHMENT_METADATA.value].append({
                                            "_from": f"{CollectionNames.RECORDS.value}/{file_key}",
                                            "_to": f"{CollectionNames.SLACK_ATTACHMENT_METADATA.value}/{file_key}",
                                            "createdAtTimestamp": timestamp,
                                            "updatedAtTimestamp": timestamp
                                        })
                                        
                                        # Reply to file edge
                                        all_edges[CollectionNames.RECORD_RELATIONS.value].append({
                                            "_from": f"{CollectionNames.RECORDS.value}/{reply_key}",
                                            "_to": f"{CollectionNames.RECORDS.value}/{file_key}",
                                            "relationshipType": RecordRelations.ATTACHMENT.value,
                                            "createdAtTimestamp": timestamp,
                                            "updatedAtTimestamp": timestamp
                                        })
                                
                                processed_threads.add(reply.get('ts'))
                                
                    except Exception as e:
                        self.logger.error("❌ Error processing thread replies: %s", str(e))
                        stats["errors"].append(f"Thread processing error: {str(e)}")
                        continue
            
            # Perform all batch operations in a single transaction
            try:
                txn = self.arango_service.db.begin_transaction(write=[
                    CollectionNames.RECORDS.value,
                    CollectionNames.SLACK_MESSAGE_METADATA.value,
                    CollectionNames.SLACK_ATTACHMENT_METADATA.value,
                    CollectionNames.BELONGS_TO_SLACK_CHANNEL.value,
                    CollectionNames.SLACK_MESSAGE_TO_METADATA.value,
                    CollectionNames.SLACK_FILE_TO_ATTACHMENT_METADATA.value,
                    CollectionNames.RECORD_RELATIONS.value
                ])
                
                try:
                    # Store all message records
                    if message_records:
                        await self.arango_service.batch_upsert_nodes(
                            message_records,
                            CollectionNames.RECORDS.value,
                            transaction=txn
                        )
                    
                    # Store all message metadata
                    if message_metadata:
                        await self.arango_service.batch_upsert_nodes(
                            message_metadata,
                            CollectionNames.SLACK_MESSAGE_METADATA.value,
                            transaction=txn
                        )
                    
                    # Store all file records
                    if file_records:
                        await self.arango_service.batch_upsert_nodes(
                            file_records,
                            CollectionNames.RECORDS.value,
                            transaction=txn
                        )
                    
                    # Store all file metadata
                    if file_metadata:
                        await self.arango_service.batch_upsert_nodes(
                            file_metadata,
                            CollectionNames.SLACK_ATTACHMENT_METADATA.value,
                            transaction=txn
                        )
                    
                    # Create all edges in their respective collections
                    for collection, edges in all_edges.items():
                        if edges:
                            await self.arango_service.batch_create_edges(
                                edges,
                                collection,
                                transaction=txn
                            )
                    
                    txn.commit_transaction()
                    stats["processed"] = len(message_records)
                    stats["files_processed"] = len(file_records)
                    
                except Exception as e:
                    txn.abort_transaction()
                    raise e
                    
            except Exception as e:
                self.logger.error("❌ Transaction failed: %s", str(e))
                stats["errors"].append(f"Transaction error: {str(e)}")
                raise
            
            return stats
            
        except Exception as e:
            self.logger.error("❌ Error processing message batch: %s", str(e))
            stats["errors"].append(str(e))
            return stats

    async def _store_single_message(
        self,
        message: Dict[str, Any],
        channel_id: str,
        channel_key: str,
        org_id: str,
        txn,
        is_thread_reply: bool = False
    ) -> Optional[Dict[str, Any]]:
        """Store a single message with all its relationships"""
        try:
            message_key = str(uuid.uuid4())
            timestamp = get_epoch_timestamp_in_ms()
            
            # Create message record
            message_record = {
                "_key": message_key,
                "orgId": org_id,
                "recordName": f"Message {message.get('ts')}",
                "recordType": RecordTypes.MESSAGE.value,
                "externalRecordId": message.get('ts'),
                "origin": OriginTypes.CONNECTOR.value,
                "connectorName": Connectors.SLACK.value,
                "createdAtTimestamp": timestamp,
                "updatedAtTimestamp": timestamp,
                "sourceCreatedAtTimestamp": int(float(message.get('ts', 0)) * 1000),
                "sourceLastModifiedTimestamp": int(float(message.get('ts', 0)) * 1000),
                "isArchived": False,
                "isDeleted": False,
                "virtualRecordId": None,
                "lastSyncTimestamp": timestamp,
                "indexingStatus": ProgressStatus.NOT_STARTED.value,
                "extractionStatus": ProgressStatus.NOT_STARTED.value,
                "isLatestVersion": True,
                "lastIndexTimestamp": None,
                "lastExtractionTimestamp": None,
                "isDirty": False,
                "reason": None
            }
            
            # Create message metadata
            message_metadata = {
                "_key": message_key,
                "orgId": org_id,
                "slackTs": message.get('ts'),
                "threadTs": message.get('thread_ts'),
                "channelId": channel_id,
                "userId": message.get('user'),
                "messageType": MessageType.THREAD_MESSAGE.value if is_thread_reply else MessageType.ROOT_MESSAGE.value,
                "text": message.get('text', ''),
                "replyCount": message.get('reply_count', 0),
                "replyUsersCount": message.get('reply_users_count', 0),
                "replyUsers": message.get('reply_users', []),
                "hasFiles": bool(message.get('files')),
                "fileCount": len(message.get('files', [])),
                "botId": message.get('bot_id'),
                "mentionedUsers": self._extract_mentions(message),
                "links": self._extract_links(message),
            }
            
            # Store message record and metadata
            await self.arango_service.batch_upsert_nodes(
                [message_record],
                collection=CollectionNames.RECORDS.value,
                transaction=txn
            )
            
            await self.arango_service.batch_upsert_nodes(
                [message_metadata],
                collection=CollectionNames.SLACK_MESSAGE_METADATA.value,
                transaction=txn
            )
            
            # Create edges
            edges = []
            
            # Edge between message and channel
            edges.append({
                "_from": f"{CollectionNames.RECORDS.value}/{message_key}",
                "_to": f"{CollectionNames.RECORD_GROUPS.value}/{channel_key}",
                "createdAtTimestamp": timestamp,
                "updatedAtTimestamp": timestamp
            })
            
            # Edge between message and metadata
            edges.append({
                "_from": f"{CollectionNames.RECORDS.value}/{message_key}",
                "_to": f"{CollectionNames.SLACK_MESSAGE_METADATA.value}/{message_key}",
                "createdAtTimestamp": timestamp,
                "updatedAtTimestamp": timestamp
            })
            
            # If this is a thread reply, create parent-child relationship
            if is_thread_reply and message.get('thread_ts'):
                parent_key = await self._find_or_fetch_parent_message(
                    message['thread_ts'],
                    channel_id,
                    channel_key,
                    org_id,
                    txn
                )
                if parent_key:
                    edges.append({
                        "_from": f"{CollectionNames.RECORDS.value}/{parent_key}",
                        "_to": f"{CollectionNames.RECORDS.value}/{message_key}",
                        "relationType": RecordRelations.PARENT_CHILD.value,
                        "createdAtTimestamp": timestamp,
                        "updatedAtTimestamp": timestamp
                    })
            
            # Create all edges
            await self.arango_service.batch_create_edges(
                edges,
                collection=CollectionNames.RECORD_RELATIONS.value,
                transaction=txn
            )
            
            # Process files if any
            if message.get('files'):
                await self._process_message_files(
                    message['files'],
                    message_key,
                    channel_key,
                    org_id,
                    txn
                )
            
            return {
                "message": message_record,
                "metadata": message_metadata
            }
            
        except Exception as e:
            self.logger.error("❌ Error storing message: %s", str(e))
            if txn:
                raise
            return None

    async def _find_or_fetch_parent_message(
        self,
        thread_ts: str,
        channel_id: str,
        channel_key: str,
        org_id: str,
        txn
    ) -> Optional[str]:
        """Find existing parent message or fetch and store it"""
        try:
            # First check if parent exists in database
            parent_message = await self.arango_service.get_message_by_slack_ts(thread_ts, org_id, txn)
            if parent_message:
                return parent_message['message']['_key']
            
            # If not found, fetch from Slack
            try:
                response = await self.slack_client.conversations_history(
                    channel=channel_id,
                    latest=thread_ts,
                    oldest=thread_ts,
                    inclusive=True,
                    limit=1
                )
                
                if response.get('ok') and response.get('messages'):
                    parent_msg = response['messages'][0]
                    parent_record = await self._store_single_message(
                        parent_msg, channel_id, channel_key, org_id, txn
                    )
                    if parent_record:
                        return parent_record['_key']
                        
            except Exception as e:
                self.logger.error(f"Error fetching parent message: {str(e)}")
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error finding parent message: {str(e)}")
            return None

    async def _process_message_files(
        self,
        files: List[Dict[str, Any]],
        message_key: str,
        channel_key: str,
        org_id: str,
        txn
    ) -> int:
        """Process files attached to a message and create proper relationships"""
        try:
            processed_files = 0
            timestamp = get_epoch_timestamp_in_ms()
            
            for file in files:
                file_key = str(uuid.uuid4())
                
                # Create file record
                file_record = {
                    "_key": file_key,
                    "orgId": org_id,
                    "recordName": file.get('name', 'Untitled'),
                    "recordType": RecordTypes.FILE.value,
                    "externalRecordId": file.get('id'),
                    "origin": OriginTypes.CONNECTOR.value,
                    "connectorName": Connectors.SLACK.value,
                    "createdAtTimestamp": timestamp,
                    "updatedAtTimestamp": timestamp,
                    "sourceCreatedAtTimestamp": int(float(file.get('created', 0)) * 1000),
                    "sourceLastModifiedTimestamp": int(float(file.get('updated', 0)) * 1000),
                    "isArchived": False,
                    "isDeleted": False,
                    "virtualRecordId": None,
                    "lastSyncTimestamp": timestamp,
                    "indexingStatus": ProgressStatus.NOT_STARTED.value,
                    "extractionStatus": ProgressStatus.NOT_STARTED.value,
                    "isLatestVersion": True,
                    "lastIndexTimestamp": None,
                    "lastExtractionTimestamp": None,
                    "isDirty": False,
                    "reason": None
                }
                
                # Create file metadata
                file_metadata_doc = {
                    "_key": file_key,
                    "orgId": org_id,
                    "slackFileId": file.get('id'),
                    "name": file.get('name', 'Untitled'),
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
                    collection=CollectionNames.RECORDS.value,
                    transaction=txn
                )
                
                await self.arango_service.batch_upsert_nodes(
                    [file_metadata_doc],
                    collection=CollectionNames.SLACK_ATTACHMENT_METADATA.value,
                    transaction=txn
                )
                
                # Create edges
                edges = []
                
                # Edge between file and channel
                edges.append({
                    "_from": f"{CollectionNames.RECORDS.value}/{file_key}",
                    "_to": f"{CollectionNames.RECORD_GROUPS.value}/{channel_key}",
                    "createdAtTimestamp": timestamp,
                    "updatedAtTimestamp": timestamp
                })
                
                # Edge between file and metadata
                edges.append({
                    "_from": f"{CollectionNames.RECORDS.value}/{file_key}",
                    "_to": f"{CollectionNames.SLACK_ATTACHMENT_METADATA.value}/{file_key}",
                    "createdAtTimestamp": timestamp,
                    "updatedAtTimestamp": timestamp
                })
                
                # Edge between message and file (attachment relationship)
                edges.append({
                    "_from": f"{CollectionNames.RECORDS.value}/{message_key}",
                    "_to": f"{CollectionNames.RECORDS.value}/{file_key}",
                    "relationType": RecordRelations.ATTACHMENT.value,
                    "createdAtTimestamp": timestamp,
                    "updatedAtTimestamp": timestamp
                })
                
                # Create all edges
                await self.arango_service.batch_create_edges(
                    edges,
                    collection=CollectionNames.RECORD_RELATIONS.value,
                    transaction=txn
                )
                
                processed_files += 1
                
            return processed_files
            
        except Exception as e:
            self.logger.error("❌ Error processing message files: %s", str(e))
            if txn:
                raise
            return 0

    def _extract_mentions(self, message: Dict[str, Any]) -> List[str]:
        """Extract user mentions from message"""
        mentions = []
        text = message.get('text', '')
        
        # Basic extraction - would need more sophisticated parsing
        import re
        user_mentions = re.findall(r'<@([A-Z0-9]+)>', text)
        mentions.extend(user_mentions)
        
        return mentions

    def _extract_links(self, message: Dict[str, Any]) -> List[str]:
        """Extract links from message"""
        links = []
        text = message.get('text', '')
        
        # Basic extraction - would need more sophisticated parsing
        import re
        url_pattern = r'<(https?://[^>]+)(?:\|[^>]+)?>'
        urls = re.findall(url_pattern, text)
        links.extend(urls)
        
        return links

    # ==================== UTILITY METHODS ====================

    async def sync_channels_members_batch(
        self,
        channels: List[Dict[str, Any]],
        org_id: str,
        transaction=None
    ) -> Dict[str, Any]:
        """Sync channel members in batch and create proper relationships"""
        try:
            self.logger.info("🚀 Syncing channel members batch")
            
            total_members = 0
            channel_member_edges = []
            channel_permissions = []
            timestamp = get_epoch_timestamp_in_ms()
            missing_users = set()  # Track users not found in DB
            
            for channel in channels:
                channel_key = channel.get('_key')
                channel_id = channel.get('externalGroupId')
                
                if not channel_key or not channel_id:
                    self.logger.warning(f"❌ Invalid channel data: {channel}")
                    continue
                    
                # Get channel members from Slack with pagination
                cursor = None
                while True:
                    response = await self.slack_client.conversations_members(
                        channel=channel_id,
                        cursor=cursor
                    )
                    
                    if not response or not response.get('ok'):
                        self.logger.warning(f"❌ Failed to get members for channel {channel_id}: {response.get('error', 'Unknown error')}")
                        break
                        
                    members = response.get('members', [])
                    if not members:
                        break
                    
                    # Process members in batches to reduce database calls
                    batch_size = 50
                    for i in range(0, len(members), batch_size):
                        member_batch = members[i:i + batch_size]
                        
                        # Get users from database in batch
                        for member_id in member_batch:
                            try:
                                # Get user from database using Slack ID
                                user = await self.arango_service.get_user_by_slack_id(member_id, org_id, transaction)
                                
                                if not user:
                                    missing_users.add(member_id)
                                    continue
                                
                                user_key = user.get('_key')
                                if not user_key:
                                    continue
                                
                                # Create channel membership edge
                                channel_member_edge = {
                                    "_from": f"{CollectionNames.USERS.value}/{user_key}",
                                    "_to": f"{CollectionNames.RECORD_GROUPS.value}/{channel_key}",
                                    "entityType": EntityType.SLACK_CHANNEL.value,
                                    "createdAtTimestamp": timestamp,
                                    "updatedAtTimestamp": timestamp
                                }
                                channel_member_edges.append(channel_member_edge)
                                
                                # Create permission edge
                                permission = {
                                    "_from": f"{CollectionNames.USERS.value}/{user_key}",
                                    "_to": f"{CollectionNames.RECORD_GROUPS.value}/{channel_key}",
                                    "type": PermissionType.USER.value,
                                    "role": "READER",  # Default role for channel members
                                    "createdAtTimestamp": timestamp,
                                    "updatedAtTimestamp": timestamp,
                                    "lastUpdatedTimestampAtSource": timestamp
                                }
                                channel_permissions.append(permission)
                                
                                total_members += 1
                                
                            except Exception as member_error:
                                self.logger.error(f"❌ Error processing member {member_id}: {str(member_error)}")
                                continue
                    
                    # Check for next page
                    cursor = response.get('response_metadata', {}).get('next_cursor')
                    if not cursor:
                        break
                        
                    # Add rate limiting delay between pages
                    await asyncio.sleep(self.rate_limit_delay)

            # Log any missing users for debugging
            if missing_users:
                self.logger.warning(f"⚠️ Users not found in database: {len(missing_users)} users")
            
            # Create edges in batch using transaction
            try:
                txn = self.arango_service.db.begin_transaction(write=[
                    CollectionNames.BELONGS_TO_SLACK_CHANNEL.value,
                    CollectionNames.PERMISSIONS.value
                ])
                
                try:
                    if channel_member_edges:
                        await self.arango_service.batch_create_edges(
                            channel_member_edges,
                            CollectionNames.BELONGS_TO_SLACK_CHANNEL.value,
                            transaction=txn
                        )
                        
                    if channel_permissions:
                        await self.arango_service.batch_create_edges(
                            channel_permissions,
                            CollectionNames.PERMISSIONS.value,
                            transaction=txn
                        )
                        
                    txn.commit_transaction()
                    
                    return {
                        "success": True,
                        "members_synced": total_members,
                        "member_edges_created": len(channel_member_edges),
                        "permission_edges_created": len(channel_permissions),
                        "missing_users": len(missing_users)
                    }
                    
                except Exception as e:
                    txn.abort_transaction()
                    raise e
                    
            except Exception as edge_error:
                self.logger.error(f"❌ Error creating edges: {str(edge_error)}")
                raise
            
        except Exception as e:
            self.logger.error(f"❌ Failed to sync channel members: {str(e)}")
            if transaction:
                raise
            return {
                "success": False,
                "error": str(e),
                "members_synced": total_members,
                "member_edges_created": len(channel_member_edges),
                "permission_edges_created": len(channel_permissions)
            }

    async def sync_incremental_changes(
        self,
        workspace_id: str,
        since_timestamp: Optional[int] = None
    ) -> Dict[str, Any]:
        """Sync incremental changes since last sync"""
        try:
            self.logger.info("🚀 Starting incremental sync")
            
            stats = {"processed": 0, "errors": []}
            
            # If no timestamp provided, get from last sync metadata
            if not since_timestamp:
                sync_metadata = await self.arango_service.get_sync_metadata("incremental")
                since_timestamp = sync_metadata.get('lastSyncTimestamp') if sync_metadata else 0

            # Get all channels to check for new messages
            channels = await self.arango_service.get_workspace_channels(workspace_id)
            
            for channel_data in channels:
                try:
                    channel_info = channel_data['channel']
                    
                    # Convert timestamp to Slack format
                    oldest = str(since_timestamp / 1000.0) if since_timestamp else None
                    
                    # Sync new messages in this channel
                    channel_stats = await self.sync_channel_messages_with_threads(
                        channel_info['externalGroupId'],
                        channel_info['_key'],
                        workspace_id,
                        oldest=oldest
                    )
                    
                    stats["processed"] += channel_stats["processed"]
                    stats["errors"].extend(channel_stats["errors"])
                    
                    await asyncio.sleep(self.rate_limit_delay)
                    
                except Exception as e:
                    self.logger.error("❌ Error in incremental sync for channel: %s", str(e))
                    stats["errors"].append(str(e))

            # Update sync metadata
            current_timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
            await self.arango_service.store_sync_metadata("incremental", {
                "lastSyncTimestamp": current_timestamp,
                "lastSyncProcessed": stats["processed"],
                "lastSyncErrors": len(stats["errors"])
            })

            self.logger.info("✅ Incremental sync completed: %s", stats)
            return stats

        except Exception as e:
            self.logger.error("❌ Incremental sync failed: %s", str(e))
            stats["errors"].append(str(e))
            return stats

    # ==================== BOT CHANNEL ACCESS METHODS ====================

    async def ensure_bot_channel_access(self, channel_id: str) -> Dict[str, Any]:
        """Ensure bot has access to channel before syncing messages"""
        try:
            self.logger.debug("🔍 Checking bot access to channel: %s", channel_id)
            
            # First, get channel info to determine channel type
            channel_info = await self.slack_client.conversations_info(channel=channel_id)
            
            if not channel_info.get('ok'):
                return {
                    "success": False,
                    "error": f"Failed to get channel info: {channel_info.get('error')}",
                    "action": "none"
                }
            
            channel_data = channel_info['channel']
            is_private = channel_data.get('is_private', False)
            is_member = channel_data.get('is_member', False)
            is_im = channel_data.get('is_im', False)  # Check if it's a DM channel
            
            # For DM channels, we don't need to join
            if is_im:
                return {
                    "success": True,
                    "action": "dm_channel",
                    "message": "DM channel - no join required"
                }
            
            # If bot is already a member, no action needed
            if is_member:
                return {
                    "success": True,
                    "action": "already_member",
                    "message": "Bot is already a member of the channel"
                }
            
            # For private channels, we can't join automatically
            if is_private:
                return {
                    "success": False,
                    "error": "Cannot join private channel - bot must be invited by a channel member",
                    "action": "manual_invite_required",
                    "channel_type": "private"
                }
            
            # For public channels, attempt to join
            return await self.join_channel(channel_id)

        except Exception as e:
            self.logger.error("❌ Error ensuring bot channel access: %s", str(e))
            return {
                "success": False,
                "error": str(e),
                "action": "error"
            }

    async def join_channel(self, channel_id: str, force: bool = False) -> Dict[str, Any]:
        """Join a public channel"""
        try:
            self.logger.info("🚀 Attempting to join channel: %s", channel_id)
            
            # Use conversations.join API to join the channel
            response = await self.slack_client.conversations_join(channel=channel_id)
            
            if response.get('ok'):
                self.logger.info("✅ Successfully joined channel: %s", channel_id)
                return {
                    "success": True,
                    "action": "joined",
                    "message": f"Successfully joined channel {channel_id}",
                    "channel_data": response.get('channel', {})
                }
            else:
                error_msg = response.get('error')
                
                # Handle specific error cases
                if error_msg == 'already_in_channel':
                    return {
                        "success": True,
                        "action": "already_member",
                        "message": "Bot is already a member of the channel"
                    }
                elif error_msg == 'is_archived':
                    return {
                        "success": False,
                        "error": "Cannot join archived channel",
                        "action": "channel_archived"
                    }
                elif error_msg == 'channel_not_found':
                    return {
                        "success": False,
                        "error": "Channel not found",
                        "action": "channel_not_found"
                    }
                elif error_msg == 'restricted_action':
                    return {
                        "success": False,
                        "error": "Bot doesn't have permission to join channels",
                        "action": "permission_denied"
                    }
                else:
                    return {
                        "success": False,
                        "error": f"Failed to join channel: {error_msg}",
                        "action": "join_failed"
                    }

        except Exception as e:
            self.logger.error("❌ Error joining channel: %s", str(e))
            return {
                "success": False,
                "error": str(e),
                "action": "error"
            }

    async def bulk_join_public_channels(self, workspace_id: str) -> Dict[str, Any]:
        """Join all accessible public channels in the workspace"""
        try:
            self.logger.info("🚀 Bulk joining public channels")
            
            stats = {
                "attempted": 0,
                "joined": 0,
                "already_member": 0,
                "failed": 0,
                "errors": []
            }
            
            # Get all public channels
            cursor = None
            
            while True:
                params = {
                    "types": "public_channel",
                    "limit": 100,
                    "exclude_archived": True  # Skip archived channels
                }
                if cursor:
                    params["cursor"] = cursor

                response = await self.slack_client.conversations_list(**params)
                
                if not response.get('ok'):
                    stats["errors"].append(f"Failed to get channels: {response.get('error')}")
                    break

                channels = response.get('channels', [])
                
                for channel in channels:
                    try:
                        channel_id = channel.get('id')
                        channel_name = channel.get('name')
                        
                        stats["attempted"] += 1
                        
                        # Skip if already a member
                        if channel.get('is_member'):
                            stats["already_member"] += 1
                            continue
                        
                        # Attempt to join
                        join_result = await self.join_channel(channel_id)
                        
                        if join_result["success"]:
                            if join_result["action"] == "joined":
                                stats["joined"] += 1
                                self.logger.info("✅ Joined channel: %s", channel_name)
                            elif join_result["action"] == "already_member":
                                stats["already_member"] += 1
                        else:
                            stats["failed"] += 1
                            stats["errors"].append(f"Channel {channel_name}: {join_result.get('error')}")
                        
                        # Rate limiting
                        await asyncio.sleep(self.rate_limit_delay)
                        
                    except Exception as e:
                        stats["failed"] += 1
                        stats["errors"].append(f"Channel {channel.get('name', 'unknown')}: {str(e)}")

                # Check for more channels
                cursor = response.get('response_metadata', {}).get('next_cursor')
                if not cursor:
                    break

                await asyncio.sleep(self.rate_limit_delay)

            self.logger.info("✅ Bulk join completed: %s", stats)
            return stats

        except Exception as e:
            self.logger.error("❌ Bulk join failed: %s", str(e))
            stats["errors"].append(str(e))
            return stats

    # ==================== KAFKA INTEGRATION METHODS ====================

    async def send_messages_to_kafka(
        self,
        slack_messages: List[Dict[str, Any]],
        message_records: List[Dict[str, Any]],
        org_id: str
    ) -> None:
        """Send message batch to Kafka"""
        try:
            if not self.kafka_service:
                return

            events = []
            for i, slack_message in enumerate(slack_messages):
                if i < len(message_records):
                    message_record = message_records[i]
                    
                    message_event = {
                        "orgId": org_id,
                        "recordId": message_record.get('_key'),
                        "recordName": f"Slack Message - {slack_message.get('ts')}",
                        "recordType": "MESSAGE",
                        "eventType": "NEW_RECORD",
                        "connectorName": "SLACK",
                        "origin": "CONNECTOR",
                        "text": slack_message.get('text', ''),
                        "channelId": slack_message.get('channel'),
                        "userId": slack_message.get('user'),
                        "createdAtSourceTimestamp": message_record.get('sourceCreatedAtTimestamp'),
                        "modifiedAtSourceTimestamp": message_record.get('sourceLastModifiedTimestamp'),
                    }
                    events.append(message_event)

            # Send events in batches
            batch_size = self.batch_size
            for i in range(0, len(events), batch_size):
                batch = events[i:i + batch_size]
                for event in batch:
                    await self.kafka_service.send_event_to_kafka(event)
                
                await asyncio.sleep(0.1)  # Brief pause between batches

            self.logger.info("✅ %d messages sent to Kafka", len(events))

        except Exception as e:
            self.logger.error("❌ Error sending messages to Kafka: %s", str(e))

    async def send_files_to_kafka(
        self,
        slack_files: List[Dict[str, Any]],
        file_records: List[Dict[str, Any]],
        org_id: str
    ) -> None:
        """Send file batch to Kafka"""
        try:
            if not self.kafka_service:
                return

            events = []
            for i, slack_file in enumerate(slack_files):
                if i < len(file_records):
                    file_record = file_records[i]
                    
                    file_event = {
                        "orgId": org_id,
                        "recordId": file_record.get('_key'),
                        "recordName": slack_file.get('name', 'Unnamed File'),
                        "recordType": "ATTACHMENT",
                        "eventType": "NEW_RECORD",
                        "connectorName": "SLACK",
                        "origin": "CONNECTOR",
                        "fileName": slack_file.get('name'),
                        "fileType": slack_file.get('filetype'),
                        "mimeType": slack_file.get('mimetype'),
                        "sizeInBytes": slack_file.get('size'),
                        "channelId": slack_file.get('channel'),
                        "uploadedBy": slack_file.get('user'),
                        "createdAtSourceTimestamp": file_record.get('sourceCreatedAtTimestamp'),
                        "modifiedAtSourceTimestamp": file_record.get('sourceLastModifiedTimestamp'),
                    }
                    events.append(file_event)

            # Send events in batches
            for event in events:
                await self.kafka_service.send_event_to_kafka(event)
                await asyncio.sleep(0.05)  # Brief pause between events

            self.logger.info("✅ %d files sent to Kafka", len(events))

        except Exception as e:
            self.logger.error("❌ Error sending files to Kafka: %s", str(e))

    # ==================== LEGACY SYNC METHODS (for compatibility) ====================

    async def sync_channel_messages(
        self,
        channel_id: str,
        channel_key: str,
        org_id: str,
        oldest: Optional[str] = None,
        latest: Optional[str] = None
    ) -> Dict[str, Any]:
        """Legacy method - redirects to new implementation"""
        return await self.sync_channel_messages_with_threads(
            channel_id, channel_key, org_id, oldest, latest
        )

    # ==================== QUERY AND UTILITY METHODS ====================

    async def get_workspace_statistics(self, workspace_id: str) -> Dict[str, Any]:
        """Get comprehensive workspace statistics"""
        try:
            stats = {
                "channels": {"total": 0, "public": 0, "private": 0, "archived": 0},
                "users": {"total": 0, "active": 0},
                "messages": {"total": 0, "with_files": 0, "threads": 0},
                "files": {"total": 0}
            }

            # Get channel stats
            channels = await self.arango_service.get_workspace_channels(workspace_id)
            stats["channels"]["total"] = len(channels)
            
            for channel_data in channels:
                channel_info = channel_data['channel']
                if channel_info.get('isDeletedAtSource'):
                    stats["channels"]["archived"] += 1
                elif channel_info.get('groupType') == 'PRIVATE_CHANNEL':
                    stats["channels"]["private"] += 1
                else:
                    stats["channels"]["public"] += 1

            return stats

        except Exception as e:
            self.logger.error("❌ Error getting workspace statistics: %s", str(e))
            return stats

    async def get_sync_health(self, workspace_id: str) -> Dict[str, Any]:
        """Get sync health status"""
        try:
            health_status = {
                "status": "healthy",
                "last_sync": None,
                "sync_lag_minutes": 0,
                "error_rate": 0.0,
                "issues": []
            }

            # Get last sync metadata
            sync_metadata = await self.arango_service.get_sync_metadata("incremental")
            
            if sync_metadata:
                health_status["last_sync"] = sync_metadata.get('lastSyncTimestamp')
                
                # Calculate sync lag
                current_time = int(datetime.now(timezone.utc).timestamp() * 1000)
                if sync_metadata.get('lastSyncTimestamp'):
                    lag_ms = current_time - sync_metadata['lastSyncTimestamp']
                    health_status["sync_lag_minutes"] = lag_ms / (1000 * 60)
                
                # Calculate error rate
                processed = sync_metadata.get('lastSyncProcessed', 0)
                errors = sync_metadata.get('lastSyncErrors', 0)
                if processed > 0:
                    health_status["error_rate"] = errors / processed

            # Determine overall health status
            if health_status["sync_lag_minutes"] > 60:  # More than 1 hour lag
                health_status["status"] = "degraded"
                health_status["issues"].append("High sync lag detected")
            
            if health_status["error_rate"] > 0.1:  # More than 10% error rate
                health_status["status"] = "unhealthy"
                health_status["issues"].append("High error rate detected")

            return health_status

        except Exception as e:
            self.logger.error("❌ Error getting sync health: %s", str(e))
            return {
                "status": "error",
                "error": str(e),
                "issues": ["Unable to determine sync health"]
            }

    async def store_workspace_data(self, workspace_data: dict, org_id: str, transaction=None) -> Optional[dict]:
        """Store workspace data and create necessary relationships"""
        try:
            # Store workspace
            await self.arango_service.batch_upsert_nodes([workspace_data], CollectionNames.SLACK_WORKSPACES.value, transaction)

            # Create edge between workspace and organization
            workspace_org_edge = {
                "_from": f"{CollectionNames.SLACK_WORKSPACES.value}/{workspace_data['_key']}",
                "_to": f"{CollectionNames.ORGS.value}/{org_id}",
                "entityType": "WORKSPACE",
                "createdAtTimestamp": get_epoch_timestamp_in_ms(),
                "updatedAtTimestamp": get_epoch_timestamp_in_ms()
            }
            
            await self.arango_service.batch_create_edges(
                [workspace_org_edge],
                collection=CollectionNames.BELONGS_TO.value,
                transaction=transaction
            )
            
            return workspace_data
            
        except Exception as e:
            self.logger.error("❌ Error storing workspace data: %s", str(e))
            if transaction:
                raise
            return None