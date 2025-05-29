"""Slack ArangoDB service """

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from arango import ArangoClient
from arango.database import TransactionDatabase

from app.config.utils.named_constants.arangodb_constants import (
    CollectionNames,
    Connectors,
    GroupType,
    MessageType,
    OriginTypes,
    ProgressStatus,
    RecordTypes,
)
from app.connectors.core.base_arango_service import BaseArangoService
from app.config.configuration_service import ConfigurationService
from app.utils.time_conversion import get_epoch_timestamp_in_ms


class SlackArangoService(BaseArangoService):
    """Updated Slack ArangoDB service for interacting with the database"""

    def __init__(self, logger, arango_client: ArangoClient, kafka_service, config: ConfigurationService):
        super().__init__(logger, arango_client, kafka_service, config)
        self.kafka_service = kafka_service
        self.logger = logger

    # ==================== WORKSPACE SYNC STATE METHODS ====================

    async def get_workspace_sync_state(self, org_id: str) -> Optional[Dict[str, Any]]:
        """Get sync state for a workspace"""
        try:
            query = """
            FOR workspace IN slackWorkspaces
                FILTER workspace.orgId == @org_id
                RETURN {
                    workspace: workspace,
                    syncState: workspace.syncState,
                    lastSyncTimestamp: workspace.lastSyncTimestamp
                }
            """
            
            cursor = self.db.aql.execute(
                query,
                bind_vars={"org_id": org_id}
            )
            return next(cursor, None)

        except Exception as e:
            self.logger.error("❌ Error getting workspace sync state: %s", str(e))
            return None

    async def create_workspace_sync_state(self, org_id: str, sync_state: str) -> bool:
        """Create initial workspace sync state"""
        try:
            timestamp = get_epoch_timestamp_in_ms()
            
            # Update existing workspace or create if doesn't exist
            query = """
            FOR workspace IN slackWorkspaces
                FILTER workspace.orgId == @org_id
                UPDATE workspace WITH {
                    syncState: @sync_state,
                    lastSyncTimestamp: @timestamp,
                    updatedAtTimestamp: @timestamp
                } IN slackWorkspaces
                RETURN NEW
            """
            
            cursor = self.db.aql.execute(
                query,
                bind_vars={
                    "org_id": org_id,
                    "sync_state": sync_state,
                    "timestamp": timestamp
                }
            )
            
            result = next(cursor, None)
            return bool(result)

        except Exception as e:
            self.logger.error("❌ Error creating workspace sync state: %s", str(e))
            return False

    async def update_workspace_sync_state(self, org_id: str, sync_state: str) -> bool:
        """Update workspace sync state"""
        try:
            timestamp = get_epoch_timestamp_in_ms()
            
            query = """
            FOR workspace IN slackWorkspaces
                FILTER workspace.orgId == @org_id
                UPDATE workspace WITH {
                    syncState: @sync_state,
                    lastSyncTimestamp: @timestamp,
                    updatedAtTimestamp: @timestamp
                } IN slackWorkspaces
                RETURN NEW
            """
            
            cursor = self.db.aql.execute(
                query,
                bind_vars={
                    "org_id": org_id,
                    "sync_state": sync_state,
                    "timestamp": timestamp
                }
            )
            
            result = next(cursor, None)
            return bool(result)

        except Exception as e:
            self.logger.error("❌ Error updating workspace sync state: %s", str(e))
            return False

    async def get_last_sync_timestamp(self, org_id: str) -> Optional[int]:
        """Get last sync timestamp for workspace"""
        try:
            query = """
            FOR workspace IN slackWorkspaces
                FILTER workspace.orgId == @org_id
                RETURN workspace.lastSyncTimestamp
            """
            
            cursor = self.db.aql.execute(
                query,
                bind_vars={"org_id": org_id}
            )
            return next(cursor, None)

        except Exception as e:
            self.logger.error("❌ Error getting last sync timestamp: %s", str(e))
            return None

    # async def get_last_shutdown_time(self) -> Optional[float]:
    #     """Get the timestamp of the last shutdown"""
    #     try:
    #         query = """
    #         FOR doc IN system_records
    #             FILTER doc.recordType == 'SHUTDOWN_TIME'
    #             SORT doc.timestamp DESC
    #             LIMIT 1
    #             RETURN doc.timestamp
    #         """
            
    #         cursor = self.db.aql.execute(query)
    #         return next(cursor, None)
            
    #     except Exception as e:
    #         self.logger.error("❌ Error getting last shutdown time: %s", str(e))
    #         return None

    # ==================== BATCH OPERATIONS ====================

    async def batch_upsert_nodes(
        self,
        nodes: List[Dict],
        collection: str,
        transaction: Optional[TransactionDatabase] = None,
    ):
        """Batch upsert multiple nodes using Python-Arango SDK methods"""
        try:
            self.logger.info("🚀 Batch upserting nodes: %s", collection)

            batch_query = """
            FOR node IN @nodes
                UPSERT { _key: node._key }
                INSERT node
                UPDATE node
                IN @@collection
                RETURN NEW
            """

            bind_vars = {"nodes": nodes, "@collection": collection}

            db = transaction if transaction else self.db

            cursor = db.aql.execute(batch_query, bind_vars=bind_vars)
            results = list(cursor)
            self.logger.info(
                "✅ Successfully upserted %d nodes in collection '%s'.",
                len(results),
                collection,
            )
            return True

        except Exception as e:
            self.logger.error("❌ Batch upsert failed: %s", str(e))
            if transaction:
                raise
            return False

    async def batch_create_edges(
        self,
        edges: List[Dict],
        collection: str,
        transaction: Optional[TransactionDatabase] = None,
    ):
        """Batch create edges"""
        try:
            self.logger.info("🚀 Batch creating edges: %s", collection)

            batch_query = """
            FOR edge IN @edges
                UPSERT { _from: edge._from, _to: edge._to }
                INSERT edge
                UPDATE edge
                IN @@collection
                RETURN NEW
            """
            bind_vars = {"edges": edges, "@collection": collection}

            db = transaction if transaction else self.db

            cursor = db.aql.execute(batch_query, bind_vars=bind_vars)
            results = list(cursor)
            self.logger.info(
                "✅ Successfully created %d edges in collection '%s'.",
                len(results),
                collection,
            )
            return True
        except Exception as e:
            self.logger.error("❌ Batch edge creation failed: %s", str(e))
            if transaction:
                raise
            return False

    # ==================== SYNC METADATA OPERATIONS ====================

    # No records such as sync metadata

    async def get_sync_metadata(
        self,
        entity_type: str,
        transaction: Optional[TransactionDatabase] = None
    ) -> Optional[Dict]:
        """Get sync metadata"""
        try:
            query = """
            FOR meta IN sync_metadata
            FILTER meta._key == @entity_type
            RETURN meta
            """
            
            db = transaction if transaction else self.db
            cursor = db.aql.execute(
                query,
                bind_vars={
                    "entity_type": entity_type
                }
            )
            return next(cursor, None)

        except Exception as e:
            self.logger.error("❌ Error getting sync metadata: %s", str(e))
            return None

    async def store_sync_metadata(
        self,
        entity_type: str,
        metadata: Dict[str, Any],
        transaction: Optional[TransactionDatabase] = None
    ) -> bool:
        """Store sync metadata"""
        try:
            timestamp = get_epoch_timestamp_in_ms()
            
            sync_record = {
                "_key": entity_type,
                "entityType": entity_type,
                "lastSyncTimestamp": metadata.get("lastSyncTimestamp"),
                "lastSyncProcessed": metadata.get("lastSyncProcessed", 0),
                "lastSyncErrors": metadata.get("lastSyncErrors", 0),
                "createdAtTimestamp": timestamp,
                "updatedAtTimestamp": timestamp,
            }
            
            query = """
            UPSERT { _key: @entity_type }
            INSERT @record
            UPDATE @record
            IN sync_metadata
            RETURN NEW
            """
            
            db = transaction if transaction else self.db
            cursor = db.aql.execute(
                query,
                bind_vars={
                    "entity_type": entity_type,
                    "record": sync_record
                }
            )
            
            result = next(cursor, None)
            return bool(result)

        except Exception as e:
            self.logger.error("❌ Error storing sync metadata: %s", str(e))
            if transaction:
                raise
            return False

    # ==================== WORKSPACE OPERATIONS ====================

    async def store_slack_workspace(
        self,
        workspace_data: Dict[str, Any],
        org_id: str,
        transaction: Optional[TransactionDatabase] = None
    ) -> Optional[Dict[str, Any]]:
        """Store Slack workspace information"""
        try:
            self.logger.info("🚀 Storing Slack workspace: %s", workspace_data.get('name'))
            workspace_id = str(uuid.uuid4())

            workspace_doc = {
                "_key": workspace_id,
                "orgId": org_id,
                "externalId": workspace_data.get('id'),
                "name": workspace_data.get('name'),
                "domain": workspace_data.get('domain'),
                "emailDomain": workspace_data.get('email_domain'),
                "url": workspace_data.get('url'),
                "isActive": True,
                "syncState": ProgressStatus.NOT_STARTED.value,
                "lastSyncTimestamp": None,
                "createdAtTimestamp": get_epoch_timestamp_in_ms(),
                "updatedAtTimestamp": get_epoch_timestamp_in_ms(),
            }

            await self.batch_upsert_nodes([workspace_doc], CollectionNames.SLACK_WORKSPACES.value, transaction)
            self.logger.info("✅ SlackWorkspace stored successfully")
            return workspace_doc

        except Exception as e:
            self.logger.error("❌ Error storing slack workspace: %s", str(e))
            if transaction:
                raise
            return None

    async def update_workspace(self, workspace_key: str, update_fields: dict, transaction=None) -> bool:
        """Update fields for an existing workspace by _key"""
        try:
            query = """
            UPDATE @workspace_key WITH @update_fields IN slackWorkspaces RETURN NEW
            """
            db = transaction if transaction else self.db
            cursor = db.aql.execute(query, bind_vars={"workspace_key": workspace_key, "update_fields": update_fields})
            result = next(cursor, None)
            return bool(result)
        except Exception as e:
            self.logger.error(f"❌ Error updating workspace {workspace_key}: {str(e)}")
            return False

    # ==================== USER OPERATIONS ====================

    async def store_slack_users_batch(
        self,
        users_data: List[Dict[str, Any]],
        org_id: str,
        transaction: Optional[TransactionDatabase] = None
    ) -> bool:
        """Batch store Slack users"""
        try:
            self.logger.info("🚀 Batch storing %d Slack users", len(users_data))
            
            timestamp = get_epoch_timestamp_in_ms()
            user_records = []

            for user_data in users_data:
                user_id = str(uuid.uuid4())
                
                user_record = {
                    "_key": user_id,
                    "userId": user_data.get('id'),
                    "orgId": org_id,
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
                user_records.append(user_record)

            db = transaction if transaction else self.db
            success = await self.batch_upsert_nodes(user_records, CollectionNames.USERS.value, db)
            
            if success:
                self.logger.info("✅ Batch stored %d Slack users successfully", len(user_records))
            return success

        except Exception as e:
            self.logger.error("❌ Error batch storing slack users: %s", str(e))
            if transaction:
                raise
            return False

    async def get_user_by_slack_id(self, slack_user_id: str, org_id: str, transaction: Optional[TransactionDatabase] = None) -> Optional[Dict]:
        """Get Slack user by slack id"""
        try:
            query = """
            FOR user IN users
            FILTER user.userId == @slack_user_id AND user.orgId == @org_id
            RETURN user
            """
            db = transaction if transaction else self.db
            cursor = db.aql.execute(
                query,
                bind_vars={
                    "slack_user_id": slack_user_id,
                    "org_id": org_id
                }
            )

            result = next(cursor, None)
            if result:
                self.logger.info("✅ SlackUser retrieved successfully")
            return result

        except Exception as e:
            self.logger.error("❌ Error retrieving slack user: %s", str(e))
            return None

    async def update_user(self, user_key: str, update_fields: dict, transaction=None) -> bool:
        """Update fields for an existing user by _key"""
        try:
            query = """
            UPDATE @user_key WITH @update_fields IN users RETURN NEW
            """
            db = transaction if transaction else self.db
            cursor = db.aql.execute(query, bind_vars={"user_key": user_key, "update_fields": update_fields})
            result = next(cursor, None)
            return bool(result)
        except Exception as e:
            self.logger.error(f"❌ Error updating user {user_key}: {str(e)}")
            return False

    # ==================== CHANNEL OPERATIONS ====================

    async def store_slack_channels_batch(
        self,
        channels_data: List[Dict[str, Any]],
        org_id: str,
        transaction: Optional[TransactionDatabase] = None
    ) -> bool:
        """Batch store Slack channels as record groups and create belongsTo edges to workspace"""
        try:
            self.logger.info("🚀 Batch storing %d Slack channels", len(channels_data))
            
            timestamp = get_epoch_timestamp_in_ms()
            channel_records = []
            belongs_to_edges = []

            # Fetch workspace key for belongsTo edge
            workspace = await self.get_workspace_by_org_id(org_id, transaction)
            if not workspace:
                self.logger.error(f"❌ Workspace not found for org_id: {org_id}")
                return False
            workspace_key = workspace.get("_key")

            for channel_data in channels_data:
                channel_id = str(uuid.uuid4())
                
                group_record = {
                    "_key": channel_id,
                    "orgId": org_id,
                    "groupName": channel_data.get('name', 'Unnamed Channel'),
                    "externalGroupId": channel_data.get('id'),  # Slack channel ID as external ID
                    "groupType": GroupType.SLACK_CHANNEL.value,
                    "connectorName": Connectors.SLACK.value,
                    "createdAtTimestamp": timestamp,
                    "updatedAtTimestamp": timestamp,
                    "lastSyncTimestamp": timestamp,
                    "isDeletedAtSource": channel_data.get('is_archived', False),
                    "sourceCreatedAtTimestamp": channel_data.get('created'),
                    "sourceUpdatedAtTimestamp": channel_data.get('updated'),
                    "syncState": ProgressStatus.NOT_STARTED.value,
                }
                channel_records.append(group_record)
                # Prepare belongsTo edge from channel to workspace
                belongs_to_edges.append({
                    "_from": f"{CollectionNames.RECORD_GROUPS.value}/{channel_id}",
                    "_to": f"{CollectionNames.SLACK_WORKSPACES.value}/{workspace_key}",
                    "entityType": "WORKSPACE",
                    "createdAtTimestamp": timestamp,
                    "updatedAtTimestamp": timestamp,
                })

            db = transaction if transaction else self.db
            success = await self.batch_upsert_nodes(channel_records, CollectionNames.RECORD_GROUPS.value, db)
            if success and belongs_to_edges:
                await self.batch_create_edges(belongs_to_edges, CollectionNames.BELONGS_TO_SLACK_WORKSPACE.value, db)
                self.logger.info("✅ Batch stored %d Slack channels and created belongsTo edges successfully", len(channel_records))
            return success

        except Exception as e:
            self.logger.error("❌ Error batch storing slack channels: %s", str(e))
            if transaction:
                raise
            return False

    async def get_workspace_by_org_id(self, org_id: str, transaction: Optional[TransactionDatabase] = None) -> Optional[Dict[str, Any]]:
        """Get workspace document by org_id"""
        try:
            query = """
            FOR workspace IN slackWorkspaces
                FILTER workspace.orgId == @org_id
                RETURN workspace
            """
            db = transaction if transaction else self.db
            cursor = db.aql.execute(query, bind_vars={"org_id": org_id})
            return next(cursor, None)
        except Exception as e:
            self.logger.error("❌ Error getting workspace by org_id: %s", str(e))
            return None

    async def get_channel_by_slack_id(self, slack_channel_id: str, org_id: str, transaction: Optional[TransactionDatabase] = None) -> Optional[Dict]:
        """Get Slack channel by slack id"""
        try:
            query = """
            FOR recordGroup IN recordGroups
            FILTER recordGroup.externalGroupId == @slack_channel_id AND recordGroup.orgId == @org_id
            RETURN recordGroup
            """
            db = transaction if transaction else self.db
            cursor = db.aql.execute(
                query,
                bind_vars={
                    "slack_channel_id": slack_channel_id,
                    "org_id": org_id
                }
            )

            result = next(cursor, None)
            if result:
                self.logger.info("✅ Slack Channel retrieved successfully")
            return result

        except Exception as e:
            self.logger.error("❌ Error retrieving slack channel: %s", str(e))
            return None

    async def get_workspace_channels(self, org_id: str) -> List[Dict]:
        """Get all channels for a workspace"""
        try:
            query = """
            FOR recordGroup IN recordGroups
            FILTER recordGroup.orgId == @org_id 
                AND recordGroup.groupType == @group_type
                AND recordGroup.connectorName == @connector_name
            RETURN {
                "channel": recordGroup
            }
            """
            cursor = self.db.aql.execute(
                query,
                bind_vars={
                    "org_id": org_id,
                    "group_type": GroupType.SLACK_CHANNEL.value,
                    "connector_name": Connectors.SLACK.value
                }
            )
            return list(cursor)

        except Exception as e:
            self.logger.error("❌ Error getting workspace channels: %s", str(e))
            return []

    async def update_channel_sync_state(
        self,
        channel_key: str,
        sync_state: str,
        transaction: Optional[TransactionDatabase] = None
    ) -> bool:
        """Update channel sync state"""
        try:
            query = """
            UPDATE @channel_key WITH {
                syncState: @sync_state,
                updatedAtTimestamp: @timestamp
            } IN recordGroups
            RETURN NEW
            """

            db = transaction if transaction else self.db
            cursor = db.aql.execute(
                query,
                bind_vars={
                    "channel_key": channel_key,
                    "sync_state": sync_state,
                    "timestamp": get_epoch_timestamp_in_ms()
                }
            )
            
            result = next(cursor, None)
            return bool(result)

        except Exception as e:
            self.logger.error("❌ Error updating channel sync state: %s", str(e))
            if transaction:
                raise
            return False

    async def update_channel(self, channel_key: str, update_fields: dict, transaction=None) -> bool:
        """Update fields for an existing channel by _key"""
        try:
            query = """
            UPDATE @channel_key WITH @update_fields IN recordGroups RETURN NEW
            """
            db = transaction if transaction else self.db
            cursor = db.aql.execute(query, bind_vars={"channel_key": channel_key, "update_fields": update_fields})
            result = next(cursor, None)
            return bool(result)
        except Exception as e:
            self.logger.error(f"❌ Error updating channel {channel_key}: {str(e)}")
            return False

    # ==================== MESSAGE OPERATIONS ====================

    async def store_slack_messages_batch(
        self,
        messages_data: List[Dict[str, Any]],
        org_id: str,
        transaction: Optional[TransactionDatabase] = None
    ) -> Dict[str, List[Dict]]:
        """Batch store Slack messages with metadata and create edges"""
        try:
            self.logger.info("🚀 Batch storing %d Slack messages", len(messages_data))
            
            timestamp = get_epoch_timestamp_in_ms()
            message_records = []
            message_metadata = []
            message_to_metadata_edges = []
            attachment_edges = []

            for message_data in messages_data:
                message_id = str(uuid.uuid4())
                
                # Core message record
                message_record = {
                    "_key": message_id,
                    "orgId": org_id,
                    "recordName": f"Slack_Message___{message_data.get('ts')}",
                    "externalRecordId": message_data.get('ts'),
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
                    "sourceCreatedAtTimestamp": int(float(message_data.get('ts', 0)) * 1000),
                    "sourceLastModifiedTimestamp": int(float(message_data.get('edited', {}).get('ts', 0)) * 1000) if message_data.get('edited') else int(float(message_data.get('ts', 0)) * 1000),
                    "isDeleted": False,
                    "isArchived": False,
                    "reason": None,
                    "lastIndexTimestamp": None,
                    "lastExtractionTimestamp": None,
                    "indexingStatus": ProgressStatus.NOT_STARTED.value,
                    "extractionStatus": ProgressStatus.NOT_STARTED.value,
                }
                message_records.append(message_record)

                # Message-specific metadata
                metadata = {
                    "_key": message_id,
                    "slackTs": message_data.get('ts'),
                    "threadTs": message_data.get('thread_ts'),
                    "orgId": org_id,
                    "text": message_data.get('text', ''),
                    "channelId": message_data.get('channel'),
                    "userId": message_data.get('user'),
                    "messageType": (
                        "bot_message" if message_data.get('bot_id')
                        else "thread_message" if message_data.get('thread_ts')
                        else "root_message"
                    ),
                    "replyCount": message_data.get('reply_count', 0),
                    "replyUsersCount": message_data.get('reply_users_count', 0),
                    "replyUsers": message_data.get('reply_users', []),
                    "hasFiles": bool(message_data.get('files')),
                    "fileCount": len(message_data.get('files', [])),
                    "botId": message_data.get('bot_id'),
                    "mentionedUsers": self._extract_mentions(message_data),
                    "links": self._extract_links(message_data),
                }
                message_metadata.append(metadata)

                # Edge: message record -> metadata
                message_to_metadata_edges.append({
                    "_from": f"{CollectionNames.RECORDS.value}/{message_id}",
                    "_to": f"{CollectionNames.SLACK_MESSAGE_METADATA.value}/{message_id}",
                    "createdAtTimestamp": timestamp,
                    "updatedAtTimestamp": timestamp
                })

                # Edges: message record -> file records (attachments)
                for file in message_data.get('files', []):
                    file_id = file.get('id')
                    if file_id:
                        attachment_edges.append({
                            "_from": f"{CollectionNames.RECORDS.value}/{message_id}",
                            "_to": f"{CollectionNames.RECORDS.value}/{file_id}",
                            "relationshipType": "ATTACHMENT",
                            "createdAtTimestamp": timestamp,
                            "updatedAtTimestamp": timestamp,
                        })

            db = transaction if transaction else self.db
            records_success = await self.batch_upsert_nodes(message_records, CollectionNames.RECORDS.value, db)
            metadata_success = await self.batch_upsert_nodes(message_metadata, CollectionNames.SLACK_MESSAGE_METADATA.value, db)

            if records_success and metadata_success:
                # Create edges in batch
                if message_to_metadata_edges:
                    await self.batch_create_edges(message_to_metadata_edges, CollectionNames.SLACK_MESSAGE_TO_METADATA.value, db)
                if attachment_edges:
                    await self.batch_create_edges(attachment_edges, CollectionNames.RECORD_RELATIONS.value, db)
                self.logger.info("✅ Batch stored %d messages and created edges successfully", len(message_records))
                return {
                    "records": message_records,
                    "metadata": message_metadata
                }
            else:
                raise Exception("Failed to store messages or metadata")

        except Exception as e:
            self.logger.error("❌ Error batch storing messages: %s", str(e))
            if transaction:
                raise
            return {"records": [], "metadata": []}

    async def get_message_by_slack_ts(
        self,
        slack_ts: str,
        org_id: str,
        transaction: Optional[TransactionDatabase] = None
    ) -> Optional[Dict[str, Any]]:
        """Get message by Slack timestamp"""
        try:
            query = """
            FOR metadata IN slackMessageMetdata
                FILTER metadata.slackTs == @slack_ts AND metadata.orgId == @org_id
                LET message = DOCUMENT(CONCAT('records/', metadata._key))
                RETURN {
                    message: message,
                    metadata: metadata
                }
            """
            
            db = transaction if transaction else self.db
            cursor = db.aql.execute(
                query,
                bind_vars={
                    "slack_ts": slack_ts,
                    "org_id": org_id
                }
            )
            return next(cursor, None)

        except Exception as e:
            self.logger.error("❌ Error getting message by timestamp: %s", str(e))
            return None

    # ==================== FILE OPERATIONS ====================

    async def store_slack_files_batch(
        self,
        files_data: List[Dict[str, Any]],
        org_id: str,
        transaction: Optional[TransactionDatabase] = None
    ) -> Dict[str, List[Dict]]:
        """Batch store Slack files with metadata and create edges"""
        try:
            self.logger.info("🚀 Batch storing %d Slack files", len(files_data))
            
            timestamp = get_epoch_timestamp_in_ms()
            file_records = []
            file_metadata = []
            file_to_metadata_edges = []

            for file_data in files_data:
                file_id = str(uuid.uuid4())
                
                # Core file record
                file_record = {
                    "_key": file_id,
                    "orgId": org_id,
                    "recordName": file_data.get('name', 'Unnamed File'),
                    "externalRecordId": file_data.get('id'),
                    "recordType": RecordTypes.ATTACHMENT.value,
                    "version": 0,
                    "origin": OriginTypes.CONNECTOR.value,
                    "connectorName": Connectors.SLACK.value,
                    "isLatestVersion": True,
                    "createdAtTimestamp": timestamp,
                    "updatedAtTimestamp": timestamp,
                    "lastSyncTimestamp": timestamp,
                    "sourceCreatedAtTimestamp": int(float(file_data.get('created', 0)) * 1000),
                    "sourceLastModifiedTimestamp": int(float(file_data.get('updated', file_data.get('created', 0))) * 1000),
                    "isDeleted": False,
                    "isArchived": False,
                    "isDirty": False,
                    "indexingStatus": ProgressStatus.NOT_STARTED.value,
                    "extractionStatus": ProgressStatus.NOT_STARTED.value,
                    "lastIndexTimestamp": None,
                    "lastExtractionTimestamp": None,
                    "reason": None,
                }
                file_records.append(file_record)

                # File-specific metadata
                metadata = {
                    "_key": file_id,
                    "orgId": org_id,
                    "slackFileId": file_data.get('id'),
                    "name": file_data.get('name'),
                    "extension": file_data.get('filetype'),
                    "mimeType": file_data.get('mimetype'),
                    "sizeInBytes": file_data.get('size'),
                    "channelId": file_data.get('channel'),
                    "uploadedBy": file_data.get('user'),
                    "sourceUrls": {
                        "privateUrl": file_data.get('url_private'),
                        "downloadUrl": file_data.get('url_private_download'),
                        "publicPermalink": file_data.get('permalink')
                    },
                    "isPublic": file_data.get('is_public', False),
                    "isEditable": file_data.get('editable', False),
                    "permalink": file_data.get('permalink'),
                    "richPreview": file_data.get('has_rich_preview', False)
                }
                file_metadata.append(metadata)

                # Edge: file record -> attachment metadata
                file_to_metadata_edges.append({
                    "_from": f"{CollectionNames.RECORDS.value}/{file_id}",
                    "_to": f"{CollectionNames.SLACK_ATTACHMENT_METADATA.value}/{file_id}",
                    "createdAtTimestamp": timestamp,
                    "updatedAtTimestamp": timestamp
                })

            db = transaction if transaction else self.db
            records_success = await self.batch_upsert_nodes(file_records, CollectionNames.RECORDS.value, db)
            metadata_success = await self.batch_upsert_nodes(file_metadata, CollectionNames.SLACK_ATTACHMENT_METADATA.value, db)

            if records_success and metadata_success:
                # Create edges in batch
                if file_to_metadata_edges:
                    await self.batch_create_edges(file_to_metadata_edges, CollectionNames.SLACK_FILE_TO_ATTACHMENT_METADATA.value, db)
                self.logger.info("✅ Batch stored %d files and created edges successfully", len(file_records))
                return {
                    "records": file_records,
                    "metadata": file_metadata
                }
            else:
                raise Exception("Failed to store files or metadata")

        except Exception as e:
            self.logger.error("❌ Error batch storing files: %s", str(e))
            if transaction:
                raise
            return {"records": [], "metadata": []}

    async def get_file_by_slack_id(
        self,
        slack_file_id: str,
        org_id: str,
        transaction: Optional[TransactionDatabase] = None
    ) -> Optional[Dict[str, Any]]:
        """Get file by Slack ID"""
        try:
            query = """
            FOR metadata IN slackAttachmentMetdata
                FILTER metadata.slackFileId == @slack_file_id AND metadata.orgId == @org_id  
                LET file = DOCUMENT(CONCAT('records/', metadata._key))
                RETURN {
                    file: file,
                    metadata: metadata
                }
            """
            
            db = transaction if transaction else self.db
            cursor = db.aql.execute(
                query,
                bind_vars={
                    "slack_file_id": slack_file_id,
                    "org_id": org_id
                }
            )
            return next(cursor, None)

        except Exception as e:
            self.logger.error("❌ Error getting file by Slack ID: %s", str(e))
            return None

    async def get_file_metadata(self, record_key: str) -> Optional[Dict[str, Any]]:
        """Get file metadata by record key"""
        try:
            query = """
            FOR metadata IN slackAttachmentMetdata
                FILTER metadata._key == @record_key
                RETURN metadata
            """
            
            cursor = self.db.aql.execute(
                query,
                bind_vars={"record_key": record_key}
            )
            return next(cursor, None)

        except Exception as e:
            self.logger.error("❌ Error getting file metadata: %s", str(e))
            return None

    # ==================== RELATIONSHIP OPERATIONS ====================

    async def create_is_of_type_edges_batch(
        self,
        source_target_pairs: List[Tuple[str, str, str]],  # (source_collection, target_collection, key)
        transaction: Optional[TransactionDatabase] = None
    ) -> bool:
        """Create is_of_type edges in batch"""
        try:
            self.logger.info("🚀 Creating %d is_of_type edges", len(source_target_pairs))
            
            timestamp = get_epoch_timestamp_in_ms()
            edges = []

            for source_collection, target_collection, key in source_target_pairs:
                edge = {
                    "_from": f"{source_collection}/{key}",
                    "_to": f"{target_collection}/{key}",
                    "createdAtTimestamp": timestamp,
                    "updatedAtTimestamp": timestamp,
                }
                edges.append(edge)

            db = transaction if transaction else self.db
            success = await self.batch_create_edges(edges, CollectionNames.IS_OF_TYPE.value, db)
            
            if success:
                self.logger.info("✅ Created %d is_of_type edges successfully", len(edges))
            return success

        except Exception as e:
            self.logger.error("❌ Error creating is_of_type edges: %s", str(e))
            if transaction:
                raise
            return False

    async def create_channel_member_edges_batch(
        self,
        memberships: List[Dict[str, Any]],
        transaction: Optional[TransactionDatabase] = None
    ) -> bool:
        """Create channel membership edges in batch using permissions collection"""
        try:
            self.logger.info("🚀 Creating %d channel membership edges", len(memberships))
            
            timestamp = get_epoch_timestamp_in_ms()
            edges = []

            for membership in memberships:
                edge = {
                    "_from": f"{CollectionNames.RECORD_GROUPS.value}/{membership['channel_key']}",
                    "_to": f"{CollectionNames.USERS.value}/{membership['user_key']}",
                    "role": membership.get('role', 'READER'),
                    "type": "USER",
                    "externalPermissionId": None,
                    "createdAtTimestamp": timestamp,
                    "updatedAtTimestamp": timestamp,
                    "lastUpdatedTimestampAtSource": timestamp,
                }
                edges.append(edge)

            db = transaction if transaction else self.db
            success = await self.batch_create_edges(edges, CollectionNames.BELONGS_TO_SLACK_CHANNEL.value, db)
            
            if success:
                self.logger.info("✅ Created %d membership edges successfully", len(edges))
            return success

        except Exception as e:
            self.logger.error("❌ Error creating membership edges: %s", str(e))
            if transaction:
                raise
            return False

    # ==================== FAILED RECORDS OPERATIONS ====================

    async def get_failed_records(self, org_id: str, connector_name: str) -> List[Dict[str, Any]]:
        """Get all failed records for a connector"""
        try:
            query = """
            FOR doc IN records
                FILTER doc.orgId == @org_id
                AND doc.indexingStatus == "FAILED"
                AND doc.connectorName == @connector_name
                RETURN doc
            """
            
            cursor = self.db.aql.execute(
                query,
                bind_vars={
                    "org_id": org_id,
                    "connector_name": connector_name
                }
            )
            return list(cursor)

        except Exception as e:
            self.logger.error("❌ Error getting failed records: %s", str(e))
            return []

    # ==================== UTILITY METHODS ====================

    def _extract_mentions(self, message_data: Dict[str, Any]) -> List[str]:
        """Extract user mentions from message"""
        mentions = []
        text = message_data.get('text', '')
        
        # Basic extraction - would need more sophisticated parsing
        import re
        user_mentions = re.findall(r'<@([A-Z0-9]+)>', text)
        mentions.extend(user_mentions)
        
        return mentions

    def _extract_links(self, message_data: Dict[str, Any]) -> List[str]:
        """Extract links from message"""
        links = []
        text = message_data.get('text', '')
        
        # Basic extraction - would need more sophisticated parsing
        import re
        url_pattern = r'<(https?://[^>]+)(?:\|[^>]+)?>'
        urls = re.findall(url_pattern, text)
        links.extend(urls)
        
        return links

    async def get_entity_id_by_email(
        self,
        email: str,
        transaction: Optional[TransactionDatabase] = None
    ) -> Optional[str]:
        """Get user or group ID by email address"""
        try:
            self.logger.info("🚀 Getting Entity Key by email")

            # First check users collection
            query = """
            FOR doc IN users
                FILTER doc.email == @email
                RETURN doc._key
            """
            db = transaction if transaction else self.db
            result = db.aql.execute(
                query,
                bind_vars={"email": email}
            )
            user_id = next(result, None)
            if user_id:
                self.logger.info("✅ Got User ID: %s", user_id)
                return user_id

            # If not found in users, check groups collection
            query = """
            FOR doc IN groups
                FILTER doc.email == @email
                RETURN doc._key
            """
            result = db.aql.execute(
                query,
                bind_vars={"email": email}
            )
            group_id = next(result, None)
            if group_id:
                self.logger.info("✅ Got group ID: %s", group_id)
                return group_id

            return None

        except Exception as e:
            self.logger.error("❌ Failed to get entity ID for email %s: %s", email, str(e))
            return None

    async def get_key_by_external_message_id(
        self,
        external_message_id: str,
        transaction: Optional[TransactionDatabase] = None,
    ) -> Optional[str]:
        """Get internal message key using the external message ID"""
        try:
            self.logger.info("🚀 Retrieving internal key for external message ID %s", external_message_id)

            query = """
            FOR record IN records
                FILTER record.externalRecordId == @external_message_id
                RETURN record._key
            """

            db = transaction if transaction else self.db
            cursor = db.aql.execute(
                query,
                bind_vars={"external_message_id": external_message_id}
            )
            result = next(cursor, None)

            if result:
                self.logger.info("✅ Successfully retrieved internal key for external message ID %s", external_message_id)
                return result
            else:
                self.logger.warning("⚠️ No internal key found for external message ID %s", external_message_id)
                return None

        except Exception as e:
            self.logger.error("❌ Failed to retrieve internal key for external message ID %s: %s", external_message_id, str(e))
            return None

    async def get_key_by_external_file_id(
        self,
        external_file_id: str,
        transaction: Optional[TransactionDatabase] = None,
    ) -> Optional[str]:
        """Get internal file key using the external file ID"""
        try:
            self.logger.info("🚀 Retrieving internal key for external file ID %s", external_file_id)

            query = """
            FOR record IN records
                FILTER record.externalRecordId == @external_file_id
                RETURN record._key
            """

            db = transaction if transaction else self.db
            cursor = db.aql.execute(
                query,
                bind_vars={"external_file_id": external_file_id}
            )
            result = next(cursor, None)

            if result:
                self.logger.info("✅ Successfully retrieved internal key for external file ID %s", external_file_id)
                return result
            else:
                self.logger.warning("⚠️ No internal key found for external file ID %s", external_file_id)
                return None

        except Exception as e:
            self.logger.error("❌ Failed to retrieve internal key for external file ID %s: %s", external_file_id, str(e))
            return None