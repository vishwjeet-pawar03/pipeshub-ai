import json
from logging import Logger

from aiokafka import AIOKafkaProducer # type: ignore

from app.config.configuration_service import ConfigurationService, config_node_constants
from app.config.utils.named_constants.arangodb_constants import EventTypes
from app.utils.time_conversion import get_epoch_timestamp_in_ms
from app.connectors.services.messaging.interface.messaging_service import IMessagingService
from app.connectors.services.messaging.messaging_factory import MessagingFactory
from app.connectors.sources.google.common.arango_service import ArangoService
from app.connectors.sources.google.common.sync_tasks import SyncTasks


class KafkaService(IMessagingService):
    def __init__(self, 
                config: ConfigurationService, 
                sync_tasks: SyncTasks, 
                arango_service: ArangoService, 
                logger: Logger) -> None:
        self.config_service = config
        self.producer = MessagingFactory.create_producer(
            "kafka", 
            config_service=config, 
            sync_tasks=sync_tasks, 
            arango_service=arango_service, 
            logger=logger
        )
        self.logger = logger

    async def send_event_to_kafka(self, event_data) -> bool | None:
        """
        Send an event to Kafka asynchronously.
        :param event_data: Dictionary containing file processing details
        """
        try:
            # Ensure producer is ready
            await self.producer.start()

            # Standardize event format
            # TODO: do not do any logic here
            formatted_event = {
                "eventType": event_data.get("eventType", EventTypes.NEW_RECORD.value),
                "timestamp": get_epoch_timestamp_in_ms(),
                "payload": {
                    "orgId": event_data.get("orgId"),
                    "recordId": event_data.get("recordId"),
                    "virtualRecordId": event_data.get("virtualRecordId", None),
                    "recordName": event_data.get("recordName"),
                    "recordType": event_data.get("recordType"),
                    "version": event_data.get("recordVersion", 0),
                    "signedUrlRoute": event_data.get("signedUrlRoute"),
                    "connectorName": event_data.get("connectorName"),
                    "origin": event_data.get("origin"),
                    "extension": event_data.get("extension"),
                    "mimeType": event_data.get("mimeType"),
                    "body": event_data.get("body"),
                    "createdAtTimestamp": event_data.get("createdAtSourceTimestamp"),
                    "updatedAtTimestamp": event_data.get("modifiedAtSourceTimestamp"),
                    "sourceCreatedAtTimestamp": event_data.get(
                        "createdAtSourceTimestamp"
                    ),
                },
            }

            # Convert to JSON bytes for aiokafka
            message_value = json.dumps(formatted_event).encode('utf-8')
            message_key = str(formatted_event["payload"]["recordId"]).encode('utf-8')

            # Send message and wait for delivery
            # TODO: pass topic as a parameter
            record_metadata = await self.producer.send_message(
                topic="record-events",
                message=message_value,
                key=message_key
            )

            # Log successful delivery
            self.logger.info(
                "✅ Record %s successfully produced to %s [%s] at offset %s",
                formatted_event["payload"]["recordId"],
                record_metadata.topic,
                record_metadata.partition,
                record_metadata.offset
            )

            return True

        except Exception as e:
            self.logger.error("❌ Failed to send event to Kafka: %s", str(e))
            return False

    # implementing abstract methods from IMessagingService
    async def __aenter__(self) -> "KafkaService":
        """Async context manager entry"""
        await self.producer.start()
        return self

    # implementing abstract methods from IMessagingService
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit"""
        await self.producer.stop()
