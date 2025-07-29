import asyncio
import json
from typing import Any, Dict, List, Optional, Callable, Awaitable

from aiokafka import AIOKafkaConsumer  # type: ignore
from logging import Logger
from app.connectors.services.messaging.kafka.config.kafka_config import KafkaConfig
from app.connectors.services.messaging.interface.consumer import IMessagingConsumer
from app.connectors.sources.google.common.arango_service import ArangoService
from app.connectors.sources.google.common.sync_tasks import SyncTasks
from app.connectors.core.base.event_service.event_service import BaseEventService
from app.connectors.services.messaging.kafka.topics import topics
from app.connectors.services.messaging.kafka.topics import sync_events_topic, entity_events_topic
from app.config.utils.named_constants.arangodb_constants import Connectors
from app.setups.connector_setup import AppContainer
from dependency_injector.wiring import inject # type: ignore
from app.connectors.services.messaging.kafka.entity.entity import EntityEventService

class KafkaMessagingConsumer(IMessagingConsumer):
    """Kafka implementation of messaging consumer"""
    
    def __init__(self,
                arango_service: ArangoService, 
                sync_tasks: SyncTasks,
                app_container: AppContainer,
                logger: Logger,
                kafka_config: KafkaConfig) -> None:
        self.logger = logger
        self.consumer = None
        self.running = False
        self.arango_service = arango_service
        self.app_container = app_container
        self.sync_tasks = sync_tasks
        self.event_services: Dict[str, BaseEventService] = {}
        self.processed_messages: Dict[str, List[int]] = {}
        self.consume_task = None
        self.kafka_config = kafka_config

    # implementing abstract methods from IMessagingConsumer
    async def initialize(self) -> None:
        """Initialize the Kafka consumer"""
        try:
            if not self.kafka_config:
                raise ValueError("Kafka configuration is not valid")

            # Initialize consumer with aiokafka
            self.consumer = AIOKafkaConsumer(
                *topics,
                **self.kafka_config
            )

            # Initialize event services for different events
            await self.__initialize_event_services()
            await self.consumer.start()
            self.logger.info(f"Successfully initialized aiokafka consumer for topics: {topics}")
        except Exception as e:
            self.logger.error(f"Failed to create consumer: {e}")
            raise
        pass
    
    # implementing abstract methods from IMessagingConsumer
    async def cleanup(self) -> None:
        """Stop the Kafka consumer and clean up resources"""
        try:
            if self.consumer:
                await self.consumer.stop()
                self.logger.info("Kafka consumer stopped")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

    # implementing abstract methods from IMessagingConsumer
    async def start(
        self
    ) -> None:
        """Start consuming messages"""
        try:
            self.running = True
            # initialize consumer
            if not self.consumer:
                await self.initialize()
            # create a task for consuming messages
            self.consume_task = asyncio.create_task(self.__consume_loop(self.__process_message))
            self.logger.info("Started Kafka consumer task")
        except Exception as e:
            self.logger.error(f"Failed to start Kafka consumer: {str(e)}")
            raise

    # implementing abstract methods from IMessagingConsumer  
    async def stop(self) -> None:
        """Stop consuming messages"""
        self.running = False
        if self.consume_task:
            self.consume_task.cancel()
            try:
                await self.consume_task
            except asyncio.CancelledError:
                pass

        if self.consumer:
            await self.consumer.stop()
            self.logger.info("✅ Kafka consumer stopped")

    # implementing abstract methods from IMessagingConsumer
    def is_running(self) -> bool:
        """Check if consumer is running"""
        return self.running

    @inject
    async def __process_message(self, message) -> bool:
        """Process incoming Kafka messages and route them to appropriate handlers"""
        message_id = None
        try:
            message_id = f"{message.topic}-{message.partition}-{message.offset}"
            self.logger.debug(f"Processing message {message_id}")

            if self.__is_message_processed(message_id):
                self.logger.info(f"Message {message_id} already processed, skipping")
                return True

            topic = message.topic
            message_value = message.value
            value = None
            event_type = None

            # Message decoding and parsing
            try:
                if isinstance(message_value, bytes):
                    message_value = message_value.decode("utf-8")
                    self.logger.debug(f"Decoded bytes message for {message_id}")

                if isinstance(message_value, str):
                    try:
                        value = json.loads(message_value)
                        # Handle double-encoded JSON
                        if isinstance(value, str):
                            value = json.loads(value)
                            self.logger.debug("Handled double-encoded JSON message")

                        event_type = value.get("eventType")
                        self.logger.debug(
                            f"Parsed message {message_id}: type={type(value)}, event_type={event_type}"
                        )
                    except json.JSONDecodeError as e:
                        self.logger.error(
                            f"JSON parsing failed for message {message_id}: {str(e)}\n"
                            f"Raw message: {message_value[:1000]}..."  # Log first 1000 chars
                        )
                        return False
                else:
                    self.logger.error(
                        f"Unexpected message value type for {message_id}: {type(message_value)}"
                    )
                    return False

            except UnicodeDecodeError as e:
                self.logger.error(
                    f"Failed to decode message {message_id}: {str(e)}\n"
                    f"Raw bytes: {message_value[:100]}..."  # Log first 100 bytes
                )
                return False

            # Validation
            if not event_type:
                self.logger.error(f"Missing event_type in message {message_id}")
                return False

            # Route and handle message
            try:
                if topic == sync_events_topic:
                    self.logger.info(f"Processing sync event: {event_type}")
                    return await self.__handle_sync_event(event_type, value)
                elif topic == entity_events_topic:
                    self.logger.info(f"Processing entity event: {event_type}")
                    return await self.__handle_entity_event(event_type, value)
                else:
                    self.logger.warning(
                        f"Unhandled topic {topic} for message {message_id}"
                    )
                    return False

            except asyncio.TimeoutError:
                self.logger.error(
                    f"Timeout while processing {event_type} event in message {message_id}"
                )
                return False
            except ValueError as e:
                self.logger.error(
                    f"Validation error processing {event_type} event: {str(e)}"
                )
                return False
            except Exception as e:
                self.logger.error(
                    f"Error processing {event_type} event in message {message_id}: {str(e)}",
                    exc_info=True,
                )
                return False

        except Exception as e:
            self.logger.error(
                f"Unexpected error processing message {message_id if message_id else 'unknown'}: {str(e)}",
                exc_info=True,
            )
            return False
        finally:
            if message_id:
                self.__mark_message_processed(message_id)

    async def __consume_loop(
        self, 
        message_handler: Callable[[Dict[str, Any]], Awaitable[bool]]
    ) -> None:
        """Main consumption loop"""
        try:
            self.logger.info("Starting Kafka consumer loop")
            while self.running:
                try:
                    # Get messages asynchronously with timeout
                    message_batch = await self.consumer.getmany(timeout_ms=1000, max_records=1)

                    if not message_batch:
                        await asyncio.sleep(0.1)
                        continue

                    # Process messages from all topic partitions
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            try:
                                self.logger.info(f"Received message: topic={message.topic}, partition={message.partition}, offset={message.offset}")
                                success = await message_handler(message)

                                if success:
                                    # Commit the offset for this message
                                    await self.consumer.commit({topic_partition: message.offset + 1})
                                    self.logger.info(
                                        f"Committed offset for topic-partition {message.topic}-{message.partition} at offset {message.offset}"
                                    )
                                else:
                                    self.logger.warning(f"Failed to process message at offset {message.offset}")

                            except Exception as e:
                                self.logger.error(f"Error processing individual message: {e}")
                                continue

                except asyncio.CancelledError:
                    self.logger.info("Kafka consumer task cancelled")
                    break
                except Exception as e:
                    self.logger.error(f"Error in consume_messages loop: {e}")
                    await asyncio.sleep(1)

        except Exception as e:
            self.logger.error(f"Fatal error in consume_messages: {e}")
        finally:
            await self.cleanup()

    async def __initialize_event_services(self) -> None:
        """Initialize different event services for different connectors, entity events and other events"""
        try:
            self.logger.info("Initializing connector-specific event services...")
            
            # Import event services here to avoid circular imports
            from app.connectors.sources.google.google_drive.services.event_service.event_service import GoogleDriveEventService
            from app.connectors.sources.google.gmail.services.event_service.event_service import GmailEventService

            # Initialize Drive event service
            self.event_services[Connectors.GOOGLE_DRIVE.value] = GoogleDriveEventService(
                self.logger, self.sync_tasks, self.arango_service
            )
            self.logger.debug(f"Initialized Drive event service with key: {Connectors.GOOGLE_DRIVE.value}")

            # Initialize Gmail event service
            self.event_services[Connectors.GOOGLE_MAIL.value] = GmailEventService(
                self.logger, self.sync_tasks, self.arango_service
            )
            self.logger.debug(f"Initialized Gmail event service with key: {Connectors.GOOGLE_MAIL.value}")

            self.logger.info(f"Successfully initialized {len(self.event_services)} event services: {list(self.event_services.keys())}")

            # Initialize entity event service
            self.event_services[EntityEventService.__name__] = EntityEventService(
                self.logger, self.sync_tasks, self.arango_service, self.app_container
            )
            self.logger.debug(f"Initialized Entity event service")

        except Exception as e:
            self.logger.error(f"Failed to initialize event services: {e}")
            raise

    def __is_message_processed(self, message_id: str) -> bool:
        """Check if a message has already been processed."""
        topic_partition = "-".join(message_id.split("-")[:-1])
        offset = int(message_id.split("-")[-1])
        return (
            topic_partition in self.processed_messages
            and offset in self.processed_messages[topic_partition]
        )
    
    def __mark_message_processed(self, message_id: str) -> None:
        """Mark a message as processed."""
        topic_partition = "-".join(message_id.split("-")[:-1])
        offset = int(message_id.split("-")[-1])
        if topic_partition not in self.processed_messages:
            self.processed_messages[topic_partition] = []
        self.processed_messages[topic_partition].append(offset)

    # handler for entity events topic
    async def __handle_entity_event(self, event_type: str, value: dict) -> bool:
        """Handle entity-related events by calling appropriate handlers"""
        if EntityEventService.__name__ in self.event_services:
            return await self.event_services[EntityEventService.__name__].process_event(event_type, value)
        else:
            self.logger.error(f"Event service not initialized for connector: {EntityEventService.__name__}")
            return False

    # handler for sync events topic
    async def __handle_sync_event(self, event_type: str, value: dict) -> bool:
        """Handle sync-related events by calling appropriate handlers"""
        try:
            # First try to get connector from payload
            connectorName = value.get("payload", {}).get("connector")
            if connectorName is not None:
                if connectorName in self.event_services:
                    event_service = self.event_services[connectorName]
                    if event_service is not None:
                        return await event_service.process_event(event_type, value["payload"])
                    else:
                        self.logger.error(f"Event service is None for connector: {connectorName}")
                        return False
                else:
                    self.logger.error(f"Event service not initialized for connector: {connectorName}")
                    return False
            else:
                self.logger.error(f"Connector not found in payload")
                return False
        except Exception as e:
            self.logger.error(f"Error handling sync event: {e}")
            return False