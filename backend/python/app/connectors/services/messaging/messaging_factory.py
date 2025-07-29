from typing import Optional
from logging import Logger

from app.connectors.services.messaging.interface.producer import IMessagingProducer
from app.connectors.services.messaging.interface.consumer import IMessagingConsumer
from app.connectors.services.messaging.kafka.producer.producer import KafkaMessagingProducer
from app.connectors.services.messaging.kafka.consumer.consumer import KafkaMessagingConsumer
from app.connectors.sources.google.common.arango_service import ArangoService
from app.connectors.sources.google.common.sync_tasks import SyncTasks
from app.config.configuration_service import ConfigurationService
from app.setups.connector_setup import AppContainer


class MessagingFactory:
    """Factory for creating messaging service instances"""
    
    @staticmethod
    def create_producer(
        broker_type: str = "kafka",
        config_service: ConfigurationService = None,
        sync_tasks: SyncTasks = None,
        arango_service: ArangoService = None,
        logger: Logger = None
    ) -> IMessagingProducer:
        """Create a messaging producer"""
        if broker_type.lower() == "kafka":
            return KafkaMessagingProducer(config_service, sync_tasks, arango_service, logger)
        else:
            raise ValueError(f"Unsupported broker type: {broker_type}")
    
    @staticmethod
    def create_consumer(
        broker_type: str = "kafka",
        config_service: ConfigurationService = None,
        sync_tasks: SyncTasks = None,
        arango_service: ArangoService = None,
        app_container: AppContainer = None,
        logger: Logger = None
    ) -> IMessagingConsumer:
        """Create a messaging consumer"""
        if broker_type.lower() == "kafka":
            return KafkaMessagingConsumer(config_service, sync_tasks, arango_service, app_container, logger)
        else:
            raise ValueError(f"Unsupported broker type: {broker_type}")
