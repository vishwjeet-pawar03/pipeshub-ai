from app.config.configuration_service import ConfigurationService, config_node_constants
from app.config.utils.named_constants.arangodb_constants import KafkaConfig

async def get_kafka_config(config_service: ConfigurationService, client_id: str) -> dict:     
        """Get Kafka configuration"""
        kafka_config = await config_service.get_config(
            config_node_constants.KAFKA.value
        )
        brokers = kafka_config["brokers"]
        return {
            "bootstrap_servers": ",".join(brokers),
            "group_id": "record_consumer_group",
            "auto_offset_reset": "earliest",
            "enable_auto_commit": True,
            "client_id": client_id,
        }