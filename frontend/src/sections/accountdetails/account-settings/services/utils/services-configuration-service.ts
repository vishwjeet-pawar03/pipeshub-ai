import axios from 'src/utils/axios';

export interface RedisConfig {
  uri?: string;
  host: string;
  port: number;
  password?: string;
  username?: string;
  db?: string;
  tls: boolean;
}

export interface MongoDBConfig {
  uri: string;
  db: string;
}

export interface ArangoDBConfig {
  uri: string;
  db: string;
  username: string;
  password: string;
}

export interface KafkaConfig {
  clientId: string;
  brokers: string;
  groupId: string;
}

export interface QdrantConfig {
  host: string;
  grpcPort: string;
  apiKey?: string;
}

// Base API URL
const API_BASE_URL = '/api/v1/configurationManager';

/**
 * Fetch Redis configuration
 * @returns {Promise<RedisConfig>} The Redis configuration
 */
export const getRedisConfig = async (): Promise<RedisConfig> => {
  try {
    const response = await axios.get(`${API_BASE_URL}/redisConfig`);
    return response.data;
  } catch (error) {
    console.error('Failed to fetch Redis configuration:', error);
    throw error;
  }
};

/**
 * Fetch MongoDB configuration
 * @returns {Promise<MongoDBConfig>} The MongoDB configuration
 */
export const getMongoDBConfig = async (): Promise<MongoDBConfig> => {
  try {
    const response = await axios.get(`${API_BASE_URL}/mongoDBConfig`);
    return response.data;
  } catch (error) {
    console.error('Failed to fetch MongoDB configuration:', error);
    throw error;
  }
};

/**
 * Fetch ArangoDB configuration
 * @returns {Promise<ArangoDBConfig>} The ArangoDB configuration
 */
export const getArangoDBConfig = async (): Promise<ArangoDBConfig> => {
  try {
    const response = await axios.get(`${API_BASE_URL}/arangoDBConfig`);
    return response.data;
  } catch (error) {
    console.error('Failed to fetch ArangoDB configuration:', error);
    throw error;
  }
};

/**
 * Fetch Kafka configuration
 * @returns {Promise<KafkaConfig>} The Kafka configuration
 */
export const getKafkaConfig = async (): Promise<KafkaConfig> => {
  try {
    const response = await axios.get(`${API_BASE_URL}/kafkaConfig`);
    return response.data;
  } catch (error) {
    console.error('Failed to fetch Kafka configuration:', error);
    throw error;
  }
};

/**
 * Fetch Qdrant configuration
 * @returns {Promise<QdrantConfig>} The Qdrant configuration
 */
export const getQdrantConfig = async (): Promise<QdrantConfig> => {
  try {
    const response = await axios.get(`${API_BASE_URL}/qdrantConfig`);
    return response.data;
  } catch (error) {
    console.error('Failed to fetch Qdrant configuration:', error);
    throw error;
  }
};

export const getBackendNodejsConfig = async (): Promise<any> => {
  try {
    const response = await axios.get(`${API_BASE_URL}/qdrantConfig`);
    return response.data;
  } catch (error) {
    console.error('Failed to fetch Qdrant configuration:', error);
    throw error;
  }
};

/**
 * Update Redis configuration
 * @param {RedisConfig} redisConfig - Redis configuration
 * @returns {Promise<any>} The API response
 */
export const updateRedisConfig = async (redisConfig: RedisConfig): Promise<any> => {
  try {
    const response = await axios.post(`${API_BASE_URL}/redisConfig`, redisConfig);
    return response.data;
  } catch (error) {
    console.error('Failed to update Redis configuration:', error);
    throw error;
  }
};

/**
 * Update MongoDB configuration
 * @param {MongoDBConfig} mongoDBConfig - MongoDB configuration
 * @returns {Promise<any>} The API response
 */
export const updateMongoDBConfig = async (mongoDBConfig: MongoDBConfig): Promise<any> => {
  try {
    const response = await axios.post(`${API_BASE_URL}/mongoDBConfig`, mongoDBConfig);
    return response.data;
  } catch (error) {
    console.error('Failed to update MongoDB configuration:', error);
    throw error;
  }
};

/**
 * Update ArangoDB configuration
 * @param {ArangoDBConfig} arangoDBConfig - ArangoDB configuration
 * @returns {Promise<any>} The API response
 */
export const updateArangoDBConfig = async (arangoDBConfig: ArangoDBConfig): Promise<any> => {
  try {
    const response = await axios.post(`${API_BASE_URL}/arangoDBConfig`, arangoDBConfig);
    return response.data;
  } catch (error) {
    console.error('Failed to update ArangoDB configuration:', error);
    throw error;
  }
};

/**
 * Update Kafka configuration
 * @param {KafkaConfig} kafkaConfig - Kafka configuration
 * @returns {Promise<any>} The API response
 */
export const updateKafkaConfig = async (kafkaConfig: KafkaConfig): Promise<any> => {
  try {
    const response = await axios.post(`${API_BASE_URL}/kafkaConfig`, kafkaConfig);
    return response.data;
  } catch (error) {
    console.error('Failed to update Kafka configuration:', error);
    throw error;
  }
};

/**
 * Update Qdrant configuration
 * @param {QdrantConfig} qdrantConfig - Qdrant configuration
 * @returns {Promise<any>} The API response
 */
export const updateQdrantConfig = async (qdrantConfig: QdrantConfig): Promise<any> => {
  try {
    const response = await axios.post(`${API_BASE_URL}/qdrantConfig`, qdrantConfig);
    return response.data;
  } catch (error) {
    console.error('Failed to update Qdrant configuration:', error);
    throw error;
  }
};
export const updateBackendNodejsConfig = async (url: string): Promise<any> => {
  try {
    const response = await axios.post(`${API_BASE_URL}/kafkaConfig`, url);
    return response.data;
  } catch (error) {
    console.error('Failed to update Kafka configuration:', error);
    throw error;
  }
};

export default {
  getRedisConfig,
  getMongoDBConfig,
  getArangoDBConfig,
  getKafkaConfig,
  getQdrantConfig,
  getBackendNodejsConfig,
  updateRedisConfig,
  updateMongoDBConfig,
  updateArangoDBConfig,
  updateKafkaConfig,
  updateQdrantConfig,
  updateBackendNodejsConfig,
};
