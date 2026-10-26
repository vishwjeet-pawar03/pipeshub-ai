import { StoreType } from '../../../libs/keyValueStore/constants/KeyValueStoreType';
import crypto from 'crypto';
import { Logger } from '../../../libs/services/logger.service';
import { RedisStoreConfig } from '../../../libs/keyValueStore/providers/RedisDistributedKeyValueStore';

const logger = Logger.getInstance({ service: 'ConfigurationManagerConfig' });

export interface ConfigurationManagerStoreConfig {
  host: string;
  port: number;
  dialTimeout: number;
}

export interface ConfigurationManagerConfig {
  storeType: string;
  storeConfig: ConfigurationManagerStoreConfig;
  redisConfig: RedisStoreConfig;
  secretKey: string;
  algorithm: string;
}

// Example values published in this repository's templates and docs. Keep in
// sync with PLACEHOLDER_SECRET_KEYS in backend/python/app/config/secret_key.py.
export const PLACEHOLDER_SECRET_KEYS: ReadonlySet<string> = new Set([
  'your_random_encryption_secret_key',
  'my-secret',
  'your-32-char-secret-key-here',
]);
export const ALLOW_PLACEHOLDER_ENV = 'PIPESHUB_ALLOW_INSECURE_SECRET_KEY';
const SECRET_KEY_FIX =
  'Generate one with `openssl rand -hex 32`, set SECRET_KEY to it in your .env (or secret store) and restart.';
let placeholderWarned = false;

export const getHashedSecretKey = (): string => {
  const secretKey = process.env.SECRET_KEY;
  if (!secretKey || !secretKey.trim()) {
    logger.warn('SECRET_KEY environment variable is not set. It is required');
    throw new Error(
      `SECRET_KEY environment variable is required. ${SECRET_KEY_FIX}`,
    );
  }
  if (PLACEHOLDER_SECRET_KEYS.has(secretKey.trim())) {
    if (
      (process.env[ALLOW_PLACEHOLDER_ENV] || '').trim().toLowerCase() !== 'true'
    ) {
      throw new Error(
        'SECRET_KEY is set to an example value published in the PipesHub templates, ' +
          'so anyone can decrypt the credentials this deployment stores. ' +
          `New deployment: ${SECRET_KEY_FIX} ` +
          'Deployment that already stored data with this value: changing SECRET_KEY makes ' +
          `that data unreadable, so set ${ALLOW_PLACEHOLDER_ENV} to true to keep running on it ` +
          "and see 'SECRET_KEY' in deployment/docker-compose/ADVANCED_DEPLOYMENT.md.",
      );
    }
    if (!placeholderWarned) {
      placeholderWarned = true;
      logger.warn(
        `SECRET_KEY is a published example value; running anyway because ${ALLOW_PLACEHOLDER_ENV} is true. ` +
          'Stored credentials can be decrypted by anyone who can read the KV store or its backups.',
      );
    }
  }
  const hashedKey = crypto.createHash('sha256').update(secretKey).digest();
  return hashedKey.toString('hex');
};

export const loadConfigurationManagerConfig =
  (): ConfigurationManagerConfig => {
    // Determine store type from KV_STORE_TYPE env variable (defaults to redis)
    const kvStoreType = process.env.KV_STORE_TYPE?.toLowerCase() || 'redis';
    const storeType =
      kvStoreType === 'redis'
        ? StoreType.Redis
        : kvStoreType === 'postgres'
          ? StoreType.Postgres
          : StoreType.Etcd3;

    return {
      storeType: storeType,
      storeConfig: {
        host: process.env.ETCD_HOST || 'http://localhost',
        port: parseInt(process.env.ETCD_PORT || '2379', 10),
        dialTimeout: parseInt(process.env.ETCD_DIAL_TIMEOUT || '2000', 10),
      },
      redisConfig: {
        host: process.env.REDIS_HOST || 'localhost',
        port: parseInt(process.env.REDIS_PORT || '6379', 10),
        username: process.env.REDIS_USERNAME || undefined,
        password: process.env.REDIS_PASSWORD || undefined,
        tls: process.env.REDIS_TLS === 'true',
        db: parseInt(process.env.REDIS_DB || '0', 10),
        keyPrefix: process.env.REDIS_KV_PREFIX || 'pipeshub:kv:',
        connectTimeout: parseInt(process.env.REDIS_TIMEOUT || '10000', 10),
      },
      secretKey: getHashedSecretKey(),
      algorithm: process.env.ALGORITHM || 'aes-256-gcm',
    };
  };
