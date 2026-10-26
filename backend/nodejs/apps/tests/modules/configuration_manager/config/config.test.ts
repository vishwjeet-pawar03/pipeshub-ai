import 'reflect-metadata';
import { expect } from 'chai';
import crypto from 'crypto';
import {
  ALLOW_PLACEHOLDER_ENV,
  PLACEHOLDER_SECRET_KEYS,
  getHashedSecretKey,
  loadConfigurationManagerConfig,
} from '../../../../src/modules/configuration_manager/config/config';
import { StoreType } from '../../../../src/libs/keyValueStore/constants/KeyValueStoreType';

describe('loadConfigurationManagerConfig store type', () => {
  const saved = {
    KV: process.env.KV_STORE_TYPE,
    SECRET: process.env.SECRET_KEY,
  };

  beforeEach(() => {
    process.env.SECRET_KEY = 'test-secret';
  });

  afterEach(() => {
    for (const [name, value] of [
      ['KV_STORE_TYPE', saved.KV],
      ['SECRET_KEY', saved.SECRET],
    ] as const) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  });

  const storeFor = (kv: string | undefined): StoreType => {
    if (kv === undefined) delete process.env.KV_STORE_TYPE;
    else process.env.KV_STORE_TYPE = kv;
    return loadConfigurationManagerConfig().storeType;
  };

  it('picks the PostgreSQL store when KV_STORE_TYPE is postgres', () => {
    expect(storeFor('postgres')).to.equal(StoreType.Postgres);
    expect(storeFor('POSTGRES')).to.equal(StoreType.Postgres);
  });

  it('keeps Redis as the default and etcd for anything else', () => {
    expect(storeFor(undefined)).to.equal(StoreType.Redis);
    expect(storeFor('redis')).to.equal(StoreType.Redis);
    expect(storeFor('etcd')).to.equal(StoreType.Etcd3);
  });
});

const sha256 = (v: string) => crypto.createHash('sha256').update(v).digest('hex')

describe('getHashedSecretKey', () => {
  let savedKey: string | undefined
  let savedAllow: string | undefined

  beforeEach(() => {
    savedKey = process.env.SECRET_KEY
    savedAllow = process.env[ALLOW_PLACEHOLDER_ENV]
    delete process.env[ALLOW_PLACEHOLDER_ENV]
  })

  afterEach(() => {
    if (savedKey === undefined) delete process.env.SECRET_KEY
    else process.env.SECRET_KEY = savedKey
    if (savedAllow === undefined) delete process.env[ALLOW_PLACEHOLDER_ENV]
    else process.env[ALLOW_PLACEHOLDER_ENV] = savedAllow
  })

  it('returns the sha256 hex of a real key', () => {
    process.env.SECRET_KEY = 'a3f1c2d4e5b6'
    expect(getHashedSecretKey()).to.equal(sha256('a3f1c2d4e5b6'))
  })

  for (const value of [undefined, '', '   ']) {
    it(`refuses a missing or empty key (${JSON.stringify(value)})`, () => {
      if (value === undefined) delete process.env.SECRET_KEY
      else process.env.SECRET_KEY = value
      expect(() => getHashedSecretKey()).to.throw(/SECRET_KEY environment variable is required.*openssl rand -hex 32/)
    })
  }

  for (const value of PLACEHOLDER_SECRET_KEYS) {
    it(`refuses the published placeholder ${value}`, () => {
      process.env.SECRET_KEY = value
      expect(() => getHashedSecretKey()).to.throw(new RegExp(`example value.*${ALLOW_PLACEHOLDER_ENV} to true`))
    })
  }

  it('refuses a placeholder with surrounding whitespace', () => {
    process.env.SECRET_KEY = ' your_random_encryption_secret_key\n'
    expect(() => getHashedSecretKey()).to.throw(/example value/)
  })

  for (const flag of ['', 'false', '1', 'yes']) {
    it(`does not opt in for ${ALLOW_PLACEHOLDER_ENV}=${JSON.stringify(flag)}`, () => {
      process.env.SECRET_KEY = 'your_random_encryption_secret_key'
      process.env[ALLOW_PLACEHOLDER_ENV] = flag
      expect(() => getHashedSecretKey()).to.throw(/example value/)
    })
  }

  it('keeps the existing key when the operator opts in, so stored data stays readable', () => {
    process.env.SECRET_KEY = 'your_random_encryption_secret_key'
    process.env[ALLOW_PLACEHOLDER_ENV] = 'TRUE'
    expect(getHashedSecretKey()).to.equal(sha256('your_random_encryption_secret_key'))
  })

  it('matches the Python placeholder list', () => {
    const fs = require('fs') as typeof import('fs')
    const path = require('path') as typeof import('path')
    const py = fs.readFileSync(
      path.resolve(__dirname, '../../../../../../python/app/config/secret_key.py'),
      'utf8',
    )
    const block = py.slice(py.indexOf('PLACEHOLDER_SECRET_KEYS = frozenset({'), py.indexOf('})'))
    const pyValues = [...block.matchAll(/"([^"]+)"/g)].map((m) => m[1]).sort()
    expect(pyValues).to.deep.equal([...PLACEHOLDER_SECRET_KEYS].sort())
  })
})
