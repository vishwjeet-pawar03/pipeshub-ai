import 'reflect-metadata';
import { expect } from 'chai';
import sinon from 'sinon';
import crypto from 'crypto';
import {
  createSlackBotConfig,
  deleteSlackBotConfig,
  getSlackBotConfigs,
  updateSlackBotConfig,
  SLACK_BOT_SETTINGS_UNREADABLE,
  addWebSearchProvider,
  deleteWebSearchProvider,
  getWebSearchProviders,
  updateDefaultWebSearchProvider,
  updateWebSearchProvider,
  updateWebSearchSettings,
} from '../../../../src/modules/configuration_manager/controller/cm_controller';
import { loadConfigurationManagerConfig } from '../../../../src/modules/configuration_manager/config/config';
import { configPaths } from '../../../../src/modules/configuration_manager/paths/paths';
import { EncryptionService } from '../../../../src/libs/encryptor/encryptor';
import { CONFIG_SECRET_PLACEHOLDER } from '../../../../src/modules/configuration_manager/utils/maskConfigSecrets';
import { InternalServerError } from '../../../../src/libs/errors/http.errors';

// Drives the real configuration-manager handlers against an in-memory key-value
// store, with real encryption. Only the store and the AI service's HTTP health
// check are faked.

class MemoryKvStore {
  values = new Map<string, string>();
  writes: string[] = [];
  casConflictsLeft = 0;

  async get<T>(key: string): Promise<T | null> {
    return (this.values.get(key) ?? null) as T | null;
  }

  async set<T>(key: string, value: T): Promise<void> {
    this.writes.push(key);
    this.values.set(key, value as unknown as string);
  }

  async compareAndSet<T>(key: string, expected: T | null, next: T): Promise<boolean> {
    if (this.casConflictsLeft > 0) {
      this.casConflictsLeft--;
      return false;
    }
    if ((this.values.get(key) ?? null) !== expected) {
      return false;
    }
    this.writes.push(key);
    this.values.set(key, next as unknown as string);
    return true;
  }
}

function makeRes() {
  const res: any = { statusCode: 200, body: undefined };
  res.status = sinon.stub().callsFake((code: number) => {
    res.statusCode = code;
    return res;
  });
  res.json = sinon.stub().callsFake((body: unknown) => {
    res.body = body;
    return res;
  });
  res.end = sinon.stub().returns(res);
  return res;
}

async function run(handler: any, req: Record<string, unknown> = {}) {
  const res = makeRes();
  const next = sinon.stub();
  await handler({ body: {}, params: {}, headers: {}, ...req }, res, next);
  return { res, error: next.firstCall?.args[0] };
}

describe('Configuration manager stored settings', () => {
  let kv: MemoryKvStore;
  let savedSecretKey: string | undefined;
  let savedHideSecrets: string | undefined;

  const encryptor = () => {
    const cfg = loadConfigurationManagerConfig();
    return EncryptionService.getInstance(cfg.algorithm, cfg.secretKey);
  };
  const seal = (value: unknown) => encryptor().encrypt(JSON.stringify(value));
  const open = (key: string) => JSON.parse(encryptor().decrypt(kv.values.get(key)!));

  // Same iv:ciphertext:tag format, sealed with a key the server does not hold,
  // which is what the store looks like after SECRET_KEY changes.
  function sealWithAnotherKey(value: unknown): string {
    const key = crypto.randomBytes(32);
    const iv = crypto.randomBytes(12);
    const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
    const data = Buffer.concat([cipher.update(JSON.stringify(value), 'utf8'), cipher.final()]);
    return `${iv.toString('hex')}:${data.toString('hex')}:${cipher.getAuthTag().toString('hex')}`;
  }

  before(() => {
    savedSecretKey = process.env.SECRET_KEY;
    process.env.SECRET_KEY = process.env.SECRET_KEY || 'stored-settings-test-secret';
  });

  after(() => {
    if (savedSecretKey === undefined) delete process.env.SECRET_KEY;
    else process.env.SECRET_KEY = savedSecretKey;
  });

  beforeEach(() => {
    kv = new MemoryKvStore();
    savedHideSecrets = process.env.HIDE_SECRET_CONFIG;
  });

  afterEach(() => {
    sinon.restore();
    if (savedHideSecrets === undefined) delete process.env.HIDE_SECRET_CONFIG;
    else process.env.HIDE_SECRET_CONFIG = savedHideSecrets;
  });

  describe('Slack bot settings', () => {
    const existingBot = {
      id: 'bot-1',
      name: 'Support bot',
      botToken: 'xoxb-existing',
      signingSecret: 'signing-existing',
      agentId: 'agent-1',
      createdAt: '2026-01-01T00:00:00.000Z',
      updatedAt: '2026-01-01T00:00:00.000Z',
    };

    describe('when the stored settings cannot be read', () => {
      beforeEach(() => {
        kv.values.set(configPaths.slackBot, sealWithAnotherKey({ configs: [existingBot] }));
      });

      it('refuses to add a bot instead of replacing every stored bot with the new one', async () => {
        const before = kv.values.get(configPaths.slackBot);

        const { res, error } = await run(createSlackBotConfig(kv as any), {
          body: { name: 'New bot', botToken: 'xoxb-new', signingSecret: 'sig-new' },
        });

        expect(error).to.be.instanceOf(InternalServerError);
        expect(error.message).to.equal(SLACK_BOT_SETTINGS_UNREADABLE);
        expect(res.body).to.be.undefined;
        expect(kv.values.get(configPaths.slackBot)).to.equal(before);
        expect(kv.writes).to.deep.equal([]);
      });

      it('answers a read with an error, not an empty list of bots', async () => {
        const { res, error } = await run(getSlackBotConfigs(kv as any));

        expect(error).to.be.instanceOf(InternalServerError);
        expect(res.body).to.be.undefined;
      });

      it('refuses updates and deletes too, and leaves the stored value alone', async () => {
        const update = await run(updateSlackBotConfig(kv as any), {
          params: { configId: 'bot-1' },
          body: { name: 'Renamed', botToken: 'x', signingSecret: 'y' },
        });
        const remove = await run(deleteSlackBotConfig(kv as any), { params: { configId: 'bot-1' } });

        expect(update.error).to.be.instanceOf(InternalServerError);
        expect(remove.error).to.be.instanceOf(InternalServerError);
        expect(kv.writes).to.deep.equal([]);
      });
    });

    it('treats a store that was never written as having no bots', async () => {
      const { res, error } = await run(getSlackBotConfigs(kv as any));

      expect(error).to.be.undefined;
      expect(res.body.configs).to.deep.equal([]);
    });

    it('keeps the existing bots when a new one is added', async () => {
      kv.values.set(configPaths.slackBot, seal({ configs: [existingBot] }));

      const { res, error } = await run(createSlackBotConfig(kv as any), {
        body: { name: 'New bot', botToken: 'xoxb-new', signingSecret: 'sig-new', agentId: ' agent-2 ' },
      });

      expect(error).to.be.undefined;
      expect(res.body.config).to.include({ name: 'New bot', agentId: 'agent-2' });
      const stored = open(configPaths.slackBot).configs;
      expect(stored.map((c: any) => c.id)).to.deep.equal(['bot-1', res.body.config.id]);
    });

    it('refuses to link one agent to two bots', async () => {
      kv.values.set(configPaths.slackBot, seal({ configs: [existingBot] }));

      const { error } = await run(createSlackBotConfig(kv as any), {
        body: { name: 'Clone', botToken: 'x', signingSecret: 'y', agentId: 'agent-1' },
      });

      expect(error.message).to.match(/already linked/);
      expect(kv.writes).to.deep.equal([]);
    });

    it('retries after a concurrent write and gives up with a plain message after five tries', async () => {
      kv.values.set(configPaths.slackBot, seal({ configs: [] }));
      kv.casConflictsLeft = 5;
      const clock = sinon.useFakeTimers({ toFake: ['setTimeout'] });

      const pending = run(createSlackBotConfig(kv as any), {
        body: { name: 'New bot', botToken: 'x', signingSecret: 'y' },
      });
      await clock.runAllAsync();
      const { error } = await pending;

      expect(error.message).to.match(/concurrent modification. Please try again/);
      expect(kv.writes).to.deep.equal([]);
    });

    it('updates one bot and leaves the others untouched', async () => {
      const other = { ...existingBot, id: 'bot-2', agentId: 'agent-2', name: 'Sales bot' };
      kv.values.set(configPaths.slackBot, seal({ configs: [existingBot, other] }));

      const { error } = await run(updateSlackBotConfig(kv as any), {
        params: { configId: 'bot-1' },
        body: { name: 'Support bot v2', botToken: 'xoxb-rotated', signingSecret: 'sig-rotated' },
      });

      expect(error).to.be.undefined;
      const stored = open(configPaths.slackBot).configs;
      expect(stored[0]).to.include({ id: 'bot-1', name: 'Support bot v2', botToken: 'xoxb-rotated' });
      expect(stored[1]).to.deep.equal(other);
    });
  });
});
