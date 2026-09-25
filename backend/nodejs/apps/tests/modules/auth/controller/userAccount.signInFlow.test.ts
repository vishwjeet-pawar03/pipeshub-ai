import 'reflect-metadata';
import { expect } from 'chai';
import sinon from 'sinon';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import mongoose from 'mongoose';
import { OAuth2Client } from 'google-auth-library';
import {
  UserAccountController,
  EMAIL_MISMATCH,
  OAUTH_SIGN_IN_FAILED,
  PROVIDER_SHARED_NO_EMAIL,
  SESSION_NO_LONGER_VALID,
} from '../../../../src/modules/auth/controller/userAccount.controller';
import { SessionService } from '../../../../src/modules/auth/services/session.service';
import { OrgAuthConfig } from '../../../../src/modules/auth/schema/orgAuthConfiguration.schema';
import { UserCredentials } from '../../../../src/modules/auth/schema/userCredentials.schema';
import { UserActivities } from '../../../../src/modules/auth/schema/userActivities.schema';
import { Org } from '../../../../src/modules/user_management/schema/org.schema';
import { Users } from '../../../../src/modules/user_management/schema/users.schema';
import * as azureAd from '../../../../src/modules/auth/utils/azureAdTokenValidation';
import {
  BadRequestError,
  NotFoundError,
  UnauthorizedError,
} from '../../../../src/libs/errors/http.errors';
import { deriveUserActionSecret } from '../../../../src/libs/utils/jwtKeys';
import { createMockRedisService } from '../../../helpers/mock-redis';
import { createMockQuery } from '../../../helpers/mock-mongo';

// Drives the real controller and the real SessionService through whole
// sign-ins, step after step, the way the /initAuth and /authenticate routes do.
// Only the outermost I/O is faked: Mongo models, the IAM HTTP service, the mail
// service, the Redis cache behind the session, and the identity providers.

const JWT_SECRET = 'sign-in-flow-jwt-secret';
const SCOPED_SECRET = 'sign-in-flow-scoped-secret';
const PASSWORD = 'Correct-Horse-9!';

const orgId = new mongoose.Types.ObjectId().toHexString();
const alice = {
  _id: new mongoose.Types.ObjectId().toHexString(),
  orgId,
  email: 'alice@acme.test',
  fullName: 'Alice Admin',
  role: 'member',
  hasLoggedIn: true,
};
const mallory = {
  _id: new mongoose.Types.ObjectId().toHexString(),
  orgId,
  email: 'mallory@acme.test',
  fullName: 'Mallory Member',
  role: 'member',
  hasLoggedIn: true,
};
const directory: Record<string, typeof alice> = {
  [alice.email]: alice,
  [mallory.email]: mallory,
};

function credentialsDoc(fields: Record<string, any>) {
  const doc: any = {
    isBlocked: false,
    wrongCredentialCount: 0,
    blockExpiresAt: null,
    ...fields,
  };
  doc.save = sinon.stub().resolves(doc);
  return doc;
}

describe('UserAccountController sign-in flow', () => {
  let controller: UserAccountController;
  let sessionService: SessionService;
  let redisStore: Map<string, string>;
  let iamService: any;
  let mailService: any;
  let configService: any;
  let jitService: any;
  let logger: any;
  let credentialsByUser: Record<string, any>;
  let activities: any[];
  let configuredSteps: string[][];

  function orgAuthConfig(steps: string[][]) {
    return {
      orgId,
      authSteps: steps.map((types, i) => ({
        order: i + 1,
        allowedMethods: types.map((type) => ({ type })),
      })),
    };
  }

  function makeRes() {
    const res: any = {
      headers: {} as Record<string, string>,
      statusCode: 200,
      body: undefined as any,
    };
    res.status = sinon.stub().callsFake((code: number) => {
      res.statusCode = code;
      return res;
    });
    res.json = sinon.stub().callsFake((body: any) => {
      res.body = body;
      return res;
    });
    res.send = res.json;
    res.setHeader = sinon.stub().callsFake((k: string, v: string) => {
      res.headers[k] = v;
      return res;
    });
    return res;
  }

  async function initAuth(steps: string[][], email = alice.email) {
    configuredSteps = steps;
    const res = makeRes();
    const next = sinon.stub();
    await controller.initAuth({ body: { email } } as any, res, next);
    expect(next.called, 'initAuth must not fail').to.be.false;
    return res.headers['x-session-token'] as string;
  }

  // Mirrors authSessionMiddleware: the session is read back from the cache on
  // every request, so whatever a step saved is what the next step sees.
  async function authenticate(token: string, body: Record<string, any>) {
    const sessionInfo = await sessionService.getSession(token);
    const res = makeRes();
    const next = sinon.stub();
    await controller.authenticate(
      { body, sessionInfo: sessionInfo ?? undefined, ip: '10.0.0.1' } as any,
      res,
      next,
    );
    return { res, next, error: next.firstCall?.args[0] };
  }

  beforeEach(() => {
    redisStore = new Map();
    const redis = createMockRedisService();
    redis.set.callsFake(async (key: string, value: unknown) => {
      redisStore.set(key, JSON.stringify(value));
    });
    redis.get.callsFake(async (key: string) => {
      const raw = redisStore.get(key);
      return raw === undefined ? null : JSON.parse(raw);
    });
    redis.delete.callsFake(async (key: string) => {
      redisStore.delete(key);
    });
    sessionService = new SessionService(redis as any);

    iamService = {
      getUserByEmail: sinon.stub().callsFake(async (email: string) => {
        const user = directory[email.toLowerCase()];
        return user
          ? { statusCode: 200, data: { ...user } }
          : { statusCode: 404, data: null };
      }),
      getUserById: sinon.stub(),
      updateUser: sinon.stub().resolves({ statusCode: 200, data: {} }),
      checkAdminUser: sinon.stub(),
      createOrg: sinon.stub(),
    };
    mailService = { sendMail: sinon.stub().resolves({ statusCode: 200 }) };
    configService = { getConfig: sinon.stub() };
    jitService = {
      provisionUser: sinon.stub(),
      extractGoogleUserDetails: sinon.stub().returns({ fullName: 'New Person' }),
      extractMicrosoftUserDetails: sinon.stub().returns({ fullName: 'New Person' }),
      extractOAuthUserDetails: sinon.stub().returns({ fullName: 'New Person' }),
    };
    logger = {
      info: sinon.stub(),
      debug: sinon.stub(),
      warn: sinon.stub(),
      error: sinon.stub(),
    };

    controller = new UserAccountController(
      {
        cmBackend: 'http://cm',
        frontendUrl: 'http://app',
        jwtSecret: JWT_SECRET,
        scopedJwtSecret: SCOPED_SECRET,
        rsAvailable: 'false',
      } as any,
      iamService,
      mailService,
      sessionService,
      configService,
      logger,
      jitService,
    );

    credentialsByUser = {};
    sinon.stub(UserCredentials, 'findOne').callsFake(((filter: any) =>
      Promise.resolve(credentialsByUser[String(filter.userId)] ?? null)) as any);
    sinon.stub(UserCredentials, 'findOneAndUpdate').callsFake(((
      filter: any,
      update: any,
    ) => {
      const doc = credentialsByUser[String(filter.userId)];
      if (!doc || ('hashedOTP' in filter && doc.hashedOTP !== filter.hashedOTP)) {
        return Promise.resolve(null);
      }
      if (update.$inc?.wrongCredentialCount) {
        doc.wrongCredentialCount += update.$inc.wrongCredentialCount;
      }
      Object.assign(doc, update.$set ?? {});
      for (const field of Object.keys(update.$unset ?? {})) {
        delete doc[field];
      }
      return Promise.resolve(doc);
    }) as any);
    configuredSteps = [['password']];
    sinon.stub(Org, 'findOne').resolves({ _id: orgId, shortName: 'Acme' } as any);
    sinon
      .stub(OrgAuthConfig, 'findOne')
      .callsFake((() => Promise.resolve(orgAuthConfig(configuredSteps))) as any);
    activities = [];
    sinon.stub(UserActivities, 'create').callsFake((async (doc: any) => {
      activities.push(doc);
      return doc;
    }) as any);
    sinon.stub(Users, 'findOne').callsFake(((filter: any) => {
      const found = Object.values(directory).find(
        (u) => u._id === String(filter._id),
      );
      return createMockQuery(found ? { kind: 'user', isDisabled: false } : null);
    }) as any);
  });

  afterEach(() => {
    sinon.restore();
  });

  async function givePassword(user: typeof alice, password = PASSWORD) {
    credentialsByUser[user._id] = credentialsDoc({
      userId: user._id,
      orgId,
      hashedPassword: await bcrypt.hash(password, 4),
    });
  }

  async function giveOtp(user: typeof alice, otp: string) {
    const existing = credentialsByUser[user._id];
    const fields = {
      userId: user._id,
      orgId,
      hashedOTP: await bcrypt.hash(otp, 4),
      otpValidity: Date.now() + 5 * 60 * 1000,
    };
    if (existing) {
      Object.assign(existing, fields);
    } else {
      credentialsByUser[user._id] = credentialsDoc(fields);
    }
  }

  function googleSignsInAs(email: string | undefined) {
    return sinon.stub(OAuth2Client.prototype, 'verifyIdToken').resolves({
      getPayload: () => (email ? { email, name: 'Someone' } : { name: 'Someone' }),
    } as any);
  }

  describe('the sign-in method has to be one the org allows at this step', () => {
    it('refuses password sign-in when the org only allows Google, even with the right password', async () => {
      await givePassword(alice);
      const token = await initAuth([['google']]);

      const { res, error } = await authenticate(token, {
        method: 'password',
        credentials: { password: PASSWORD },
      });

      expect(error).to.be.instanceOf(BadRequestError);
      expect(error.message).to.match(/isn't turned on/);
      expect(res.body).to.be.undefined;
      expect(await sessionService.getSession(token)).to.not.equal(null);
    });

    it('refuses to let the password from step one stand in for the code step two asks for', async () => {
      await givePassword(alice);
      const token = await initAuth([['password'], ['otp']]);

      const first = await authenticate(token, {
        method: 'password',
        credentials: { password: PASSWORD },
      });
      expect(first.error).to.be.undefined;
      expect(first.res.body).to.include({ status: 'success', nextStep: 1 });
      expect(first.res.body.allowedMethods).to.deep.equal(['otp']);

      const second = await authenticate(token, {
        method: 'password',
        credentials: { password: PASSWORD },
      });

      expect(second.error).to.be.instanceOf(BadRequestError);
      expect(second.error.message).to.match(/isn't turned on/);
      expect(second.res.body).to.be.undefined;
    });

    it('completes a password-then-code sign-in and ends the sign-in session', async () => {
      await givePassword(alice);
      const token = await initAuth([['password'], ['otp']]);
      await authenticate(token, {
        method: 'password',
        credentials: { password: PASSWORD },
      });
      await giveOtp(alice, '482913');

      const { res, error } = await authenticate(token, {
        method: 'otp',
        credentials: { otp: '482913' },
      });

      expect(error).to.be.undefined;
      expect(res.statusCode).to.equal(200);
      expect(res.body.message).to.equal('Fully authenticated');
      const access = jwt.verify(res.body.accessToken, JWT_SECRET) as any;
      expect(access.userId).to.equal(alice._id);
      const refresh = jwt.verify(
        res.body.refreshToken,
        deriveUserActionSecret(SCOPED_SECRET),
      ) as any;
      expect(refresh.userId).to.equal(alice._id);
      expect(await sessionService.getSession(token)).to.equal(null);
    });
  });

  describe('every step has to prove the same account', () => {
    it("refuses a Google step two for another member after step one checked Alice's password", async () => {
      await givePassword(alice);
      configService.getConfig.resolves({ data: { clientId: 'google-client' } });
      const token = await initAuth([['password'], ['google']]);
      await authenticate(token, {
        method: 'password',
        credentials: { password: PASSWORD },
      });
      googleSignsInAs(mallory.email);

      const { res, error } = await authenticate(token, {
        method: 'google',
        credentials: 'mallory-google-id-token',
      });

      expect(error).to.be.instanceOf(UnauthorizedError);
      expect(error.message).to.match(/same account/);
      expect(res.body).to.be.undefined;
    });

    it('does not create an account for an unknown Google identity at step two, even with JIT on', async () => {
      await givePassword(alice);
      configService.getConfig.resolves({
        data: { clientId: 'google-client', enableJit: true },
      });
      const token = await initAuth([['password'], ['google']]);
      await authenticate(token, {
        method: 'password',
        credentials: { password: PASSWORD },
      });
      googleSignsInAs('stranger@elsewhere.test');

      const { error } = await authenticate(token, {
        method: 'google',
        credentials: 'stranger-google-id-token',
      });

      expect(error).to.be.instanceOf(Error);
      expect(jitService.provisionUser.called).to.be.false;
    });

    it("signs Alice in when step two is Alice's own Google account", async () => {
      await givePassword(alice);
      configService.getConfig.resolves({ data: { clientId: 'google-client' } });
      const token = await initAuth([['password'], ['google']]);
      await authenticate(token, {
        method: 'password',
        credentials: { password: PASSWORD },
      });
      const verify = googleSignsInAs(alice.email);

      const { res, error } = await authenticate(token, {
        method: 'google',
        credentials: 'alice-google-id-token',
      });

      expect(error).to.be.undefined;
      expect(res.body.message).to.equal('Fully authenticated');
      expect(verify.firstCall.args[0]).to.deep.include({
        idToken: 'alice-google-id-token',
        audience: 'google-client',
      });
      expect(
        (jwt.verify(res.body.accessToken, JWT_SECRET) as any).userId,
      ).to.equal(alice._id);
    });
  });

  describe('a sign-in code works only once', () => {
    it('refuses the same code a second time after it signed the user in', async () => {
      const first = await initAuth([['otp']]);
      await giveOtp(alice, '731640');
      const ok = await authenticate(first, {
        method: 'otp',
        credentials: { otp: '731640' },
      });
      expect(ok.error).to.be.undefined;
      expect(ok.res.body.message).to.equal('Fully authenticated');

      const replayToken = await initAuth([['otp']]);

      const replay = await authenticate(replayToken, {
        method: 'otp',
        credentials: { otp: '731640' },
      });

      expect(replay.error).to.be.instanceOf(UnauthorizedError);
      expect(replay.res.body).to.be.undefined;
    });
  });

});
