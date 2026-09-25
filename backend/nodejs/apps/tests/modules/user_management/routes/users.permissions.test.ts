import 'reflect-metadata';
import { expect } from 'chai';
import sinon from 'sinon';
import express from 'express';
import type { AddressInfo } from 'net';
import type { Server } from 'http';
import mongoose from 'mongoose';
import jwt from 'jsonwebtoken';
import { Container } from 'inversify';
import { createUserRouter } from '../../../../src/modules/user_management/routes/users.routes';
import { UserController } from '../../../../src/modules/user_management/controller/users.controller';
import { AuthMiddleware } from '../../../../src/libs/middlewares/auth.middleware';
import { AuthTokenService } from '../../../../src/libs/services/authtoken.service';
import { ErrorMiddleware } from '../../../../src/libs/middlewares/error.middleware';
import {
  authJwtGenerator,
  fetchConfigJwtGenerator,
  iamUserLookupJwtGenerator,
} from '../../../../src/libs/utils/createJwt';
import { Users } from '../../../../src/modules/user_management/schema/users.schema';
import { Org } from '../../../../src/modules/user_management/schema/org.schema';
import { UserCredentials } from '../../../../src/modules/auth/schema/userCredentials.schema';
import { UserActivities } from '../../../../src/modules/auth/schema/userActivities.schema';
import { UserDisplayPicture } from '../../../../src/modules/user_management/schema/userDp.schema';
import { userActivitiesType } from '../../../../src/libs/utils/userActivities.utils';
import * as appConfigModule from '../../../../src/modules/tokens_manager/config/config';

// Serves the real user router over HTTP with its real middleware chain
// (AuthMiddleware, userAdminCheck, userAdminOrSelfCheck, userExists) and the
// real UserController. Tokens are real JWTs. Only Mongo and the event bus are
// faked: the Users collection is an in-memory table the fake queries filter.

const JWT_SECRET = 'users-permissions-jwt-secret';
const SCOPED_SECRET = 'users-permissions-scoped-secret';

type Row = Record<string, any>;

function matches(row: Row, filter: Row): boolean {
  return Object.entries(filter).every(([key, cond]) => {
    const value = row[key];
    if (cond && typeof cond === 'object' && !(cond instanceof mongoose.Types.ObjectId)) {
      if ('$ne' in cond) return String(value) !== String(cond.$ne);
      if ('$in' in cond) return cond.$in.some((c: unknown) => String(c) === String(value));
      return false;
    }
    if (cond === false && value === undefined) return true;
    return String(value) === String(cond);
  });
}

class FakeTable {
  rows: Row[] = [];

  insert(row: Row) {
    this.rows.push({ ...row });
    return row;
  }

  get(id: string) {
    return this.rows.find((r) => String(r._id) === String(id));
  }

  doc(row: Row) {
    const copy: Row = { ...row };
    Object.defineProperty(copy, 'save', {
      enumerable: false,
      value: async () => {
        Object.assign(row, copy);
        return copy;
      },
    });
    Object.defineProperty(copy, 'toObject', {
      enumerable: false,
      value: () => ({ ...copy }),
    });
    return copy;
  }

  query<T>(compute: () => T) {
    const q: any = {
      select: () => q,
      lean: () => q,
      sort: () => q,
      exec: async () => compute(),
      then: (ok: (v: T) => unknown, fail: (e: unknown) => unknown) =>
        Promise.resolve().then(compute).then(ok, fail),
    };
    return q;
  }

  findOne(filter: Row) {
    return this.query(() => {
      const row = this.rows.find((r) => matches(r, filter));
      return row ? this.doc(row) : null;
    });
  }

  find(filter: Row) {
    return this.query(() => this.rows.filter((r) => matches(r, filter)).map((r) => ({ ...r })));
  }
}

const orgA = new mongoose.Types.ObjectId().toHexString();
const orgB = new mongoose.Types.ObjectId().toHexString();
const oid = () => new mongoose.Types.ObjectId().toHexString();

describe('User routes: who may do what', () => {
  let users: FakeTable;
  let credentials: FakeTable;
  let server: Server;
  let baseUrl: string;
  let events: any;
  let ids: Record<string, string>;

  function person(name: string, orgId: string, role: 'admin' | 'member', extra: Row = {}) {
    const id = oid();
    users.insert({
      _id: id,
      orgId,
      email: `${name}@${orgId === orgA ? 'a' : 'b'}.test`,
      fullName: name,
      role,
      isDeleted: false,
      ...extra,
    });
    return id;
  }

  function sessionFor(userId: string, opts: { role?: 'admin' | 'member' | null } = {}) {
    const row = users.get(userId)!;
    const role = opts.role === undefined ? row.role : opts.role;
    return authJwtGenerator(JWT_SECRET, row.email, userId, row.orgId, row.fullName, 'business', role);
  }

  async function call(
    method: string,
    path: string,
    token: string | null,
    body?: unknown,
  ): Promise<{ status: number; body: any }> {
    const response = await fetch(`${baseUrl}/users${path}`, {
      method,
      headers: {
        'content-type': 'application/json',
        ...(token ? { authorization: `Bearer ${token}` } : {}),
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const text = await response.text();
    let parsed: any = text;
    try {
      parsed = JSON.parse(text);
    } catch {
      // plain-text answers stay as text
    }
    return { status: response.status, body: parsed };
  }

  function errorMessage(res: { body: any }): string {
    return res.body?.error?.message ?? res.body?.message ?? '';
  }

  beforeEach(async () => {
    users = new FakeTable();
    credentials = new FakeTable();
    ids = {
      adminA: person('ada', orgA, 'admin'),
      memberA: person('max', orgA, 'member'),
      otherMemberA: person('mia', orgA, 'member'),
      secondAdminA: person('abe', orgA, 'admin'),
      adminB: person('bea', orgB, 'admin'),
      memberB: person('bob', orgB, 'member'),
      deletedA: person('dan', orgA, 'member', { isDeleted: true }),
      disabledA: person('dee', orgA, 'member', { isDisabled: true }),
    };

    sinon.stub(Users, 'findOne').callsFake(((f: Row) => users.findOne(f)) as any);
    sinon.stub(Users, 'find').callsFake(((f: Row) => users.find(f)) as any);
    sinon.stub(Users, 'countDocuments').callsFake(((f: Row) =>
      users.query(() => users.rows.filter((r) => matches(r, f)).length)) as any);
    sinon.stub(Users, 'updateOne').callsFake(((f: Row, u: Row) =>
      users.query(() => {
        const row = users.rows.find((r) => matches(r, f));
        if (row) Object.assign(row, u.$set ?? {});
        return { modifiedCount: row ? 1 : 0 };
      })) as any);
    sinon.stub(Org, 'findOne').callsFake(((f: Row) =>
      users.query(() =>
        [orgA, orgB].includes(String(f._id)) ? { _id: f._id, accountType: 'business', isDeleted: false } : null,
      )) as any);
    sinon.stub(UserActivities, 'findOne').callsFake((() => users.query(() => null)) as any);
    sinon.stub(UserActivities, 'insertMany').resolves([] as any);
    sinon.stub(UserDisplayPicture, 'find').callsFake((() => users.query(() => [])) as any);
    sinon.stub(UserCredentials, 'findOneAndUpdate').callsFake(((f: Row, u: Row) => {
      const row = credentials.rows.find((r) => matches(r, f));
      if (row) Object.assign(row, u.$set ?? {});
      return Promise.resolve(row ?? null);
    }) as any);

    events = {
      start: sinon.stub().resolves(),
      stop: sinon.stub().resolves(),
      publishEvent: sinon.stub().resolves(),
    };
    const logger: any = { debug: sinon.stub(), info: sinon.stub(), warn: sinon.stub(), error: sinon.stub() };
    const config: any = {
      jwtSecret: JWT_SECRET,
      scopedJwtSecret: SCOPED_SECRET,
      cmBackend: 'http://cm',
      frontendUrl: 'http://app',
      rsAvailable: 'false',
    };

    const container = new Container();
    container.bind('Logger').toConstantValue(logger);
    container.bind('AppConfig').toConstantValue(config);
    container
      .bind('AuthMiddleware')
      .toConstantValue(new AuthMiddleware(logger, new AuthTokenService(JWT_SECRET, SCOPED_SECRET)));
    container.bind('MailService').toConstantValue({ sendMail: sinon.stub().resolves({ statusCode: 200 }) });
    container.bind('AuthService').toConstantValue({});
    container.bind('EntitiesEventProducer').toConstantValue(events);
    container.bind('NotificationProducer').toConstantValue({});
    container.bind('OrgController').toConstantValue({});
    container
      .bind('UserController')
      .toConstantValue(
        new UserController(config, {} as any, {} as any, logger, events, {} as any),
      );

    const app = express();
    app.use(express.json());
    app.use('/users', createUserRouter(container));
    app.use(ErrorMiddleware.handleError());
    server = await new Promise<Server>((resolve) => {
      const s = app.listen(0, '127.0.0.1', () => resolve(s));
    });
    baseUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}/`.replace(/\/$/, '');
  });

  afterEach(async () => {
    sinon.restore();
    await new Promise((resolve) => server.close(resolve));
  });

  describe('reloading the app config', () => {
    it('does not send the reloaded secrets back in the response', async () => {
      sinon.stub(appConfigModule, 'loadAppConfig').resolves({
        jwtSecret: 'reloaded-jwt-secret',
        scopedJwtSecret: 'reloaded-scoped-secret',
        cookieSecret: 'reloaded-cookie-secret',
        mongo: { uri: 'mongodb://root:hunter2@mongo:27017', db: 'es' },
      } as any);

      const res = await call(
        'POST',
        '/updateAppConfig',
        fetchConfigJwtGenerator(ids.adminA, orgA, SCOPED_SECRET),
      );

      expect(res.status).to.equal(200);
      expect(res.body).to.deep.equal({ message: 'User configuration updated successfully' });
    });
  });
});
