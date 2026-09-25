import 'reflect-metadata';
import { expect } from 'chai';
import sinon from 'sinon';
import { Container } from 'inversify';
import { createConfigurationManagerRouter } from '../../../../src/modules/configuration_manager/routes/cm_routes';
import { userAdminCheck } from '../../../../src/modules/user_management/middlewares/userAdminCheck';

// Every configuration route has to say who may call it. A route added without
// userAdminCheck would let any signed-in member read or change workspace
// secrets, so this walks the real router and fails on the first one that does.

// Routes a member may call. Each one is documented as not needing admin, and
// returns nothing a member must not see.
const MEMBER_ROUTES = new Set([
  'GET /smtpConfig/status',
  'GET /platform/feature-flags/effective',
  'GET /web-search',
  'GET /ai-models/available/:modelType',
  'GET /frontendPublicUrl',
  'GET /connectorPublicUrl',
]);

describe('Configuration manager routes: who may call them', () => {
  const authenticate = function authenticate(_req: any, _res: any, next: any) {
    next();
  };
  const serviceTokenCheck = function serviceTokenCheck(_req: any, _res: any, next: any) {
    next();
  };
  let routes: Array<{ id: string; path: string; handlers: unknown[] }>;

  before(() => {
    const container = new Container();
    const events = { start: sinon.stub(), publishEvent: sinon.stub(), stop: sinon.stub() };
    container.bind('KeyValueStoreService').toConstantValue({});
    container.bind('AppConfig').toConstantValue({ aiBackend: 'http://ai', cmBackend: 'http://cm' });
    container.bind('EntitiesEventProducer').toConstantValue(events);
    container.bind('AiConfigEventProducer').toConstantValue(events);
    container.bind('SyncEventProducer').toConstantValue(events);
    container.bind('ConfigService').toConstantValue({});
    container.bind('AuthMiddleware').toConstantValue({
      authenticate,
      scopedTokenValidator: () => serviceTokenCheck,
    });
    container.bind('SamlController').toConstantValue({});

    const router = createConfigurationManagerRouter(container);
    routes = router.stack
      .filter((layer: any) => layer.route)
      .flatMap((layer: any) =>
        Object.keys(layer.route.methods).map((method) => ({
          id: `${method.toUpperCase()} ${layer.route.path}`,
          path: layer.route.path as string,
          handlers: layer.route.stack.map((s: any) => s.handle),
        })),
      );
  });

  it('finds the routes it is meant to check', () => {
    expect(routes.length).to.be.greaterThan(60);
    for (const id of MEMBER_ROUTES) {
      expect(routes.map((r) => r.id), id).to.include(id);
    }
  });

  it('puts every internal route behind the service token and never a user session', () => {
    const wrong = routes
      .filter((r) => r.path.startsWith('/internal/'))
      .filter((r) => !r.handlers.includes(serviceTokenCheck) || r.handlers.includes(authenticate))
      .map((r) => r.id);

    expect(wrong).to.deep.equal([]);
  });

  it('puts every other route behind a signed-in user', () => {
    const open = routes
      .filter((r) => !r.path.startsWith('/internal/'))
      .filter((r) => r.handlers[0] !== authenticate)
      .map((r) => r.id);

    expect(open).to.deep.equal([]);
  });

  it('requires an admin on every user route except the few members may read', () => {
    const missingAdmin = routes
      .filter((r) => !r.path.startsWith('/internal/') && !MEMBER_ROUTES.has(r.id))
      .filter((r) => !r.handlers.includes(userAdminCheck))
      .map((r) => r.id);

    expect(missingAdmin).to.deep.equal([]);
  });

  it('checks for an admin before the handler that does the work', () => {
    const late = routes
      .filter((r) => r.handlers.includes(userAdminCheck))
      .filter((r) => r.handlers.indexOf(userAdminCheck) === r.handlers.length - 1)
      .map((r) => r.id);

    expect(late).to.deep.equal([]);
  });
});
