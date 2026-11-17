import 'reflect-metadata';
import { expect } from 'chai';
import { createProjectsRouter } from '../../../../src/modules/projects/routes/project.routes';
import { requireProjectsEnabled } from '../../../../src/modules/projects/utils/projects-feature';

describe('projects/routes/project.routes', () => {
  const authenticate = function authenticate() {};
  const container = {
    get: (id: string) => (id === 'AuthMiddleware' ? { authenticate } : {}),
  } as any;

  it('checks the Projects flag after authenticate on every route', () => {
    const router = createProjectsRouter(container);
    const routes = router.stack.filter((layer: any) => layer.route);

    expect(router.stack.length).to.equal(routes.length);
    expect(routes.length).to.be.greaterThan(0);
    for (const layer of routes as any[]) {
      const handlers: unknown[] = layer.route.stack.map((s: any) => s.handle);
      const auth = handlers.indexOf(authenticate);
      expect(auth, layer.route.path).to.be.greaterThan(-1);
      expect(handlers.indexOf(requireProjectsEnabled), layer.route.path).to.equal(auth + 1);
    }
  });

  // An OAuth token or PAT without the matching scope must get 403 from each CRUD route
  // (api-access... no: projects.crud.scopes). The middleware right after requireProjectsEnabled
  // is a fresh closure per route (requireScopes(...) is a factory), so it cannot be compared by
  // reference like authenticate/requireProjectsEnabled above; this invokes it directly instead.
  const findRoute = (router: any, method: string, path: string) =>
    router.stack.find(
      (layer: any) =>
        layer.route && layer.route.path === path && layer.route.methods[method],
    );

  const scopeCheckOf = (router: any, method: string, path: string) => {
    const layer = findRoute(router, method, path);
    expect(layer, `${method} ${path}`).to.exist;
    const handlers: any[] = layer.route.stack.map((s: any) => s.handle);
    const afterFlag = handlers.indexOf(requireProjectsEnabled) + 1;
    return handlers[afterFlag];
  };

  const expectForbidden = (middleware: any, scope: string) => {
    const req = { user: { isOAuth: true, oauthScopes: [] } } as any;
    let error: any;
    middleware(req, {}, (e: any) => (error = e));
    expect(error, `${scope} should be required`).to.exist;
    expect(error.statusCode ?? error.status).to.equal(403);
    expect(error.message).to.include(scope);
  };

  const expectAllowed = (middleware: any, scope: string) => {
    const req = { user: { isOAuth: true, oauthScopes: [scope] } } as any;
    let error: any;
    middleware(req, {}, (e: any) => (error = e));
    expect(error, `${scope} should be enough`).to.not.exist;
  };

  it('requires project:read on GET /, project:write on POST / and PATCH /:projectId, project:delete on DELETE /:projectId', () => {
    const router = createProjectsRouter(container);
    const cases: [string, string, string][] = [
      ['get', '/', 'project:read'],
      ['post', '/', 'project:write'],
      ['patch', '/:projectId', 'project:write'],
      ['delete', '/:projectId', 'project:delete'],
    ];
    for (const [method, path, scope] of cases) {
      const middleware = scopeCheckOf(router, method, path);
      expectForbidden(middleware, scope);
      expectAllowed(middleware, scope);
    }
  });
});
