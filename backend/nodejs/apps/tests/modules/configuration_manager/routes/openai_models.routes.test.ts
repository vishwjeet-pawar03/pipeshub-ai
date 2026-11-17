/// <reference types="mocha" />
import 'reflect-metadata';
import { expect } from 'chai';
import sinon from 'sinon';
import { Container } from 'inversify';
import { createOpenAIModelsRouter } from '../../../../src/modules/configuration_manager/routes/openai_models.routes';
import { AuthMiddleware } from '../../../../src/libs/middlewares/auth.middleware';
import { KeyValueStoreService } from '../../../../src/libs/services/keyValueStore.service';

describe('createOpenAIModelsRouter', () => {
  let container: Container;

  beforeEach(() => {
    container = new Container();
    container
      .bind<AuthMiddleware>('AuthMiddleware')
      .toConstantValue({ authenticate: (_req: any, _res: any, next: any) => next() } as any);
    container
      .bind<KeyValueStoreService>('KeyValueStoreService')
      .toConstantValue({} as any);
  });

  const routeFor = (path: string) => {
    const router = createOpenAIModelsRouter(container);
    return (router.stack as any[]).find(
      (layer) => layer.route?.path === path && layer.route.methods.get,
    );
  };

  it('registers GET /models and GET /models/* needing a signed-in caller', () => {
    for (const path of ['/models', '/models/*']) {
      const layer = routeFor(path);
      expect(layer, path).to.exist;
      // authenticate, requireScopes, handler
      expect(layer.route.stack.length).to.equal(3);
    }
  });

  describe('config:read scope', () => {
    const runScopeCheck = (path: string, oauthScopes: string[]) => {
      const layer = routeFor(path);
      const requireScopes = layer.route.stack[1].handle;
      const next = sinon.spy();
      requireScopes({ user: { isOAuth: true, oauthScopes } }, {}, next);
      return next.firstCall.args[0];
    };

    for (const path of ['/models', '/models/*']) {
      it(`admits an OAuth token with config:read on ${path}`, () => {
        expect(runScopeCheck(path, ['config:read'])).to.be.undefined;
      });

      it(`refuses an OAuth token without config:read on ${path}`, () => {
        const error = runScopeCheck(path, ['conversation:read']);
        expect(error).to.be.an('error');
        expect(error.message).to.equal('Insufficient scope. Required: config:read');
      });
    }
  });
});
