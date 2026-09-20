import { Router } from 'express';
import { Container } from 'inversify';
import { ValidationMiddleware } from '../../../libs/middlewares/validation.middleware';
import { AuthMiddleware } from '../../../libs/middlewares/auth.middleware';
import { ServiceAccountsController } from '../controller/service-accounts.controller';
import { userAdminCheck } from '../middlewares/userAdminCheck';
import {
  createServiceAccountSchema,
  serviceAccountIdParamsSchema,
  updateServiceAccountSchema,
} from '../validators/service-account.validators';

export function createServiceAccountsRouter(container: Container): Router {
  const router = Router();
  const controller = container.get<ServiceAccountsController>(
    'ServiceAccountsController',
  );
  const authMiddleware = container.get<AuthMiddleware>('AuthMiddleware');

  router.use(authMiddleware.authenticate.bind(authMiddleware));

  // Every route is admin-only. A service account is a principal with its own
  // view of the organisation's documents, so creating one is an act of
  // granting access, and that belongs with the people who administer access.
  router.use(userAdminCheck);

  router.get('/', (req, res, next) => controller.list(req, res, next));

  router.post(
    '/',
    ValidationMiddleware.validate(createServiceAccountSchema),
    (req, res, next) => controller.create(req, res, next),
  );

  router.get(
    '/:id',
    ValidationMiddleware.validate(serviceAccountIdParamsSchema),
    (req, res, next) => controller.get(req, res, next),
  );

  router.patch(
    '/:id',
    ValidationMiddleware.validate(updateServiceAccountSchema),
    (req, res, next) => controller.update(req, res, next),
  );

  router.delete(
    '/:id',
    ValidationMiddleware.validate(serviceAccountIdParamsSchema),
    (req, res, next) => controller.remove(req, res, next),
  );

  return router;
}
