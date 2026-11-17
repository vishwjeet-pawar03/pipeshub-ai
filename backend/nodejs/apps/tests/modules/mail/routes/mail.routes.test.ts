import 'reflect-metadata'
import { expect } from 'chai'
import sinon from 'sinon'
import { Container } from 'inversify'
import { createMailServiceRouter, smtpConfigSchema } from '../../../../src/modules/mail/routes/mail.routes'
import { AuthMiddleware } from '../../../../src/libs/middlewares/auth.middleware'
import { TokenScopes } from '../../../../src/libs/enums/token-scopes.enum'

describe('Mail Routes', () => {
  let container: Container
  let mockAuthMiddleware: any
  let mockMailController: any

  beforeEach(() => {
    container = new Container()

    mockAuthMiddleware = {
      authenticate: sinon.stub().callsFake((_req: any, _res: any, next: any) => next()),
      scopedTokenValidator: sinon.stub().returns(
        sinon.stub().callsFake((_req: any, _res: any, next: any) => next()),
      ),
    }

    mockMailController = {
      sendMail: sinon.stub().resolves(),
    }

    const mockLogger = {
      info: sinon.stub(),
      debug: sinon.stub(),
      warn: sinon.stub(),
      error: sinon.stub(),
    }

    container.bind<AuthMiddleware>('AuthMiddleware').toConstantValue(mockAuthMiddleware as any)
    container.bind<any>('MailController').toConstantValue(mockMailController)
    container.bind<any>('Logger').toConstantValue(mockLogger)
  })

  afterEach(() => {
    sinon.restore()
  })

  it('should export createMailServiceRouter function', () => {
    expect(createMailServiceRouter).to.be.a('function')
  })

  it('should export smtpConfigSchema', () => {
    expect(smtpConfigSchema).to.exist
  })

  it('should create a router successfully', () => {
    const router = createMailServiceRouter(container)
    expect(router).to.be.a('function')
  })

  it('should have route handlers registered', () => {
    const router = createMailServiceRouter(container)
    const routes = (router as any).stack || []
    expect(routes.length).to.be.greaterThan(0)
  })

  describe('route registration', () => {
    it('should register POST /emails/sendEmail route', () => {
      const router = createMailServiceRouter(container)
      const routes = (router as any).stack

      const sendEmailRoute = routes.find(
        (layer: any) =>
          layer.route &&
          layer.route.path === '/emails/sendEmail' &&
          layer.route.methods.post,
      )
      expect(sendEmailRoute).to.not.be.undefined
    })

    it('should register POST /updateSmtpConfig route', () => {
      const router = createMailServiceRouter(container)
      const routes = (router as any).stack

      const updateConfigRoute = routes.find(
        (layer: any) =>
          layer.route &&
          layer.route.path === '/updateSmtpConfig' &&
          layer.route.methods.post,
      )
      expect(updateConfigRoute).to.not.be.undefined
    })
  })

  describe('route count', () => {
    it('should register exactly 2 route endpoints', () => {
      const router = createMailServiceRouter(container)
      const routes = (router as any).stack.filter((layer: any) => layer.route)

      expect(routes.length).to.equal(2)
    })
  })

  describe('middleware chains', () => {
    it('should use attachContainerMiddleware as router-level middleware', () => {
      const router = createMailServiceRouter(container)
      const middlewareLayers = (router as any).stack.filter(
        (layer: any) => !layer.route,
      )
      expect(middlewareLayers.length).to.be.greaterThanOrEqual(1)
    })

    it('should have smtp config checker and scoped token validator on sendEmail route', () => {
      const router = createMailServiceRouter(container)
      const routes = (router as any).stack.filter((layer: any) => layer.route)

      const sendEmailRoute = routes.find(
        (layer: any) => layer.route.path === '/emails/sendEmail' && layer.route.methods.post,
      )
      expect(sendEmailRoute).to.not.be.undefined
      // smtpConfigChecker + scopedTokenValidator + handler
      expect(sendEmailRoute.route.stack.length).to.be.greaterThanOrEqual(2)
    })

    it('should have scoped token validator on updateSmtpConfig route', () => {
      const router = createMailServiceRouter(container)
      const routes = (router as any).stack.filter((layer: any) => layer.route)

      const updateConfigRoute = routes.find(
        (layer: any) => layer.route.path === '/updateSmtpConfig' && layer.route.methods.post,
      )
      expect(updateConfigRoute).to.not.be.undefined
      // scopedTokenValidator + handler
      expect(updateConfigRoute.route.stack.length).to.be.greaterThanOrEqual(2)
    })
  })

  describe('service-token scope (no session auth accepted)', () => {
    it('should gate sendEmail on the send-mail scope, with no session/authenticate middleware in the chain', () => {
      const router = createMailServiceRouter(container)
      const routes = (router as any).stack.filter((layer: any) => layer.route)
      const sendEmailRoute = routes.find(
        (layer: any) => layer.route.path === '/emails/sendEmail' && layer.route.methods.post,
      )
      // smtpConfigChecker, scopedTokenValidator, handler: no authenticate/session middleware is in the chain.
      expect(sendEmailRoute.route.stack.length).to.equal(3)
      expect(mockAuthMiddleware.authenticate.called).to.equal(false)
      expect(mockAuthMiddleware.scopedTokenValidator.calledWith(TokenScopes.SEND_MAIL)).to.equal(true)
    })

    it('should gate updateSmtpConfig on the fetch-config scope, with no session/authenticate middleware in the chain', () => {
      const router = createMailServiceRouter(container)
      const routes = (router as any).stack.filter((layer: any) => layer.route)
      const updateConfigRoute = routes.find(
        (layer: any) => layer.route.path === '/updateSmtpConfig' && layer.route.methods.post,
      )
      // scopedTokenValidator, handler: no authenticate/session middleware is in the chain.
      expect(updateConfigRoute.route.stack.length).to.equal(2)
      expect(mockAuthMiddleware.authenticate.called).to.equal(false)
      expect(mockAuthMiddleware.scopedTokenValidator.calledWith(TokenScopes.FETCH_CONFIG)).to.equal(true)
    })
  })

  describe('smtpConfigSchema', () => {
    it('should validate valid SMTP config', () => {
      const validData = {
        body: {
          host: 'smtp.example.com',
          port: 587,
          username: 'user',
          password: 'pass',
          fromEmail: 'noreply@example.com',
        },
      }
      const result = smtpConfigSchema.safeParse(validData)
      expect(result.success).to.be.true
    })

    it('should reject missing host', () => {
      const invalidData = {
        body: {
          port: 587,
          fromEmail: 'noreply@example.com',
        },
      }
      const result = smtpConfigSchema.safeParse(invalidData)
      expect(result.success).to.be.false
    })

    it('should reject missing port', () => {
      const invalidData = {
        body: {
          host: 'smtp.example.com',
          fromEmail: 'noreply@example.com',
        },
      }
      const result = smtpConfigSchema.safeParse(invalidData)
      expect(result.success).to.be.false
    })

    it('should reject missing fromEmail', () => {
      const invalidData = {
        body: {
          host: 'smtp.example.com',
          port: 587,
        },
      }
      const result = smtpConfigSchema.safeParse(invalidData)
      expect(result.success).to.be.false
    })

    it('should reject empty host', () => {
      const invalidData = {
        body: {
          host: '',
          port: 587,
          fromEmail: 'noreply@example.com',
        },
      }
      const result = smtpConfigSchema.safeParse(invalidData)
      expect(result.success).to.be.false
    })

    it('should reject empty fromEmail', () => {
      const invalidData = {
        body: {
          host: 'smtp.example.com',
          port: 587,
          fromEmail: '',
        },
      }
      const result = smtpConfigSchema.safeParse(invalidData)
      expect(result.success).to.be.false
    })

    it('should allow optional username and password', () => {
      const validData = {
        body: {
          host: 'smtp.example.com',
          port: 25,
          fromEmail: 'noreply@example.com',
        },
      }
      const result = smtpConfigSchema.safeParse(validData)
      expect(result.success).to.be.true
    })
  })
})
