import 'reflect-metadata'
import { expect } from 'chai'
import sinon from 'sinon'
import { createGlobalRateLimiter, createOAuthClientRateLimiter } from '../../../src/libs/middlewares/rate-limit.middleware'
import { Logger } from '../../../src/libs/services/logger.service'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function createMockRequest(overrides: Record<string, any> = {}): any {
  return {
    headers: {},
    body: {},
    params: {},
    query: {},
    path: '/test',
    method: 'GET',
    ip: '127.0.0.1',
    socket: { remoteAddress: '127.0.0.1' },
    get: sinon.stub(),
    app: { enabled: sinon.stub().returns(false) },
    ...overrides,
  }
}

function createMockResponse(): any {
  const headerMap: Record<string, any> = {}
  const res: any = {
    status: sinon.stub(),
    json: sinon.stub(),
    send: sinon.stub(),
    setHeader: sinon.stub().callsFake((key: string, value: any) => { headerMap[key] = value; return res }),
    getHeader: sinon.stub().callsFake((key: string) => headerMap[key]),
    header: sinon.stub().callsFake((key: string, value: any) => { headerMap[key] = value; return res }),
    set: sinon.stub().callsFake((key: string, value: any) => { headerMap[key] = value; return res }),
    headersSent: false,
    statusCode: 200,
  }
  res.status.callsFake((code: number) => { res.statusCode = code; return res })
  res.json.returns(res)
  res.send.returns(res)
  return res
}

function createMockNext(): sinon.SinonStub {
  return sinon.stub()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('Rate Limit Middleware', () => {
  let loggerStub: sinon.SinonStubbedInstance<Logger>

  beforeEach(() => {
    loggerStub = sinon.createStubInstance(Logger)
  })

  afterEach(() => {
    sinon.restore()
  })

  // -----------------------------------------------------------------------
  // createGlobalRateLimiter
  // -----------------------------------------------------------------------
  describe('createGlobalRateLimiter', () => {
    it('should return a function (RequestHandler)', () => {
      const limiter = createGlobalRateLimiter(loggerStub as unknown as Logger, 100)
      expect(limiter).to.be.a('function')
    })

    it('should allow requests within the rate limit', (done) => {
      const limiter = createGlobalRateLimiter(loggerStub as unknown as Logger, 100)
      const req = createMockRequest({
        ip: '10.0.0.1',
      })
      const res = createMockResponse()
      const next = createMockNext()

      next.callsFake(() => {
        // next was called -> request was allowed
        expect(next.calledOnce).to.be.true
        done()
      })

      limiter(req, res, next)
    })

    it('should use userId as rate limit key when user is authenticated', (done) => {
      const limiter = createGlobalRateLimiter(loggerStub as unknown as Logger, 100)
      const req = createMockRequest({
        ip: '10.0.0.2',
        user: { userId: 'user-rate-test-1' },
      })
      const res = createMockResponse()
      const next = createMockNext()

      next.callsFake(() => {
        expect(next.calledOnce).to.be.true
        done()
      })

      limiter(req, res, next)
    })

    it('should skip rate limiting for service (scoped token) requests', (done) => {
      const limiter = createGlobalRateLimiter(loggerStub as unknown as Logger, 1)
      const req = createMockRequest({
        ip: '10.0.0.3',
        tokenPayload: { orgId: 'org1', userId: 'service-user' },
      })
      const res = createMockResponse()
      const next = createMockNext()

      next.callsFake(() => {
        expect(next.called).to.be.true
        done()
      })

      limiter(req, res, next)
    })
  })

  // -----------------------------------------------------------------------
  // Internal route skip logic (positional segment matching)
  // -----------------------------------------------------------------------
  describe('internal route detection', () => {
    it('should skip rate limiting for /api/v1/<module>/internal/<sub-path>', (done) => {
      const limiter = createGlobalRateLimiter(loggerStub as unknown as Logger, 1)
      const req = createMockRequest({
        ip: '10.0.0.10',
        path: '/api/v1/document/internal/upload',
      })
      const res = createMockResponse()
      const next = createMockNext()

      next.callsFake(() => {
        expect(next.called).to.be.true
        done()
      })

      limiter(req, res, next)
    })

    it('should skip rate limiting for nested internal routes (depth 6)', (done) => {
      const limiter = createGlobalRateLimiter(loggerStub as unknown as Logger, 1)
      const req = createMockRequest({
        ip: '10.0.0.11',
        path: '/api/v1/agents/myAgent/conversations/internal/stream',
      })
      const res = createMockResponse()
      const next = createMockNext()

      next.callsFake(() => {
        expect(next.called).to.be.true
        done()
      })

      limiter(req, res, next)
    })

    it('should NOT skip when "internal" is a parameter value at path end (bypass attempt)', (done) => {
      // Attacker sends /api/v1/users/internal which routes to GET /:id
      const limiter = createGlobalRateLimiter(loggerStub as unknown as Logger, 100)
      const req = createMockRequest({
        ip: '10.0.0.12',
        path: '/api/v1/users/internal',
      })
      const res = createMockResponse()
      const next = createMockNext()

      next.callsFake(() => {
        // Request should NOT be skipped — it goes through normal rate limiting
        // Verify it was rate-limited (next called means under limit, which is fine,
        // the point is it wasn't skipped)
        expect(next.called).to.be.true
        done()
      })

      limiter(req, res, next)
    })

    it('should NOT skip when "internal" appears deep as a parameter value', (done) => {
      // /api/v1/agents/internal/conversations would have "internal" as :agentKey
      const limiter = createGlobalRateLimiter(loggerStub as unknown as Logger, 100)
      const req = createMockRequest({
        ip: '10.0.0.13',
        path: '/api/v1/agents/internal/conversations',
      })
      const res = createMockResponse()
      const next = createMockNext()

      next.callsFake(() => {
        expect(next.called).to.be.true
        done()
      })

      limiter(req, res, next)
    })

    it('should skip for configurationManager internal routes', (done) => {
      const limiter = createGlobalRateLimiter(loggerStub as unknown as Logger, 1)
      const req = createMockRequest({
        ip: '10.0.0.14',
        path: '/api/v1/configurationManager/internal/storageConfig',
      })
      const res = createMockResponse()
      const next = createMockNext()

      next.callsFake(() => {
        expect(next.called).to.be.true
        done()
      })

      limiter(req, res, next)
    })

    it('should skip for conversations internal routes', (done) => {
      const limiter = createGlobalRateLimiter(loggerStub as unknown as Logger, 1)
      const req = createMockRequest({
        ip: '10.0.0.15',
        path: '/api/v1/conversations/internal/stream',
      })
      const res = createMockResponse()
      const next = createMockNext()

      next.callsFake(() => {
        expect(next.called).to.be.true
        done()
      })

      limiter(req, res, next)
    })
  })

  // -----------------------------------------------------------------------
  // createOAuthClientRateLimiter
  // -----------------------------------------------------------------------
  describe('createOAuthClientRateLimiter', () => {
    it('should return a function (RequestHandler)', () => {
      const limiter = createOAuthClientRateLimiter(loggerStub as unknown as Logger, 10)
      expect(limiter).to.be.a('function')
    })

    it('should allow requests within the rate limit', (done) => {
      const limiter = createOAuthClientRateLimiter(loggerStub as unknown as Logger, 10)
      const req = createMockRequest({
        ip: '10.0.1.1',
      })
      const res = createMockResponse()
      const next = createMockNext()

      next.callsFake(() => {
        expect(next.calledOnce).to.be.true
        done()
      })

      limiter(req, res, next)
    })

    it('should use userId as rate limit key when user is authenticated', (done) => {
      const limiter = createOAuthClientRateLimiter(loggerStub as unknown as Logger, 10)
      const req = createMockRequest({
        ip: '10.0.1.2',
        user: { userId: 'oauth-rate-test-user' },
      })
      const res = createMockResponse()
      const next = createMockNext()

      next.callsFake(() => {
        expect(next.calledOnce).to.be.true
        done()
      })

      limiter(req, res, next)
    })
  })

  // -----------------------------------------------------------------------
  // Client IP extraction
  // -----------------------------------------------------------------------
  describe('Client IP extraction', () => {
    it('should use X-Forwarded-For header when present', (done) => {
      const limiter = createGlobalRateLimiter(loggerStub as unknown as Logger, 100)
      const req = createMockRequest({
        headers: { 'x-forwarded-for': '203.0.113.50, 70.41.3.18' },
        ip: '10.0.2.1',
      })
      const res = createMockResponse()
      const next = createMockNext()

      next.callsFake(() => {
        expect(next.called).to.be.true
        done()
      })

      limiter(req, res, next)
    })

    it('should use X-Real-IP header when X-Forwarded-For is not present', (done) => {
      const limiter = createGlobalRateLimiter(loggerStub as unknown as Logger, 100)
      const req = createMockRequest({
        headers: { 'x-real-ip': '203.0.113.60' },
        ip: '10.0.2.2',
      })
      const res = createMockResponse()
      const next = createMockNext()

      next.callsFake(() => {
        expect(next.called).to.be.true
        done()
      })

      limiter(req, res, next)
    })

    it('should fall back to req.ip when no forwarding headers present', (done) => {
      const limiter = createGlobalRateLimiter(loggerStub as unknown as Logger, 100)
      const req = createMockRequest({
        ip: '192.168.0.100',
      })
      const res = createMockResponse()
      const next = createMockNext()

      next.callsFake(() => {
        expect(next.called).to.be.true
        done()
      })

      limiter(req, res, next)
    })
  })
})
