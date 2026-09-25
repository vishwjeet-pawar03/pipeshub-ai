import 'reflect-metadata'
import { expect } from 'chai'
import sinon from 'sinon'
import { ADMIN_ID, USERS, Harness, call, sessionToken, startHarness } from './connectors-http-harness'
import { Logger } from '../../../../src/libs/services/logger.service'
import { SERVICE_UNAVAILABLE_MESSAGE } from '../../../../src/libs/errors/backend-error'

const admin = USERS.find((u) => u._id === ADMIN_ID)!

const CONNECTOR_ID = '3f2c9e7a-1b4d-4c8e-9a6f-2d5e8b7c1a90'
const JWT_SHAPE = /eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/

const serialise = (value: unknown): string => {
  const seen = new WeakSet<object>()
  return JSON.stringify(value, (_key, v: unknown) => {
    if (v instanceof Error) return { message: v.message, stack: v.stack, cause: v.cause }
    if (v !== null && typeof v === 'object') {
      if (seen.has(v)) return '[Circular]'
      seen.add(v)
    }
    return v
  })
}

describe('Connector routes: failed service calls keep credentials out of the logs', () => {
  let h: Harness
  let logged: Array<{ label: string; text: string }>

  // Report which log call leaked, never the leaked value itself.
  const leaking = (test: (text: string) => boolean): string[] =>
    logged.filter((entry) => test(entry.text)).map((entry) => entry.label)

  beforeEach(async () => {
    h = await startHarness()
    logged = []
    for (const level of ['error', 'warn', 'info', 'debug'] as const) {
      sinon.stub(Logger.prototype, level).callsFake((message: string, meta?: unknown) => {
        logged.push({ label: `${level}: ${message}`, text: `${message} ${serialise(meta)}` })
      })
    }
  })

  afterEach(async () => {
    sinon.restore()
    await h.close()
  })

  it("does not log the caller's token or the connector secret when the connector service is unreachable", async () => {
    h.backend.on('PUT', `/api/v1/connectors/${CONNECTOR_ID}/config/auth`, 'drop')
    const token = sessionToken(h, admin)

    const r = await call(h, 'PUT', `/${CONNECTOR_ID}/config/auth`, token, {
      auth: { clientId: 'client-abc', clientSecret: 'client-secret-do-not-log' },
    })

    expect(r.status).to.equal(503)
    expect(r.body.error).to.include({ message: SERVICE_UNAVAILABLE_MESSAGE })
    expect(logged.map((e) => e.label)).to.include('error: Connector service command failed')
    expect(
      leaking((text) => text.includes(token) || JWT_SHAPE.test(text) || text.includes('client-secret-do-not-log')),
    ).to.deep.equal([])
  })

  it('does not log the internal config token when the configuration manager is unreachable', async () => {
    h.backend.on('GET', '/api/v1/configurationManager/internal/connectors/googleWorkspaceOauthConfig', 'drop')

    const r = await call(h, 'POST', '/getTokenFromCode', sessionToken(h, admin), { tempCode: 'code-1' })

    expect(r.status).to.be.within(500, 599)
    expect(logged.map((e) => e.label)).to.include('error: Configuration Manager service command failed')
    expect(leaking((text) => JWT_SHAPE.test(text))).to.deep.equal([])
  })
})
