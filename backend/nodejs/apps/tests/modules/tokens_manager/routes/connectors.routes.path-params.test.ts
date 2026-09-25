import 'reflect-metadata'
import { expect } from 'chai'
import sinon from 'sinon'
import {
  MEMBER,
  ORG_A,
  Harness,
  call,
  sessionToken,
  startHarness,
} from './connectors-http-harness'

const member = MEMBER

// Express decodes %2F and %3F inside a path parameter, and the controllers
// interpolate the parameter into the connector-service URL, where `..` and `?`
// are structural again.
const HOSTILE_IDS = ['..%2F..%2Fadmin', 'vector-store%2Fcleanup%3F', 'abc%3Fx%3D1', 'a.b', 'a%23b']

const ROUTES: Array<{ method: string; path: (id: string) => string; body?: unknown }> = [
  { method: 'GET', path: (id) => `/${id}/stats` },
  { method: 'POST', path: (id) => `/${id}/reindex`, body: {} },
  { method: 'POST', path: (id) => `/${id}/resync`, body: { connectorName: 'Google Drive' } },
  { method: 'PUT', path: (id) => `/${id}/config`, body: {} },
  { method: 'PUT', path: (id) => `/${id}/config/auth`, body: { auth: {} } },
  { method: 'PUT', path: (id) => `/${id}/config/filters-sync`, body: {} },
  { method: 'PUT', path: (id) => `/${id}/name`, body: { instanceName: 'Renamed' } },
  { method: 'GET', path: (id) => `/${id}/oauth/authorize` },
  { method: 'POST', path: (id) => `/${id}/filters`, body: { filters: {} } },
  { method: 'GET', path: (id) => `/${id}/filters/folders/options` },
  { method: 'POST', path: (id) => `/${id}/toggle`, body: { type: 'sync' } },
]

describe('Connector routes: path parameters stay inside their URL segment', () => {
  let h: Harness

  beforeEach(async () => {
    h = await startHarness()
  })

  afterEach(async () => {
    sinon.restore()
    await h.close()
  })

  it("does not let a member reach the admin-only vector store cleanup through reindex's connector id", async () => {
    h.backend.on('POST', '/api/v1/connectors/vector-store/cleanup', { status: 202, body: { success: true } })

    const r = await call(h, 'POST', '/vector-store%2Fcleanup%3F/reindex', sessionToken(h, member), {})

    expect(h.backend.callsTo('POST', '/api/v1/connectors/vector-store/cleanup')).to.have.length(0)
    expect(r.status).to.equal(400)
  })

  for (const route of ROUTES) {
    it(`${route.method} ${route.path(':connectorId')} rejects connector ids that are not a plain key`, async () => {
      const token = sessionToken(h, member)
      for (const id of HOSTILE_IDS) {
        const r = await call(h, route.method, route.path(id), token, route.body)
        expect(r.status, `status for ${id}`).to.equal(400)
      }
      expect(h.backend.calls.map((c) => `${c.method} ${c.path}`)).to.deep.equal([])
    })
  }

  it('rejects a filter key that would climb out of the options path', async () => {
    const r = await call(
      h,
      'GET',
      '/3f2c9e7a-1b4d-4c8e-9a6f-2d5e8b7c1a90/filters/..%2F..%2F..%2Fstats%3F/options',
      sessionToken(h, member),
    )

    expect(r.status).to.equal(400)
    expect(h.backend.calls).to.have.length(0)
  })

  for (const id of ['3f2c9e7a-1b4d-4c8e-9a6f-2d5e8b7c1a90', `knowledgeBase_${ORG_A}`]) {
    it(`still forwards a real connector id (${id}) to its own path`, async () => {
      h.backend.on('POST', `/api/v1/connectors/${id}/reindex`, { status: 200, body: { success: true } })
      h.backend.on('GET', `/api/v1/connectors/${id}/filters/space_keys/options`, { status: 200, body: { options: [] } })
      const token = sessionToken(h, member)

      const reindex = await call(h, 'POST', `/${id}/reindex`, token, {})
      const options = await call(h, 'GET', `/${id}/filters/space_keys/options`, token)

      expect(reindex.status).to.equal(200)
      expect(options.status).to.equal(200)
      expect(h.backend.calls.map((c) => c.path)).to.deep.equal([
        `/api/v1/connectors/${id}/reindex`,
        `/api/v1/connectors/${id}/filters/space_keys/options`,
      ])
    })
  }
})
