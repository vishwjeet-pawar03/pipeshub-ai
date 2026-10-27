import 'reflect-metadata'
import { expect } from 'chai'
import sinon from 'sinon'
import jwt from 'jsonwebtoken'
import mongoose from 'mongoose'
import {
  STAND_IN_TOKEN_TTL_SECONDS,
  hydrateScopedRequestAsUser,
  stableObjectIdHexForExternalEmail,
} from '../../../../src/modules/enterprise_search/utils/scoped-request'
import { AIServiceCommand } from '../../../../src/libs/commands/ai_service/ai.service.command'
import { InternalServerError, NotFoundError } from '../../../../src/libs/errors/http.errors'
import * as cmController from '../../../../src/modules/configuration_manager/controller/cm_controller'
import { Org } from '../../../../src/modules/user_management/schema/org.schema'
import { Users } from '../../../../src/modules/user_management/schema/users.schema'

describe('hydrateScopedRequestAsUser', () => {
  afterEach(() => {
    sinon.restore()
  })

  it('gives the token it mints the role claim Node requires of a session, as member', async () => {
    const orgId = new mongoose.Types.ObjectId()
    sinon.stub(Org, 'find').returns({
      limit: sinon.stub().returnsThis(),
      lean: sinon.stub().returnsThis(),
      exec: sinon.stub().resolves([{ _id: orgId }]),
    } as any)
    sinon.stub(Users, 'findOne').resolves({
      _id: new mongoose.Types.ObjectId(),
      orgId,
      email: 'person@example.com',
      fullName: 'Person',
      role: 'admin',
    } as any)
    const req: any = {
      headers: { authorization: 'Bearer slack-service-token' },
      params: {},
      tokenPayload: { email: 'person@example.com' },
    }

    await hydrateScopedRequestAsUser(req, {
      jwtSecret: 'session-secret',
      scopedJwtSecret: 'scoped-secret',
    } as any)

    const token = req.headers.authorization.replace('Bearer ', '')
    const claims = jwt.verify(token, 'session-secret') as Record<string, unknown>
    expect(claims.role).to.equal('member')
    expect(claims.email).to.equal('person@example.com')
    // Node honours it like a session, so it must not outlive the request.
    expect((claims.exp as number) - (claims.iat as number)).to.equal(STAND_IN_TOKEN_TTL_SECONDS)
    expect(STAND_IN_TOKEN_TTL_SECONDS).to.be.at.most(60)
  })
})

describe('hydrateScopedRequestAsUser: Slack caller without an account', () => {
  const appConfig = {
    jwtSecret: 'session-secret',
    scopedJwtSecret: 'scoped-secret',
    aiBackend: 'http://ai.local',
  } as any
  const email = 'outsider@example.com'
  let orgId: mongoose.Types.ObjectId
  let checkHeaders: Record<string, string> | undefined

  const makeReq = (): any => ({
    headers: { authorization: 'Bearer incoming-slack-token', cookie: 'session=abc', 'x-other': '1' },
    params: { agentKey: 'agent-1' },
    tokenPayload: { email, scopes: ['conversation:create'], configId: 'cfg-1' },
  })

  const stubCheck = (result: { statusCode: number; data?: unknown } | Error) => {
    sinon.stub(AIServiceCommand.prototype, 'execute').callsFake(function (this: any) {
      checkHeaders = this.headers
      return result instanceof Error ? Promise.reject(result) : Promise.resolve(result as any)
    })
  }

  beforeEach(() => {
    checkHeaders = undefined
    orgId = new mongoose.Types.ObjectId()
    sinon.stub(Org, 'find').returns({
      limit: sinon.stub().returnsThis(),
      lean: sinon.stub().returnsThis(),
      exec: sinon.stub().resolves([{ _id: orgId }]),
    } as any)
    sinon.stub(Org, 'findOne').resolves({ _id: orgId } as any)
    sinon.stub(Users, 'findOne').resolves(null)
    sinon.stub(cmController, 'getSlackBotStore').resolves({
      configs: [{ id: 'cfg-1', agentId: 'agent-1' }],
    } as any)
  })

  afterEach(() => {
    sinon.restore()
  })

  it('asks the AI service with a short token naming the org and the stand-in user, and nothing else', async () => {
    stubCheck({ statusCode: 200, data: { isServiceAccount: true } })
    const req = makeReq()

    await hydrateScopedRequestAsUser(req, appConfig, {} as any)

    expect(Object.keys(checkHeaders!).sort()).to.deep.equal(['authorization', 'content-type'])
    const claims = jwt.verify(checkHeaders!.authorization.replace('Bearer ', ''), 'scoped-secret') as Record<string, any>
    expect(claims.userId).to.equal(stableObjectIdHexForExternalEmail(email))
    expect(String(claims.orgId)).to.equal(String(orgId))
    expect(claims.email).to.equal(email)
    expect(claims.scopes).to.deep.equal(['conversation:create'])
    expect(claims.isServiceAccount).to.equal(undefined)
    expect(claims.exp - claims.iat).to.equal(STAND_IN_TOKEN_TTL_SECONDS)
    expect(req.user.isServiceAccount).to.equal(true)
    expect(String(req.user.userId)).to.equal(stableObjectIdHexForExternalEmail(email))
  })

  for (const statusCode of [403, 404]) {
    it(`treats ${statusCode} from the check as "not a service agent"`, async () => {
      stubCheck({ statusCode, data: { detail: 'no' } })
      let error: any
      try {
        await hydrateScopedRequestAsUser(makeReq(), appConfig, {} as any)
      } catch (e) {
        error = e
      }
      expect(error).to.be.instanceOf(NotFoundError)
      expect(error.message).to.contain('User not found')
    })
  }

  for (const [label, result] of [
    ['a 401', { statusCode: 401, data: { detail: 'Token missing orgId' } }],
    ['a 500', { statusCode: 500, data: {} }],
    ['a network error', new Error('ECONNREFUSED')],
  ] as const) {
    it(`reports ${label} from the check as a server error, not a missing account`, async () => {
      stubCheck(result as any)
      let error: any
      try {
        await hydrateScopedRequestAsUser(makeReq(), appConfig, {} as any)
      } catch (e) {
        error = e
      }
      expect(error).to.be.instanceOf(InternalServerError)
      expect(error.message).to.not.contain('User not found')
    })
  }
})
