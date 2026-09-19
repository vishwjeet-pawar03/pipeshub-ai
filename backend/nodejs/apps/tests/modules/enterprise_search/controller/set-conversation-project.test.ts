import 'reflect-metadata'
import { expect } from 'chai'
import sinon from 'sinon'
import mongoose from 'mongoose'
import { setConversationProject } from '../../../../src/modules/enterprise_search/controller/es_controller'
import { ChatSession } from '../../../../src/modules/enterprise_search/schema/chat.session.schema'
import { ProjectService } from '../../../../src/modules/projects/services/project.service'

const USER_ID = 'aaaaaaaaaaaaaaaaaaaaaaaa'
const ORG_ID = 'bbbbbbbbbbbbbbbbbbbbbbbb'
const CONVERSATION_ID = 'cccccccccccccccccccccccc'
const PROJECT_ID = 'dddddddddddddddddddddddd'

function createMockRequest(body: Record<string, unknown>): any {
  return {
    headers: {},
    body,
    params: { conversationId: CONVERSATION_ID },
    query: {},
    user: { userId: USER_ID, orgId: ORG_ID },
    context: { requestId: 'req-1' },
  }
}

function createMockResponse(): any {
  const res: any = { status: sinon.stub(), json: sinon.stub() }
  res.status.returns(res)
  res.json.returns(res)
  return res
}

describe('setConversationProject response', () => {
  afterEach(() => {
    sinon.restore()
  })

  it('returns projectId and projectVisibility as null after unlinking', async () => {
    sinon.stub(ChatSession, 'findOne').resolves({ _id: CONVERSATION_ID } as any)
    // An unlinked session has neither field; the response must still carry both keys.
    sinon.stub(ChatSession, 'findOneAndUpdate').resolves({ _id: CONVERSATION_ID } as any)

    const res = createMockResponse()
    const next = sinon.stub()
    await setConversationProject(createMockRequest({ projectId: null }), res, next)

    expect(next.called).to.equal(false)
    expect(res.status.calledWith(200)).to.equal(true)
    expect(res.json.firstCall.args[0]).to.deep.equal({
      conversationId: CONVERSATION_ID,
      projectId: null,
      projectVisibility: null,
    })
  })

  it('returns the linked projectId and visibility', async () => {
    const projectObjectId = new mongoose.Types.ObjectId(PROJECT_ID)
    sinon.stub(ChatSession, 'findOne').resolves({ _id: CONVERSATION_ID } as any)
    sinon.stub(ProjectService, 'assertAccess').resolves({ role: 'owner', project: { _id: projectObjectId } } as any)
    sinon.stub(ProjectService, 'touchActivity').resolves()
    sinon.stub(ChatSession, 'findOneAndUpdate').resolves({
      _id: CONVERSATION_ID,
      projectId: projectObjectId,
      projectVisibility: 'private',
    } as any)

    const res = createMockResponse()
    await setConversationProject(createMockRequest({ projectId: PROJECT_ID }), res, sinon.stub())

    const body = res.json.firstCall.args[0]
    expect(body.projectId.toString()).to.equal(PROJECT_ID)
    expect(body.projectVisibility).to.equal('private')
  })
})
