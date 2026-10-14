import 'reflect-metadata'
import { expect } from 'chai'
import sinon from 'sinon'
import * as connectorUtils from '../../../../src/modules/tokens_manager/utils/connector.utils'
import { deleteConversationArtifacts } from '../../../../src/modules/artifacts/services/conversation-artifacts.service'
import { InternalServerError, ServiceUnavailableError } from '../../../../src/libs/errors/http.errors'

describe('deleteConversationArtifacts', () => {
  afterEach(() => sinon.restore())

  it("asks the connectors service to delete the chat's artifacts with the caller's own token", async () => {
    const exec = sinon.stub(connectorUtils, 'executeConnectorCommand').resolves({
      statusCode: 200,
      data: { deleted: 3, kept: 0, purgedFiles: 1 },
    })

    await deleteConversationArtifacts('http://connector.local', '507f1f77bcf86cd799439011', {
      authorization: 'Bearer user-token',
      cookie: 'session=1',
    })

    const [uri, method, headers] = exec.firstCall.args
    expect(uri).to.equal('http://connector.local/api/v1/artifacts/conversations/507f1f77bcf86cd799439011')
    expect(method).to.equal('DELETE')
    expect(headers).to.deep.equal({ authorization: 'Bearer user-token' })
  })

  it('fails when storage could not delete the files, so the chat is not deleted', async () => {
    sinon.stub(connectorUtils, 'executeConnectorCommand').resolves({ statusCode: 503, data: { detail: 'retry' } })

    let error: unknown
    try {
      await deleteConversationArtifacts('http://connector.local', 'c1', { authorization: 'Bearer t' })
    } catch (e) {
      error = e
    }
    expect(error).to.be.instanceOf(ServiceUnavailableError)
  })

  it('fails when the connectors service errors or cannot be reached', async () => {
    const exec = sinon.stub(connectorUtils, 'executeConnectorCommand')
    exec.onFirstCall().resolves({ statusCode: 500, data: { detail: 'boom' } })
    exec.onSecondCall().rejects(new TypeError('fetch failed'))

    for (let i = 0; i < 2; i++) {
      let error: unknown
      try {
        await deleteConversationArtifacts('http://connector.local', 'c1', { authorization: 'Bearer t' })
      } catch (e) {
        error = e
      }
      expect(error, `call ${i}`).to.be.instanceOf(i === 0 ? InternalServerError : ServiceUnavailableError)
    }
  })
})
