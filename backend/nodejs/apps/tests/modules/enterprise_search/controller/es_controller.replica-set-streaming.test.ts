import 'reflect-metadata'
import { expect } from 'chai'
import sinon from 'sinon'
import mongoose from 'mongoose'
import { CHAT_ERROR_MESSAGES } from '../../../../src/modules/enterprise_search/utils/chat-error-messages'
import { fakeReplicaSetSession, settle } from './chat-test-harness'
import { Controller, delta, finalAnswer, flows, startStream } from './streaming-flows'

const CONTROLLER = '../../../../src/modules/enterprise_search/controller/es_controller'

describe('es_controller on a replica set: streamed answers are saved after the request’s session ends', () => {
  // The controller reads REPLICA_SET_AVAILABLE when it loads, so load a copy with it set.
  let controller: Controller
  const previous = process.env.REPLICA_SET_AVAILABLE

  before(() => {
    process.env.REPLICA_SET_AVAILABLE = 'true'
    delete require.cache[require.resolve(CONTROLLER)]
    controller = require(CONTROLLER) as Controller
  })

  after(() => {
    if (previous === undefined) delete process.env.REPLICA_SET_AVAILABLE
    else process.env.REPLICA_SET_AVAILABLE = previous
    delete require.cache[require.resolve(CONTROLLER)]
  })

  afterEach(() => {
    sinon.restore()
  })

  for (const flow of flows) {
    it(`${flow.name}: the final answer is saved and the stream closes`, async () => {
      const session = fakeReplicaSetSession()
      const run = await startStream(flow, controller, () => {
        sinon.stub(mongoose, 'startSession').resolves(session as never)
      })

      run.ai.send('CUSTOM', { name: 'ask_user_question', value: { toolData: { question: 'Which team?' } } })
      run.ai.send('TEXT_MESSAGE_CONTENT', delta('Saved '))
      run.ai.send('RUN_FINISHED', finalAnswer('Saved answer.'))
      run.ai.finish()
      await Promise.race([run.res.ended, settle(40)])

      expect(run.res.writableEnded, 'stream closed').to.equal(true)
      expect(run.res.eventsOf('RUN_ERROR').map((e) => e.data.message)).to.deep.equal([])
      expect(run.res.eventsOf('RUN_FINISHED')).to.have.length(1)
      expect(run.answer()?.content).to.equal('Saved answer.')
      if (!flow.regenerate) {
        expect(run.messages().some((m) => m.messageType === 'tool_call'), 'question the agent asked is saved').to.equal(true)
      }
    })

    it(`${flow.name}: an answer that fails mid-stream is still recorded as failed`, async () => {
      const session = fakeReplicaSetSession()
      const run = await startStream(flow, controller, () => {
        sinon.stub(mongoose, 'startSession').resolves(session as never)
      })

      run.ai.send('TEXT_MESSAGE_CONTENT', delta('Half'))
      await settle()
      run.ai.breakConnection(Object.assign(new TypeError('terminated'), { cause: { code: 'UND_ERR_SOCKET' } }))
      await Promise.race([run.res.ended, settle(40)])

      expect(run.res.writableEnded).to.equal(true)
      expect(run.conversation().status).to.equal('Failed')
      expect(run.answer()?.content).to.equal(CHAT_ERROR_MESSAGES.interrupted)
    })
  }
})
