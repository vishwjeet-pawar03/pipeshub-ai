import 'reflect-metadata';
import { expect } from 'chai';
import sinon from 'sinon';
import mongoose from 'mongoose';
import { OutboxDispatcher } from '../../../../src/libs/services/outbox/outbox.dispatcher';
import { OutboxEvent } from '../../../../src/libs/services/outbox/outbox.schema';

function makeLogger() {
  return {
    info: sinon.stub(),
    debug: sinon.stub(),
    warn: sinon.stub(),
    error: sinon.stub(),
  };
}

function makeProducer(overrides: Record<string, unknown> = {}) {
  return {
    isConnected: sinon.stub().returns(true),
    connect: sinon.stub().resolves(),
    publish: sinon.stub().resolves(),
    ...overrides,
  };
}

function row(extra: Record<string, unknown> = {}) {
  return {
    _id: new mongoose.Types.ObjectId(),
    topic: 'entity-events',
    key: 'userAdded',
    value: '{"eventType":"userAdded"}',
    headers: { eventType: 'userAdded' },
    attempts: 0,
    ...extra,
  };
}

/**
 * Returns rows one at a time and then nothing, which is how the dispatcher
 * sees a queue: claim, deliver, claim again, stop when empty.
 */
function stubClaims(rows: unknown[]) {
  const claim = sinon.stub(OutboxEvent, 'findOneAndUpdate');
  rows.forEach((r, i) => claim.onCall(i).returns({ exec: async () => r } as any));
  claim.onCall(rows.length).returns({ exec: async () => null } as any);
  return claim;
}

describe('OutboxDispatcher', () => {
  let update: sinon.SinonStub;

  beforeEach(() => {
    update = sinon
      .stub(OutboxEvent, 'updateOne')
      .returns({ exec: async () => ({}) } as any);
  });

  afterEach(() => sinon.restore());

  it('publishes a queued event and marks it delivered', async () => {
    const event = row();
    stubClaims([event]);
    const producer = makeProducer();

    const delivered = await new OutboxDispatcher(
      producer as any,
      makeLogger() as any,
    ).drain();

    expect(delivered).to.equal(1);
    expect(producer.publish.calledOnce).to.equal(true);
    expect(producer.publish.firstCall.args[0]).to.equal('entity-events');
    expect(producer.publish.firstCall.args[1]).to.include({
      key: 'userAdded',
      value: '{"eventType":"userAdded"}',
    });
    expect(update.firstCall.args[1].$set.status).to.equal('published');
  });

  it('keeps a failed event for another try instead of losing it', async () => {
    // This is the whole point: the old code logged the failure and returned,
    // leaving the caller believing the event had been delivered.
    stubClaims([row()]);
    const producer = makeProducer({
      publish: sinon.stub().rejects(new Error('broker unreachable')),
    });

    const delivered = await new OutboxDispatcher(
      producer as any,
      makeLogger() as any,
    ).drain();

    expect(delivered).to.equal(0);
    const set = update.firstCall.args[1].$set;
    expect(set.status).to.equal('pending');
    expect(set.attempts).to.equal(1);
    expect(set.lastError).to.contain('broker unreachable');
    expect(set.nextAttemptAt).to.be.a('date');
  });

  it('backs off further on each successive failure', async () => {
    stubClaims([row({ attempts: 1 }), row({ attempts: 4 })]);
    const producer = makeProducer({
      publish: sinon.stub().rejects(new Error('still down')),
    });

    await new OutboxDispatcher(producer as any, makeLogger() as any).drain();

    const firstWait =
      update.firstCall.args[1].$set.nextAttemptAt.getTime() - Date.now();
    const laterWait =
      update.secondCall.args[1].$set.nextAttemptAt.getTime() - Date.now();
    expect(laterWait).to.be.greaterThan(firstWait);
  });

  it('parks an event that will never be accepted, and says so loudly', async () => {
    // Retrying a malformed message forever would block everything queued
    // behind it, so it is set aside — visibly, never silently.
    stubClaims([row({ attempts: 19 })]);
    const logger = makeLogger();
    const producer = makeProducer({
      publish: sinon.stub().rejects(new Error('message too large')),
    });

    await new OutboxDispatcher(producer as any, logger as any).drain();

    expect(update.firstCall.args[1].$set.status).to.equal('failed');
    expect(logger.error.called).to.equal(true);
    expect(logger.error.firstCall.args[0]).to.contain('parked');
  });

  it('warns quietly at first and escalates to an error', async () => {
    stubClaims([row({ attempts: 0 })]);
    const early = makeLogger();
    await new OutboxDispatcher(
      makeProducer({ publish: sinon.stub().rejects(new Error('blip')) }) as any,
      early as any,
    ).drain();
    expect(early.warn.called).to.equal(true);
    expect(early.error.called).to.equal(false);

    sinon.restore();
    update = sinon
      .stub(OutboxEvent, 'updateOne')
      .returns({ exec: async () => ({}) } as any);
    stubClaims([row({ attempts: 8 })]);
    const persistent = makeLogger();
    await new OutboxDispatcher(
      makeProducer({ publish: sinon.stub().rejects(new Error('still down')) }) as any,
      persistent as any,
    ).drain();
    expect(persistent.error.called).to.equal(true);
  });

  it('claims only events that are due, and reclaims abandoned ones', async () => {
    const claim = stubClaims([]);
    await new OutboxDispatcher(
      makeProducer() as any,
      makeLogger() as any,
    ).drain();

    const query = claim.firstCall.args[0] as { $or: Record<string, unknown>[] };
    // Due pending work.
    expect(query.$or[0]).to.have.property('status', 'pending');
    expect(query.$or[0]).to.have.property('nextAttemptAt');
    // And rows whose holder died mid-publish, which would otherwise stick.
    expect(query.$or[1]).to.have.property('status', 'publishing');
    expect(query.$or[1]).to.have.property('claimedAt');
    // Taken atomically, so two instances cannot send the same event.
    expect(claim.firstCall.args[1].$set.status).to.equal('publishing');
    expect(claim.firstCall.args[2]).to.deep.include({ sort: { createdAt: 1 } });
  });

  it('connects the producer if it is not connected yet', async () => {
    stubClaims([row()]);
    const producer = makeProducer({ isConnected: sinon.stub().returns(false) });

    await new OutboxDispatcher(producer as any, makeLogger() as any).drain();

    expect(producer.connect.calledOnce).to.equal(true);
    expect(producer.publish.calledOnce).to.equal(true);
  });

  it('survives the database being unreachable', async () => {
    // A failing pass must not stop later ticks from running.
    sinon.stub(OutboxEvent, 'findOneAndUpdate').throws(new Error('no mongo'));
    const logger = makeLogger();

    const delivered = await new OutboxDispatcher(
      makeProducer() as any,
      logger as any,
    ).drain();

    expect(delivered).to.equal(0);
    expect(logger.error.called).to.equal(true);
  });

  it('does not start a second pass while one is running', async () => {
    let release: (() => void) | null = null;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    stubClaims([row(), row()]);
    const producer = makeProducer({
      publish: sinon.stub().callsFake(async () => gate),
    });
    const dispatcher = new OutboxDispatcher(producer as any, makeLogger() as any);

    const first = dispatcher.drain();
    const second = await dispatcher.drain();
    expect(second).to.equal(0);

    release!();
    await first;
  });
});
