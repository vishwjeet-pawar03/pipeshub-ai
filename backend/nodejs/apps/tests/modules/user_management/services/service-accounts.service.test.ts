import 'reflect-metadata';
import { expect } from 'chai';
import sinon from 'sinon';
import mongoose from 'mongoose';
import { ServiceAccountsService } from '../../../../src/modules/user_management/services/service-accounts.service';
import { Users } from '../../../../src/modules/user_management/schema/users.schema';

function makeService() {
  const logger = {
    info: sinon.stub(),
    debug: sinon.stub(),
    warn: sinon.stub(),
    error: sinon.stub(),
  };
  const events = {
    start: sinon.stub().resolves(),
    stop: sinon.stub().resolves(),
    publishEvent: sinon.stub().resolves(),
  };
  return {
    service: new ServiceAccountsService(logger as any, events as any),
    events,
  };
}

describe('ServiceAccountsService', () => {
  const orgId = new mongoose.Types.ObjectId().toString();
  const id = new mongoose.Types.ObjectId().toString();

  afterEach(() => sinon.restore());

  describe('create', () => {
    it('rejects a name that would not make a valid address, before touching the database', async () => {
      const { service } = makeService();
      const findOne = sinon.stub(Users, 'findOne');

      // Note that mixed case is not in this list: the slug is lowercased
      // before it is checked, so "Nightly" is accepted and stored as
      // "nightly" rather than rejected.
      for (const bad of ['-bad', 'bad-', 'double--hyphen', 'has space', 'ab', 'a'.repeat(49)]) {
        try {
          await service.create(orgId, { slug: bad, fullName: 'X' });
          expect.fail(`expected "${bad}" to be rejected`);
        } catch (error) {
          expect((error as Error).message).to.contain('lowercase letters');
        }
      }
      expect(findOne.called).to.equal(false);
    });

    it('refuses a name already taken in the organisation', async () => {
      const { service } = makeService();
      sinon.stub(Users, 'findOne').returns({
        select: sinon.stub().returns({
          lean: sinon.stub().returns({ exec: sinon.stub().resolves({ _id: id }) }),
        }),
      } as any);

      try {
        await service.create(orgId, { slug: 'nightly-sync', fullName: 'X' });
        expect.fail('expected a duplicate name to be refused');
      } catch (error) {
        expect((error as Error).message).to.contain('already exists');
      }
    });
  });

  describe('lookups are confined to service accounts', () => {
    it('will not return a human user through a service-account route', async () => {
      const { service } = makeService();
      // A human's id resolves to nothing here because `kind` is part of the
      // query, not a check applied to whatever came back.
      const findOne = sinon
        .stub(Users, 'findOne')
        .returns({ exec: sinon.stub().resolves(null) } as any);

      try {
        await service.get(orgId, id);
        expect.fail('expected a non-service account to be not found');
      } catch (error) {
        expect((error as Error).message).to.equal('Service account not found');
      }
      expect(findOne.firstCall.args[0]).to.include({
        kind: 'service',
        orgId,
        isDeleted: false,
      });
    });

    it('treats a malformed id as not found rather than throwing a cast error', async () => {
      const { service } = makeService();
      const findOne = sinon.stub(Users, 'findOne');

      try {
        await service.get(orgId, 'not-an-object-id');
        expect.fail('expected a malformed id to be not found');
      } catch (error) {
        expect((error as Error).message).to.equal('Service account not found');
      }
      expect(findOne.called).to.equal(false);
    });
  });

  describe('update', () => {
    it('disables an account and leaves the record in place', async () => {
      const { service } = makeService();
      const account: any = {
        _id: id,
        email: `svc-x-${orgId}@service.pipeshub.internal`,
        fullName: 'Nightly sync',
        isDisabled: false,
        isDeleted: false,
        save: sinon.stub().resolvesThis(),
      };
      sinon
        .stub(Users, 'findOne')
        .returns({ exec: sinon.stub().resolves(account) } as any);

      const view = await service.update(orgId, id, { isDisabled: true });

      expect(view.isDisabled).to.equal(true);
      expect(account.isDeleted).to.equal(false);
      expect(account.save.calledOnce).to.equal(true);
    });

    it('tells the permission graph about a rename, but not about a disable', async () => {
      const account: any = {
        _id: id,
        email: `svc-x-${orgId}@service.pipeshub.internal`,
        fullName: 'Old name',
        isDisabled: false,
        save: sinon.stub().resolvesThis(),
      };
      sinon
        .stub(Users, 'findOne')
        .returns({ exec: sinon.stub().resolves(account) } as any);

      const disabling = makeService();
      await disabling.service.update(orgId, id, { isDisabled: true });
      expect(disabling.events.publishEvent.called).to.equal(false);

      const renaming = makeService();
      await renaming.service.update(orgId, id, { fullName: 'New name' });
      expect(renaming.events.publishEvent.calledOnce).to.equal(true);
      expect(renaming.events.publishEvent.firstCall.args[0].eventType).to.equal(
        'userUpdated',
      );
    });
  });
});
