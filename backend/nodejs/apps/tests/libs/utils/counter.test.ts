import 'reflect-metadata';
import { expect } from 'chai';
import sinon from 'sinon';
import mongoose from 'mongoose';
import { Counter, generateUniqueSlug } from '../../../src/libs/utils/counter';

describe('counter', () => {
  afterEach(() => {
    sinon.restore();
  });

  describe('Counter model', () => {
    it('should be a valid mongoose model', () => {
      expect(Counter).to.exist;
      expect(Counter.modelName).to.equal('Counter');
    });

    it('should have the expected schema fields', () => {
      const paths = Counter.schema.paths;
      expect(paths).to.have.property('_id');
      expect(paths).to.have.property('name');
      expect(paths).to.have.property('seq');
    });

    it('should declare a unique index on name so racing creates cannot collide', () => {
      expect((Counter.schema.path('name') as any).options.unique).to.equal(
        true,
      );
    });
  });

  describe('generateUniqueSlug', () => {
    it('should return a slug combining the name and the counter value', async () => {
      // Mock Counter.findOneAndUpdate to return a counter value
      const findOneAndUpdateStub = sinon
        .stub(Counter, 'findOneAndUpdate')
        .resolves({ _id: 'test', name: 'test', seq: 1001 } as any);

      const slug = await generateUniqueSlug('my-project');
      expect(slug).to.be.a('string');
      expect(slug).to.include('my-project');
      expect(slug).to.include('1001');
      expect(findOneAndUpdateStub.calledOnce).to.be.true;
    });

    it('should call findOneAndUpdate with upsert and $inc', async () => {
      const findOneAndUpdateStub = sinon
        .stub(Counter, 'findOneAndUpdate')
        .resolves({ _id: 'test', name: 'test', seq: 1002 } as any);

      await generateUniqueSlug('widget');

      const args = findOneAndUpdateStub.firstCall.args;
      // First arg: filter
      expect(args[0]).to.deep.equal({ name: 'widget' });
      // Second arg: update
      expect(args[1]).to.deep.equal({ $inc: { seq: 1 } });
      // Third arg: options
      expect(args[2]).to.deep.include({ new: true, upsert: true });
    });

    it('should produce URL-friendly slugs', async () => {
      sinon
        .stub(Counter, 'findOneAndUpdate')
        .resolves({ _id: 'test', name: 'test', seq: 1003 } as any);

      const slug = await generateUniqueSlug('Hello World');
      // slug library should convert spaces to hyphens and lowercase
      expect(slug).to.match(/^[a-z0-9-]+$/);
      expect(slug).to.include('hello-world');
      expect(slug).to.include('1003');
    });

    it('should produce different slugs for different counter values', async () => {
      const stub = sinon.stub(Counter, 'findOneAndUpdate');
      stub
        .onFirstCall()
        .resolves({ _id: 'test', name: 'test', seq: 1000 } as any);
      stub
        .onSecondCall()
        .resolves({ _id: 'test', name: 'test', seq: 1001 } as any);

      const slug1 = await generateUniqueSlug('project');
      const slug2 = await generateUniqueSlug('project');
      expect(slug1).not.to.equal(slug2);
    });

    it('should propagate errors from findOneAndUpdate', async () => {
      sinon
        .stub(Counter, 'findOneAndUpdate')
        .rejects(new Error('MongoDB connection failed'));

      try {
        await generateUniqueSlug('fail');
        expect.fail('Should have thrown');
      } catch (err: any) {
        expect(err.message).to.equal('MongoDB connection failed');
      }
    });

    it('should retry on a duplicate-key error and take the next counter value', async () => {
      const dupErr: any = new Error('E11000 duplicate key');
      dupErr.code = 11000;
      const stub = sinon.stub(Counter, 'findOneAndUpdate');
      stub.onFirstCall().rejects(dupErr);
      stub.onSecondCall().resolves({ _id: 'Org', name: 'Org', seq: 2 } as any);

      const slug = await generateUniqueSlug('Org');

      expect(slug).to.include('2');
      expect(stub.calledTwice).to.be.true;
    });

    it('should give up after repeated duplicate-key errors rather than loop forever', async () => {
      const dupErr: any = new Error('E11000 duplicate key');
      dupErr.code = 11000;
      const stub = sinon.stub(Counter, 'findOneAndUpdate').rejects(dupErr);

      try {
        await generateUniqueSlug('Org');
        expect.fail('Should have thrown');
      } catch (err: any) {
        expect(err.code).to.equal(11000);
      }
      expect(stub.callCount).to.equal(5);
    });

    it('should not retry on a non-duplicate error', async () => {
      const stub = sinon
        .stub(Counter, 'findOneAndUpdate')
        .rejects(new Error('some other failure'));

      try {
        await generateUniqueSlug('Org');
        expect.fail('Should have thrown');
      } catch (err: any) {
        expect(err.message).to.equal('some other failure');
      }
      expect(stub.calledOnce).to.be.true;
    });
  });
});
