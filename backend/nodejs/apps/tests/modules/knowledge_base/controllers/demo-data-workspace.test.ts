import 'reflect-metadata';
import { expect } from 'chai';
import sinon from 'sinon';
import { setDemoDataWorkspace } from '../../../../src/modules/knowledge_base/controllers/kb_controllers';
import { ConnectorServiceCommand } from '../../../../src/libs/commands/connector_service/connector.service.command';
import * as userAdmin from '../../../../src/modules/user_management/services/user-admin.service';
import * as demoAccounts from '../../../../src/modules/user_management/services/demo-accounts.service';
import { demoDataWorkspaceSchema } from '../../../../src/modules/knowledge_base/validators/validators';

const appConfig = { connectorBackend: 'http://localhost:8088' } as any;

function request(enabled: boolean): any {
  return { headers: { authorization: 'Bearer t' }, body: { enabled }, params: {}, query: {}, user: { userId: 'admin-1', orgId: 'org-1' } };
}

function response(): any {
  const res: any = { status: sinon.stub(), json: sinon.stub() };
  res.status.returns(res);
  return res;
}

describe('setDemoDataWorkspace', () => {
  afterEach(() => sinon.restore());

  it('turns the demo off for everyone and stops the sample accounts signing in', async () => {
    sinon.stub(userAdmin, 'isUserOrgAdmin').resolves(true);
    const execute = sinon.stub(ConnectorServiceCommand.prototype, 'execute').resolves({ statusCode: 200, data: { offForEveryone: true } } as any);
    const signIn = sinon.stub(demoAccounts, 'setSampleAccountsSignIn').resolves(2);
    const next = sinon.stub();

    await setDemoDataWorkspace(appConfig)(request(false), response(), next);

    expect(next.called).to.equal(false);
    expect(execute.calledOnce).to.equal(true);
    expect(signIn.calledOnceWith('org-1', 'admin-1', false)).to.equal(true);
  });

  it('refuses a member and changes nothing', async () => {
    sinon.stub(userAdmin, 'isUserOrgAdmin').resolves(false);
    const execute = sinon.stub(ConnectorServiceCommand.prototype, 'execute');
    const signIn = sinon.stub(demoAccounts, 'setSampleAccountsSignIn');
    const next = sinon.stub();

    await setDemoDataWorkspace(appConfig)(request(false), response(), next);

    expect(next.calledOnce).to.equal(true);
    expect(next.firstCall.args[0].statusCode ?? next.firstCall.args[0].status).to.equal(403);
    expect(execute.called).to.equal(false);
    expect(signIn.called).to.equal(false);
  });

  it('leaves the sample accounts alone when the setting could not be saved', async () => {
    sinon.stub(userAdmin, 'isUserOrgAdmin').resolves(true);
    sinon.stub(ConnectorServiceCommand.prototype, 'execute').resolves({ statusCode: 500, data: { detail: 'kv down' } } as any);
    const signIn = sinon.stub(demoAccounts, 'setSampleAccountsSignIn');
    const next = sinon.stub();

    await setDemoDataWorkspace(appConfig)(request(false), response(), next);

    expect(next.calledOnce).to.equal(true);
    expect(signIn.called).to.equal(false);
  });
});

describe('demoDataWorkspaceSchema', () => {
  it('takes one yes/no and nothing else', () => {
    expect(demoDataWorkspaceSchema.safeParse({ body: { enabled: false } }).success).to.equal(true);
    expect(demoDataWorkspaceSchema.safeParse({ body: {} }).success).to.equal(false);
    expect(demoDataWorkspaceSchema.safeParse({ body: { enabled: false, orgId: 'other' } }).success).to.equal(false);
  });
});
