import 'reflect-metadata';
import { expect } from 'chai';
import { UserAccountController } from '../../../../src/modules/auth/controller/userAccount.controller';

// A subclass reuses these helpers instead of copying the flows around them.
class ExtendedController extends UserAccountController {
  helpers() {
    return [
      this.claimResetLink,
      this.releaseResetLink,
      this.refuseIfSessionEnded,
      this.activeAccountForRefresh,
      this.publishEmailChanged,
    ];
  }
}

describe('UserAccountController helpers available to subclasses', () => {
  it('exposes the reset-link, session and email-change helpers', () => {
    const controller = Object.create(ExtendedController.prototype) as ExtendedController;

    expect(controller.helpers().every((fn) => typeof fn === 'function')).to.be.true;
  });
});
