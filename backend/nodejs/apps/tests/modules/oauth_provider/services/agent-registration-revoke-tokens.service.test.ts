import 'reflect-metadata';
import { expect } from 'chai';
import sinon from 'sinon';
import { AgentRegistrationService } from '../../../../src/modules/oauth_provider/services/agent-registration.service';
import { OAuthTokenService } from '../../../../src/modules/oauth_provider/services/oauth_token.service';
import {
  AgentRegistrationStatus,
  setOAuthTokenRepository,
  type AgentRegistrationDto,
  type NewAgentRegistration,
} from '../../../../src/libs/repositories/oauth-token';
import { setUserRepository } from '../../../../src/libs/repositories/user';
import { setOrgRepository } from '../../../../src/libs/repositories/org';
import { createMockLogger } from '../../../helpers/mock-logger';

const ISSUER = 'https://pipeshub.test';
const SECRET = 'test-secret-key-for-signing-tokens-at-least-32-bytes';
const USER = {
  userId: 'a'.repeat(24),
  orgId: 'b'.repeat(24),
  email: 'Ada@Example.com',
};

/** Just enough of the repository, in memory, to drive a claim and then a revoke. */
class FakeRepo {
  rows = new Map<string, AgentRegistrationDto>();
  private n = 0;
  async createAgentRegistration(i: NewAgentRegistration) {
    const _id = (++this.n).toString(16).padStart(24, '0');
    const row = {
      ...i,
      _id,
      status: AgentRegistrationStatus.PENDING,
      failedAttempts: 0,
      createdAt: new Date(),
      updatedAt: new Date(),
    } as AgentRegistrationDto;
    this.rows.set(_id, row);
    return { ...row };
  }
  private find(pick: (r: AgentRegistrationDto) => boolean) {
    const r = [...this.rows.values()].find(pick);
    return r ? { ...r } : null;
  }
  async findAgentRegistrationById(id: string) {
    return this.find((r) => r._id === id);
  }
  async findAgentRegistrationByClaimHash(h: string) {
    return this.find((r) => r.claimTokenHash === h);
  }
  async findAgentRegistrationByAttemptHash(h: string) {
    return this.find((r) => r.attemptTokenHash === h);
  }
  async startAgentClaimAttempt(id: string, a: any) {
    const r = this.rows.get(id)!;
    if (r.status !== AgentRegistrationStatus.PENDING) return false;
    Object.assign(r, a, { failedAttempts: 0 });
    return true;
  }
  async recordAgentFailedAttempt(id: string, max: number) {
    const r = this.rows.get(id)!;
    if (r.status !== AgentRegistrationStatus.PENDING) return;
    r.failedAttempts += 1;
    if (r.failedAttempts >= max) r.status = AgentRegistrationStatus.EXPIRED;
  }
  async recordAgentPoll(id: string, at: Date) {
    this.rows.get(id)!.lastPolledAt = at;
  }
  async decideAgentRegistration(id: string, d: any) {
    const r = this.rows.get(id)!;
    if (r.status !== AgentRegistrationStatus.PENDING) return false;
    r.status = d.status;
    if (d.userId) {
      r.claimedUserId = d.userId;
      r.claimedOrgId = d.orgId;
      r.approvedScopes = d.scopes;
    }
    return true;
  }
  async consumeAgentRegistration(id: string, purgeAt: Date) {
    const r = this.rows.get(id)!;
    if (r.status !== AgentRegistrationStatus.CLAIMED) return null;
    r.status = AgentRegistrationStatus.CONSUMED;
    r.purgeAt = purgeAt;
    return { ...r };
  }
  async revokeAgentRegistration(id: string, userId?: string) {
    const r = this.rows.get(id);
    if (
      !r ||
      (userId !== undefined && r.claimedUserId !== userId) ||
      ![
        AgentRegistrationStatus.CLAIMED,
        AgentRegistrationStatus.CONSUMED,
      ].includes(r.status)
    ) {
      return false;
    }
    r.status = AgentRegistrationStatus.REVOKED;
    return true;
  }
  /** What `revokeAllTokensForUser` (the "disconnect this app" path) calls to make a client+user's issued tokens stop working. */
  revokedForClientUser: { clientId: string; userId: string }[] = [];
  async revokeAllForClientUser(clientId: string, userId: string) {
    this.revokedForClientUser.push({ clientId, userId });
  }
}

describe('AgentRegistrationService revocation reaches issued tokens', () => {
  let repo: FakeRepo;
  let service: AgentRegistrationService;
  let tokens: OAuthTokenService;

  beforeEach(() => {
    delete process.env.PIPESHUB_ENABLE_DEVICE_GRANT;
    const app = { clientId: 'pipeshub-agent', allowedScopes: ['org:read', 'offline_access'] };
    repo = new FakeRepo();
    setOAuthTokenRepository(repo as any);
    setUserRepository({ getLiveFields: async () => ({ fullName: 'Ada' }) } as any);
    setOrgRepository({
      getAccountType: async () => ({ accountType: 'business' }),
    } as any);
    tokens = new OAuthTokenService(
      createMockLogger() as any,
      { algorithm: 'HS256', secret: SECRET } as any,
      ISSUER,
    );
    sinon.stub(tokens, 'generateTokens').callsFake(
      async (_a, _u, _o, scopes) =>
        ({
          accessToken: 'at',
          jti: 'j',
          accessTokenId: 'i',
          tokenType: 'Bearer',
          expiresIn: 3600,
          scope: scopes.join(' '),
        }) as any,
    );
    service = new AgentRegistrationService(
      createMockLogger() as any,
      { getAppByClientId: sinon.stub().callsFake(async () => app) } as any,
      tokens,
      {
        resolveGrantedScopes: (r: string[]) => ({ granted: r, notGranted: [] }),
        getScopeDefinitions: (s: string[]) => s.map((name) => ({ name })),
      } as any,
      { getOrCreate: sinon.stub().resolves('pipeshub-agent') } as any,
      { frontendUrl: 'https://app.test', oauthIssuer: ISSUER } as any,
    );
  });

  afterEach(() => {
    sinon.restore();
    setOAuthTokenRepository();
    setUserRepository();
    setOrgRepository();
    delete process.env.PIPESHUB_ENABLE_DEVICE_GRANT;
  });

  const attemptToken = (uri: string) =>
    new URL(uri).searchParams.get('claim_attempt_token')!;
  const claimed = async () => {
    const r = await service.register('service_auth', 'ada@example.com');
    await service.completeClaim(
      attemptToken(r.claim.verification_uri),
      r.claim.user_code,
      'approve',
      USER,
    );
    return r;
  };

  // bug: DELETE /api/v1/agent/registrations/{id} (AgentRegistrationService.revokeForUser,
  // backend/nodejs/apps/src/modules/oauth_provider/services/agent-registration.service.ts:417-428)
  // only flips the registration row's status. Unlike revokeAgentToken (the reverse direction,
  // oauth_token.service.ts:724-752) and revokeAllTokensForUser (the "disconnect this app" path,
  // oauth_token.service.ts:802-809), it never revokes the access tokens already issued under the
  // registration, so a claimed agent keeps working with its existing token until it expires.
  it('revoking a registration also revokes the access tokens already issued under it', async () => {
    const r = await claimed();
    await service.claimGrant(r.claim_token);
    await service.revokeForUser(r.registration_id, USER);
    expect([...repo.rows.values()][0].status).to.equal(
      AgentRegistrationStatus.REVOKED,
    );
    expect(repo.revokedForClientUser).to.have.length(1);
  });
});
