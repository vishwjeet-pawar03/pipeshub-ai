import { describe, it, expect, beforeEach, vi } from 'vitest';
import { KnowledgeHubApi } from '@/app/(main)/knowledge-base/api';
import { UsersApi } from '@/app/(main)/workspace/users/api';
import type { User } from '@/app/(main)/workspace/users/types';
import { ConnectorsApi } from '../../api';
import type { Connector } from '../../types';
import { RESTRICTED_RECORD_READER, RESTRICTED_RECORD_TITLE } from '../demo-data';
import { checkRestrictedQuestionAccess } from '../restricted-question';

vi.mock('@/app/(main)/knowledge-base/api', () => ({
  KnowledgeHubApi: { searchAllRecords: vi.fn() },
}));
vi.mock('@/app/(main)/workspace/users/api', () => ({
  UsersApi: { listUsers: vi.fn() },
}));
vi.mock('../../api', () => ({
  ConnectorsApi: { getActiveConnectors: vi.fn() },
}));

const searchAllRecords = vi.mocked(KnowledgeHubApi.searchAllRecords);
const listUsers = vi.mocked(UsersApi.listUsers);
const getActiveConnectors = vi.mocked(ConnectorsApi.getActiveConnectors);

function records(...names: string[]) {
  return { items: names.map((name, i) => ({ id: `r${i}`, name })) } as never;
}

function users(...emails: string[]) {
  return { users: emails.map((email, i) => ({ userId: `u${i}`, email }) as User), totalCount: emails.length };
}

/** What the viewer's listing returns: for the title search, and for "any demo record". */
function listing(titleMatches: string[], anyRecord: string[]) {
  searchAllRecords.mockImplementation(async (params) =>
    params.q ? records(...titleMatches) : records(...anyRecord),
  );
}

function demoStatus(status: string) {
  getActiveConnectors.mockResolvedValue({
    success: true,
    connectors: [{ _key: 'demo-1', type: 'Demo', status } as Connector],
  } as never);
}

beforeEach(() => {
  vi.clearAllMocks();
  demoStatus('IDLE');
});

describe('checkRestrictedQuestionAccess', () => {
  it('asks for the restricted record by title, within the demo connectors, as the viewer', async () => {
    listing([RESTRICTED_RECORD_TITLE], ['Export runbook']);

    const access = await checkRestrictedQuestionAccess(['demo-1', 'demo-2']);

    expect(access).toEqual({ canSee: true, readerEmail: null });
    expect(searchAllRecords).toHaveBeenCalledWith(
      expect.objectContaining({ q: RESTRICTED_RECORD_TITLE, nodeTypes: 'record', connectorIds: 'demo-1,demo-2' }),
    );
    expect(listUsers).not.toHaveBeenCalled();
  });

  it('names the committee member to sign in as when that account exists', async () => {
    listing([], ['Export runbook']);
    listUsers.mockResolvedValue(users('someone@company.com', RESTRICTED_RECORD_READER));

    expect(await checkRestrictedQuestionAccess(['demo-1'])).toEqual({
      canSee: false,
      readerEmail: RESTRICTED_RECORD_READER,
    });
  });

  it('names nobody when the sample accounts were not created', async () => {
    listing([], ['Export runbook']);
    // The search is a substring match; only the exact address counts.
    listUsers.mockResolvedValue(users(`x${RESTRICTED_RECORD_READER}`));

    expect(await checkRestrictedQuestionAccess(['demo-1'])).toEqual({ canSee: false, readerEmail: null });
  });

  it('claims nothing before the demo has synced, when nobody can see any of it yet', async () => {
    listing([], []);

    expect(await checkRestrictedQuestionAccess(['demo-1'])).toBeNull();
    expect(listUsers).not.toHaveBeenCalled();
  });

  it('claims nothing while the demo is syncing, when the record may simply not be in yet', async () => {
    listing([], ['Export runbook']);
    demoStatus('FULL_SYNCING');

    expect(await checkRestrictedQuestionAccess(['demo-1'])).toBeNull();
  });

  it('does not count a record whose title only resembles the restricted one', async () => {
    listing([`${RESTRICTED_RECORD_TITLE} (draft notes)`], ['Export runbook']);
    listUsers.mockResolvedValue(users());

    expect((await checkRestrictedQuestionAccess(['demo-1']))?.canSee).toBe(false);
  });

  it('still reports the restriction when the account lookup fails', async () => {
    listing([], ['Export runbook']);
    listUsers.mockRejectedValue(new Error('403'));

    expect(await checkRestrictedQuestionAccess(['demo-1'])).toEqual({ canSee: false, readerEmail: null });
  });

  it('claims nothing when a lookup fails or there is no demo', async () => {
    searchAllRecords.mockRejectedValue(new Error('offline'));
    expect(await checkRestrictedQuestionAccess(['demo-1'])).toBeNull();

    listing([], ['Export runbook']);
    getActiveConnectors.mockRejectedValue(new Error('offline'));
    expect(await checkRestrictedQuestionAccess(['demo-1'])).toBeNull();

    searchAllRecords.mockClear();
    expect(await checkRestrictedQuestionAccess([])).toBeNull();
    expect(searchAllRecords).not.toHaveBeenCalled();
  });
});
