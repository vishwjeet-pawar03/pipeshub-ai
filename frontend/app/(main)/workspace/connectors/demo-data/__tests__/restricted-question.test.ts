import { describe, it, expect, beforeEach, vi } from 'vitest';
import { KnowledgeHubApi } from '@/app/(main)/knowledge-base/api';
import { UsersApi } from '@/app/(main)/workspace/users/api';
import type { User } from '@/app/(main)/workspace/users/types';
import { RESTRICTED_RECORD_READER, RESTRICTED_RECORD_TITLE } from '../demo-data';
import { checkRestrictedQuestionAccess } from '../restricted-question';

vi.mock('@/app/(main)/knowledge-base/api', () => ({
  KnowledgeHubApi: { searchAllRecords: vi.fn() },
}));
vi.mock('@/app/(main)/workspace/users/api', () => ({
  UsersApi: { listUsers: vi.fn() },
}));

const searchAllRecords = vi.mocked(KnowledgeHubApi.searchAllRecords);
const listUsers = vi.mocked(UsersApi.listUsers);

function records(...names: string[]) {
  return { items: names.map((name, i) => ({ id: `r${i}`, name })) } as never;
}

function users(...emails: string[]) {
  return { users: emails.map((email, i) => ({ userId: `u${i}`, email }) as User), totalCount: emails.length };
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe('checkRestrictedQuestionAccess', () => {
  it('asks for the restricted record by title, within the demo connectors, as the viewer', async () => {
    searchAllRecords.mockResolvedValue(records(RESTRICTED_RECORD_TITLE));

    const access = await checkRestrictedQuestionAccess(['demo-1', 'demo-2']);

    expect(access).toEqual({ canSee: true, readerEmail: null });
    expect(searchAllRecords).toHaveBeenCalledWith(
      expect.objectContaining({ q: RESTRICTED_RECORD_TITLE, nodeTypes: 'record', connectorIds: 'demo-1,demo-2' }),
    );
    expect(listUsers).not.toHaveBeenCalled();
  });

  it('names the committee member to sign in as when that account exists', async () => {
    searchAllRecords.mockResolvedValue(records());
    listUsers.mockResolvedValue(users('someone@company.com', RESTRICTED_RECORD_READER));

    expect(await checkRestrictedQuestionAccess(['demo-1'])).toEqual({
      canSee: false,
      readerEmail: RESTRICTED_RECORD_READER,
    });
  });

  it('names nobody when the sample accounts were not created', async () => {
    searchAllRecords.mockResolvedValue(records());
    // The search is a substring match; only the exact address counts.
    listUsers.mockResolvedValue(users(`x${RESTRICTED_RECORD_READER}`));

    expect(await checkRestrictedQuestionAccess(['demo-1'])).toEqual({ canSee: false, readerEmail: null });
  });

  it('does not count a record whose title only resembles the restricted one', async () => {
    searchAllRecords.mockResolvedValue(records(`${RESTRICTED_RECORD_TITLE} (draft notes)`));
    listUsers.mockResolvedValue(users());

    expect((await checkRestrictedQuestionAccess(['demo-1']))?.canSee).toBe(false);
  });

  it('still reports the restriction when the account lookup fails', async () => {
    searchAllRecords.mockResolvedValue(records());
    listUsers.mockRejectedValue(new Error('403'));

    expect(await checkRestrictedQuestionAccess(['demo-1'])).toEqual({ canSee: false, readerEmail: null });
  });

  it('claims nothing when the record lookup fails or there is no demo', async () => {
    searchAllRecords.mockRejectedValue(new Error('offline'));
    expect(await checkRestrictedQuestionAccess(['demo-1'])).toBeNull();

    expect(await checkRestrictedQuestionAccess([])).toBeNull();
    expect(searchAllRecords).toHaveBeenCalledTimes(1);
  });
});
