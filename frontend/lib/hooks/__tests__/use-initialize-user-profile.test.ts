import { beforeEach, describe, expect, it, vi } from 'vitest';
import { act, renderHook } from '@testing-library/react';

const get = vi.fn();
vi.mock('@/lib/api', () => ({ apiClient: { get: (...args: unknown[]) => get(...args) } }));
vi.mock('@/lib/utils/jwt', () => ({ getUserIdFromToken: () => 'u1' }));
vi.mock('@/app/(main)/workspace/profile/api', () => ({
  ProfileApi: { getAvatar: () => Promise.resolve(null) },
}));

import { useInitializeUserProfile } from '../use-initialize-user-profile';
import { useUserStore } from '@/lib/store/user-store';

function answer(user: unknown, groups: unknown) {
  get.mockImplementation((url: string) => {
    const value = url.startsWith('/api/v1/userGroups') ? groups : user;
    return value instanceof Error ? Promise.reject(value) : Promise.resolve({ data: value });
  });
}

async function resolvedIsAdmin() {
  const { result } = renderHook(() => useInitializeUserProfile());
  await act(async () => {
    await result.current.initialize(true);
  });
  return useUserStore.getState().profile?.isAdmin;
}

beforeEach(() => {
  get.mockReset();
  useUserStore.getState().clearProfile();
});

describe('useInitializeUserProfile isAdmin', () => {
  it('reads the role from the user record', async () => {
    answer({ role: 'admin' }, [{ type: 'everyone' }]);
    expect(await resolvedIsAdmin()).toBe(true);
    answer({ role: 'member' }, [{ type: 'everyone' }]);
    expect(await resolvedIsAdmin()).toBe(false);
  });

  it('leaves the role unknown when the user record fails and no admin group says otherwise', async () => {
    answer(new Error('500'), [{ type: 'everyone' }]);
    expect(await resolvedIsAdmin()).toBeNull();
  });

  it('still trusts an admin group when the user record fails', async () => {
    answer(new Error('500'), [{ type: 'admin' }]);
    expect(await resolvedIsAdmin()).toBe(true);
  });

  it('treats a user record without a role and no admin group as a member', async () => {
    answer({ fullName: 'A' }, [{ type: 'everyone' }]);
    expect(await resolvedIsAdmin()).toBe(false);
  });
});
