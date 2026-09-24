import { ConnectorsApi } from '../api';
import { UsersApi } from '@/app/(main)/workspace/users/api';
import { DEMO_ACCOUNT_DOMAIN, isSampleAccountEmail } from './demo-data';

export interface SampleAccount {
  /** MongoDB id, which is what the delete endpoint takes. */
  userId: string;
  email: string;
  name?: string;
}

/**
 * Accounts on the reserved demo domain, such as Alice and Bob. Never the
 * person doing the removal, whatever address they signed up with.
 */
export async function findSampleAccounts(currentUserEmail?: string | null): Promise<SampleAccount[]> {
  const { users } = await UsersApi.listUsers({ search: DEMO_ACCOUNT_DOMAIN, limit: 100 });
  const me = (currentUserEmail ?? '').trim().toLowerCase();
  return users
    .filter((u) => !!u.userId && isSampleAccountEmail(u.email))
    .filter((u) => (u.email ?? '').trim().toLowerCase() !== me)
    .map((u) => ({ userId: u.userId, email: u.email as string, name: u.name }));
}

export interface RemoveDemoDataResult {
  /** Sample accounts that could not be deleted; the demo data itself is gone. */
  failedAccounts: SampleAccount[];
}

/**
 * Delete the Demo connector instances, then the chosen sample accounts.
 *
 * The connector goes first because it is what puts Acme Corp into answers. If
 * it fails, this throws and nothing else is touched; an account that fails
 * afterwards is reported rather than undoing a removal that already happened.
 */
export async function removeDemoData(
  connectorIds: string[],
  accounts: SampleAccount[],
): Promise<RemoveDemoDataResult> {
  for (const id of connectorIds) {
    await ConnectorsApi.deleteConnectorInstance(id);
  }
  const failedAccounts: SampleAccount[] = [];
  for (const account of accounts) {
    try {
      await UsersApi.deleteUser(account.userId);
    } catch {
      failedAccounts.push(account);
    }
  }
  return { failedAccounts };
}
