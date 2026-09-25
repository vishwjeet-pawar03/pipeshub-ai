import { KnowledgeHubApi } from '@/app/(main)/knowledge-base/api';
import { UsersApi } from '@/app/(main)/workspace/users/api';
import { RESTRICTED_RECORD_READER, RESTRICTED_RECORD_TITLE } from './demo-data';

export interface RestrictedQuestionAccess {
  /** Whether this person can open the record the restricted question is about. */
  canSee: boolean;
  /** A sample account that can, if it exists in this org, to sign in as instead. */
  readerEmail: string | null;
}

/**
 * The demo's pricing question only answers for Acme's pricing committee.
 * Records are listed with the viewer's own permissions, so a missing record
 * means this person cannot see it. `null` when that cannot be told, in which
 * case the question is offered like any other.
 */
export async function checkRestrictedQuestionAccess(
  demoConnectorIds: string[],
): Promise<RestrictedQuestionAccess | null> {
  if (demoConnectorIds.length === 0) return null;
  let canSee: boolean;
  try {
    const res = await KnowledgeHubApi.searchAllRecords({
      q: RESTRICTED_RECORD_TITLE,
      nodeTypes: 'record',
      connectorIds: demoConnectorIds.join(','),
      flattened: true,
      limit: 5,
      include: undefined,
    });
    canSee = (res.items ?? []).some((item) => item.name === RESTRICTED_RECORD_TITLE);
  } catch {
    return null;
  }
  if (canSee) return { canSee, readerEmail: null };

  // Sample accounts are optional at install, so only name one that exists.
  try {
    const { users } = await UsersApi.listUsers({ search: RESTRICTED_RECORD_READER, limit: 5 });
    const exists = users.some((u) => (u.email ?? '').trim().toLowerCase() === RESTRICTED_RECORD_READER);
    return { canSee, readerEmail: exists ? RESTRICTED_RECORD_READER : null };
  } catch {
    return { canSee, readerEmail: null };
  }
}
