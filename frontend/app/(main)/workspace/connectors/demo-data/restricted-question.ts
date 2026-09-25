'use client';

import { KnowledgeHubApi } from '@/app/(main)/knowledge-base/api';
import { UsersApi } from '@/app/(main)/workspace/users/api';
import { ConnectorsApi } from '../api';
import { CONNECTOR_INSTANCE_STATUS } from '../constants';
import { RESTRICTED_RECORD_READER, RESTRICTED_RECORD_TITLE } from './demo-data';

export interface RestrictedQuestionAccess {
  /** Whether this person can open the record the restricted question is about. */
  canSee: boolean;
  /** A sample account that can, if it exists in this org, to sign in as instead. */
  readerEmail: string | null;
}

const SYNCING_STATUSES: ReadonlySet<string> = new Set([
  CONNECTOR_INSTANCE_STATUS.SYNCING,
  CONNECTOR_INSTANCE_STATUS.FULL_SYNCING,
]);

/**
 * Whether the demo's data is there to judge by: not mid-sync, and at least one
 * of its records visible. Before that, a missing record says nothing about access.
 */
async function demoDataSettled(demoConnectorIds: string[]): Promise<boolean> {
  const [{ connectors }, anyRecord] = await Promise.all([
    ConnectorsApi.getActiveConnectors('team'),
    KnowledgeHubApi.searchAllRecords({
      nodeTypes: 'record',
      connectorIds: demoConnectorIds.join(','),
      flattened: true,
      limit: 1,
      include: undefined,
    }),
  ]);
  const syncing = (connectors ?? []).some(
    (c) => !!c._key && demoConnectorIds.includes(c._key) && SYNCING_STATUSES.has(c.status ?? ''),
  );
  return !syncing && (anyRecord.items ?? []).length > 0;
}

/**
 * The demo's pricing question only answers for Acme's pricing committee.
 * Records are listed with the viewer's own permissions, so once the demo data
 * is in, a missing record means this person cannot see it. `null` when that
 * cannot be told yet, in which case the question is offered like any other.
 */
export async function checkRestrictedQuestionAccess(
  demoConnectorIds: string[],
): Promise<RestrictedQuestionAccess | null> {
  if (demoConnectorIds.length === 0) return null;
  try {
    const res = await KnowledgeHubApi.searchAllRecords({
      q: RESTRICTED_RECORD_TITLE,
      nodeTypes: 'record',
      connectorIds: demoConnectorIds.join(','),
      flattened: true,
      limit: 5,
      include: undefined,
    });
    if ((res.items ?? []).some((item) => item.name === RESTRICTED_RECORD_TITLE)) {
      return { canSee: true, readerEmail: null };
    }
    if (!(await demoDataSettled(demoConnectorIds))) return null;
  } catch {
    return null;
  }

  // Sample accounts are optional at install, so only name one that exists.
  try {
    const { users } = await UsersApi.listUsers({ search: RESTRICTED_RECORD_READER, limit: 5 });
    const exists = users.some((u) => (u.email ?? '').trim().toLowerCase() === RESTRICTED_RECORD_READER);
    return { canSee: false, readerEmail: exists ? RESTRICTED_RECORD_READER : null };
  } catch {
    return { canSee: false, readerEmail: null };
  }
}
