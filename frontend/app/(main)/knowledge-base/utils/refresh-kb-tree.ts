import { useKnowledgeBaseStore } from '../store';
import { KnowledgeHubApi } from '../api';
import { SIDEBAR_PAGINATION_PAGE_SIZE } from '../constants';
import { categorizeNodes } from './tree-builder';
import { isKbCollectionsHubApp } from './all-records-transformer';

/** Bounds the walk if the API keeps reporting `hasNext` (defensive). */
const MAX_ROOT_PAGES_FOR_COLLECTIONS = 50;

/**
 * Refreshes the Collections sidebar tree by re-fetching root KB apps.
 *
 * Each KB is now a standalone root-level app — the sidebar tree is built
 * directly from KB apps (categorized into Shared/Private by sharingStatus).
 *
 * Re-merges any previously-expanded folder's cached children back into the
 * rebuilt tree automatically, so expanded state survives a refresh triggered
 * from elsewhere on the page (rename/delete/create-folder). `afterRefresh` is
 * for additional caller-specific work after that merge, not for opting into it.
 */
export async function refreshKbTree(afterRefresh?: () => void): Promise<void> {
  const {
    setNodes,
    setCategorizedNodes,
    setAppNodes,
    setAppRootListPagination,
    reMergeCachedChildrenIntoTree,
  } = useKnowledgeBaseStore.getState();

  // Always re-fetch root app nodes from the API — this is a "refresh", so
  // stale in-memory data (e.g. a KB that was just renamed) must not be reused.
  const fetchRootPage = (page: number) =>
    KnowledgeHubApi.getNavigationNodes({
      page,
      limit: SIDEBAR_PAGINATION_PAGE_SIZE,
      include: 'counts',
      sortBy: 'updatedAt',
      sortOrder: 'desc',
    });
  const response = await fetchRootPage(1);
  const appItems = response.items.filter((n) => n.nodeType === 'app');
  const freshKbApps = appItems.filter((n) => isKbCollectionsHubApp(n));
  const connectorApps = appItems.filter((n) => !isKbCollectionsHubApp(n));
  setAppNodes([...freshKbApps, ...connectorApps]);
  const p = response.pagination;
  setAppRootListPagination(
    p
      ? {
          hasNext: p.hasNext,
          nextPage: p.hasNext ? p.page + 1 : p.page,
        }
      : null
  );

  // Root apps of every kind share one list sorted by recent update, so a full
  // page of connectors can push every collection onto a later page. Read those
  // pages rather than the cached list, which may still hold deleted or renamed
  // collections.
  let kbApps = freshKbApps;
  let pagination = p;
  let page = 1;
  while (kbApps.length === 0 && pagination?.hasNext && page < MAX_ROOT_PAGES_FOR_COLLECTIONS) {
    page += 1;
    const next = await fetchRootPage(page);
    kbApps = next.items.filter((n) => n.nodeType === 'app' && isKbCollectionsHubApp(n));
    pagination = next.pagination;
  }

  setNodes(kbApps);
  setCategorizedNodes(categorizeNodes(kbApps, null));
  reMergeCachedChildrenIntoTree();
  afterRefresh?.();
}
