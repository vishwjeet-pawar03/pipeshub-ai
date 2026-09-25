import { useKnowledgeBaseStore } from '../store';
import { KnowledgeHubApi } from '../api';
import { SIDEBAR_PAGINATION_PAGE_SIZE } from '../constants';
import { categorizeNodes } from './tree-builder';
import { isKbCollectionsHubApp } from './all-records-transformer';
import type { KnowledgeHubApiResponse, KnowledgeHubNode } from '../types';

/** Bounds the walk; past it, the sidebar's own "load more" carries on. */
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

  // Root apps of every kind share one list sorted by recent update, so
  // collections can sit on any page behind connectors. Read every page before
  // touching the store: a page that fails part-way must not leave `appNodes`
  // and the tree describing different lists.
  const appItems: KnowledgeHubNode[] = [];
  let pagination: KnowledgeHubApiResponse['pagination'] | undefined;
  let page = 0;
  do {
    page += 1;
    const response = await fetchRootPage(page);
    appItems.push(...response.items.filter((n) => n.nodeType === 'app'));
    pagination = response.pagination;
  } while (pagination?.hasNext && page < MAX_ROOT_PAGES_FOR_COLLECTIONS);

  const kbApps = appItems.filter((n) => isKbCollectionsHubApp(n));
  const connectorApps = appItems.filter((n) => !isKbCollectionsHubApp(n));
  setAppNodes([...kbApps, ...connectorApps]);
  setAppRootListPagination(
    pagination
      ? {
          hasNext: pagination.hasNext,
          nextPage: pagination.hasNext ? pagination.page + 1 : pagination.page,
        }
      : null
  );

  setNodes(kbApps);
  setCategorizedNodes(categorizeNodes(kbApps, null));
  reMergeCachedChildrenIntoTree();
  afterRefresh?.();
}
