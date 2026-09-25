import { useKnowledgeBaseStore } from '../store';
import { KnowledgeHubApi } from '../api';
import { SIDEBAR_PAGINATION_PAGE_SIZE } from '../constants';
import { isKbCollectionsHubApp } from './all-records-transformer';
import type { KnowledgeHubApiResponse, KnowledgeHubNode } from '../types';

// Several loads write the root app list: the first-page load when the page
// opens, the full refresh after a create/rename/delete, and "load more". They
// can overlap, and whichever finished last used to win, so a slower, older
// response could hide a collection just created or restore one just deleted.
let rootListGeneration = 0;
// A replacing load writes only when it finishes, so until then the cursor
// still looks current to "load more". Only the newest load counts: an older
// one that never settles must not block "load more" for good.
let newestReplacingLoadDone = true;

/**
 * Runs a load that replaces the list. `isCurrent` turns false once a newer
 * one starts, so a load that finishes late can skip its writes.
 */
export async function runReplacingRootListLoad<T>(
  load: (isCurrent: () => boolean) => Promise<T>,
): Promise<T> {
  const generation = ++rootListGeneration;
  const isCurrent = () => generation === rootListGeneration;
  newestReplacingLoadDone = false;
  try {
    return await load(isCurrent);
  } finally {
    if (isCurrent()) newestReplacingLoadDone = true;
  }
}

/** True while the newest load that will replace the whole list is still reading. */
export function isReplacingRootListLoadInFlight(): boolean {
  return !newestReplacingLoadDone;
}

/** For a load that extends the list. The check turns false once any replacing load starts. */
export function watchRootList(): () => boolean {
  const generation = rootListGeneration;
  return () => generation === rootListGeneration;
}

export function fetchRootAppPage(page: number): Promise<KnowledgeHubApiResponse> {
  return KnowledgeHubApi.getNavigationNodes({
    page,
    limit: SIDEBAR_PAGINATION_PAGE_SIZE,
    include: 'counts',
    sortBy: 'updatedAt',
    sortOrder: 'desc',
  });
}

export function rootListPaginationAfter(
  pagination: KnowledgeHubApiResponse['pagination'] | undefined,
): { hasNext: boolean; nextPage: number } | null {
  return pagination
    ? { hasNext: pagination.hasNext, nextPage: pagination.hasNext ? pagination.page + 1 : pagination.page }
    : null;
}

export function collectionsFirst(appItems: KnowledgeHubNode[]): KnowledgeHubNode[] {
  return [
    ...appItems.filter((n) => isKbCollectionsHubApp(n)),
    ...appItems.filter((n) => !isKbCollectionsHubApp(n)),
  ];
}

/**
 * Loads the first page of root apps. Resolves `false`, without writing, when a
 * newer load started meanwhile; a failure is thrown only while still current,
 * so the caller's error handling never clobbers newer data.
 */
export async function loadRootAppListFirstPage(): Promise<boolean> {
  return runReplacingRootListLoad(async (isCurrent) => {
    let response: KnowledgeHubApiResponse;
    try {
      response = await fetchRootAppPage(1);
    } catch (error) {
      if (!isCurrent()) return false;
      throw error;
    }
    if (!isCurrent()) return false;

    const { setAppNodes, setAppRootListPagination } = useKnowledgeBaseStore.getState();
    setAppNodes(collectionsFirst(response.items.filter((n) => n.nodeType === 'app')));
    setAppRootListPagination(rootListPaginationAfter(response.pagination));
    return true;
  });
}
