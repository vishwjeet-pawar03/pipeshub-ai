import { useKnowledgeBaseStore } from '../store';
import { KnowledgeHubApi } from '../api';
import { SIDEBAR_PAGINATION_PAGE_SIZE } from '../constants';
import { isKbCollectionsHubApp } from './all-records-transformer';
import { categorizeNodes, withOpenFoldersRestored } from './tree-builder';
import { sidebarNodeChildrenMetaAfterPage } from './sidebar-child-pagination-meta';
import type { KnowledgeHubApiResponse, KnowledgeHubNode, NodeType } from '../types';

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

/** Puts the cached children of every open folder back into the sidebar tree, at any depth. */
export function restoreOpenFoldersInSidebar(): void {
  const { categorizedNodes, nodeChildrenCache, expandedFolders, setCategorizedNodes } =
    useKnowledgeBaseStore.getState();
  if (!categorizedNodes) return;
  setCategorizedNodes({
    shared: withOpenFoldersRestored(categorizedNodes.shared, nodeChildrenCache, expandedFolders),
    private: withOpenFoldersRestored(categorizedNodes.private, nodeChildrenCache, expandedFolders),
  });
}

/**
 * Fetches fresh children for every open folder under `rootIds` (after a
 * rename, any of them may show an old name), reading as many pages as were
 * shown before and storing the cursor, so "load more" carries on from there.
 */
export async function reloadOpenFoldersUnder(rootIds: string[]): Promise<void> {
  const { expandedFolders, nodeChildrenCache, nodes } = useKnowledgeBaseStore.getState();
  const open: string[] = [];
  const seen = new Set<string>();
  const queue = [...rootIds];
  while (queue.length > 0) {
    const id = queue.shift()!;
    if (seen.has(id)) continue;
    seen.add(id);
    if (!expandedFolders[id]) continue;
    open.push(id);
    for (const child of nodeChildrenCache.get(id) ?? []) queue.push(child.id);
  }

  for (const id of open) {
    const nodeType = (nodes.find((n) => n.id === id)?.nodeType ?? 'folder') as NodeType;
    const pagesShown = Math.max(1, Math.ceil((nodeChildrenCache.get(id)?.length ?? 0) / SIDEBAR_PAGINATION_PAGE_SIZE));
    const byId = new Map<string, KnowledgeHubNode>();
    let cursor = sidebarNodeChildrenMetaAfterPage(undefined, 0, SIDEBAR_PAGINATION_PAGE_SIZE, 1, nodeType);
    for (let page = 1; page <= pagesShown; page += 1) {
      const response = await KnowledgeHubApi.getNodeChildren(nodeType, id, {
        onlyContainers: true,
        page,
        limit: SIDEBAR_PAGINATION_PAGE_SIZE,
        include: 'counts',
        sortBy: 'name',
        sortOrder: 'asc',
      });
      for (const item of response.items) byId.set(item.id, item);
      cursor = sidebarNodeChildrenMetaAfterPage(
        response.pagination,
        response.items.length,
        SIDEBAR_PAGINATION_PAGE_SIZE,
        page,
        nodeType,
      );
      if (!cursor.hasNext) break;
    }
    const { cacheNodeChildren, addNodes, setNodeChildrenPagination } = useKnowledgeBaseStore.getState();
    cacheNodeChildren(id, [...byId.values()]);
    addNodes([...byId.values()]);
    setNodeChildrenPagination(id, cursor);
  }
  restoreOpenFoldersInSidebar();
}

/**
 * Replaces the collections shown in the sidebar and keeps open folders open.
 * Folders loaded by expanding stay in `nodes` while their collection is still
 * listed: the sidebar's expand handler looks them up there.
 */
export function showCollectionsInSidebar(collections: KnowledgeHubNode[]): void {
  const { nodes, nodeChildrenCache, setNodes, setCategorizedNodes } = useKnowledgeBaseStore.getState();
  const reachable = new Set(collections.map((n) => n.id));
  const queue = [...reachable];
  while (queue.length > 0) {
    for (const child of nodeChildrenCache.get(queue.pop()!) ?? []) {
      if (!reachable.has(child.id)) {
        reachable.add(child.id);
        queue.push(child.id);
      }
    }
  }
  setNodes([...collections, ...nodes.filter((n) => n.nodeType !== 'app' && reachable.has(n.id))]);
  setCategorizedNodes(categorizeNodes(collections, null));
  restoreOpenFoldersInSidebar();
}
