import { describe, it, expect, vi, beforeEach } from 'vitest';
import { useKnowledgeBaseStore } from '../store';
import { refreshKbTree } from '../utils/refresh-kb-tree';
import { collection, hubNode, hubResponse } from './kb-page-harness';
import type { KnowledgeHubNode } from '../types';

const getNavigationNodes = vi.hoisted(() => vi.fn());
vi.mock('../api', () => ({ KnowledgeHubApi: { getNavigationNodes } }));

const ENGINEERING = collection('kb-eng', 'Engineering');
const DRIVE = hubNode({ id: 'app-drive', name: 'Google Drive', nodeType: 'app', origin: 'CONNECTOR', connector: 'DRIVE' });

function sidebarCollectionIds() {
  const tree = useKnowledgeBaseStore.getState().categorizedNodes;
  return [...(tree?.shared ?? []), ...(tree?.private ?? [])].map((n) => n.id);
}

function sidebarCollectionNames() {
  const tree = useKnowledgeBaseStore.getState().categorizedNodes;
  return [...(tree?.shared ?? []), ...(tree?.private ?? [])].map((n) => n.name);
}

function connectors(count: number, offset = 0) {
  return Array.from({ length: count }, (_, i) =>
    hubNode({ id: `app-${offset + i}`, name: `Connector ${offset + i}`, nodeType: 'app', origin: 'CONNECTOR', connector: 'DRIVE' }),
  );
}

function pages(itemsByPage: KnowledgeHubNode[][]) {
  getNavigationNodes.mockImplementation(async ({ page }: { page: number }) => {
    const items = itemsByPage[page - 1] ?? [];
    const hasNext = page < itemsByPage.length;
    return hubResponse(items, {
      pagination: { page, limit: 20, totalItems: 0, totalPages: itemsByPage.length, hasNext, hasPrev: page > 1 },
    });
  });
}

beforeEach(() => {
  useKnowledgeBaseStore.setState(useKnowledgeBaseStore.getInitialState(), true);
  const { setAppNodes, setNodes, setCategorizedNodes } = useKnowledgeBaseStore.getState();
  setAppNodes([ENGINEERING]);
  setNodes([ENGINEERING]);
  setCategorizedNodes({ shared: [], private: [] });
  getNavigationNodes.mockReset();
});

describe('refreshKbTree', () => {
  it('empties the sidebar when the server reports no collections at all', async () => {
    getNavigationNodes.mockResolvedValue(hubResponse([DRIVE]));

    await refreshKbTree();

    expect(sidebarCollectionIds()).toEqual([]);
  });

  it('finds the collections that sit behind a full page of connectors', async () => {
    pages([connectors(20), [collection('kb-eng', 'Engineering (renamed)')]]);

    await refreshKbTree();

    expect(sidebarCollectionIds()).toEqual(['kb-eng']);
    expect(sidebarCollectionNames()).toEqual(['Engineering (renamed)']);
    expect(getNavigationNodes).toHaveBeenLastCalledWith(expect.objectContaining({ page: 2 }));
  });

  it('does not bring back a deleted collection when connectors fill the first page', async () => {
    const { setAppNodes, setNodes } = useKnowledgeBaseStore.getState();
    setAppNodes([collection('kb-gone', 'Deleted'), ENGINEERING]);
    setNodes([ENGINEERING]);
    pages([connectors(20), [ENGINEERING]]);

    await refreshKbTree();

    expect(sidebarCollectionIds()).toEqual(['kb-eng']);
  });

  it('empties the sidebar when no page of the list has a collection', async () => {
    pages([connectors(20), connectors(3, 20)]);

    await refreshKbTree();

    expect(sidebarCollectionIds()).toEqual([]);
    expect(getNavigationNodes).toHaveBeenCalledTimes(2);
  });

  it('shows the collections the server returned', async () => {
    getNavigationNodes.mockResolvedValue(hubResponse([collection('kb-new', 'Handbook'), DRIVE]));
    const after = vi.fn();

    await refreshKbTree(after);

    expect(sidebarCollectionIds()).toEqual(['kb-new']);
    expect(after).toHaveBeenCalledTimes(1);
  });
});
