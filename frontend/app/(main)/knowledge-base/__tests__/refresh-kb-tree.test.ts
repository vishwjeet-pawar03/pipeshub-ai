import { describe, it, expect, vi, beforeEach } from 'vitest';
import { useKnowledgeBaseStore } from '../store';
import { refreshKbTree } from '../utils/refresh-kb-tree';
import { collection, hubNode, hubResponse } from './kb-page-harness';

const getNavigationNodes = vi.hoisted(() => vi.fn());
vi.mock('../api', () => ({ KnowledgeHubApi: { getNavigationNodes } }));

const ENGINEERING = collection('kb-eng', 'Engineering');
const DRIVE = hubNode({ id: 'app-drive', name: 'Google Drive', nodeType: 'app', origin: 'CONNECTOR', connector: 'DRIVE' });

function sidebarCollectionIds() {
  const tree = useKnowledgeBaseStore.getState().categorizedNodes;
  return [...(tree?.shared ?? []), ...(tree?.private ?? [])].map((n) => n.id);
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

  it('keeps the known collections when they may be on a later page of the list', async () => {
    const firstPage = hubResponse([DRIVE]);
    firstPage.pagination = { ...firstPage.pagination, hasNext: true, totalPages: 2 };
    getNavigationNodes.mockResolvedValue(firstPage);

    await refreshKbTree();

    expect(sidebarCollectionIds()).toEqual(['kb-eng']);
  });

  it('shows the collections the server returned', async () => {
    getNavigationNodes.mockResolvedValue(hubResponse([collection('kb-new', 'Handbook'), DRIVE]));
    const after = vi.fn();

    await refreshKbTree(after);

    expect(sidebarCollectionIds()).toEqual(['kb-new']);
    expect(after).toHaveBeenCalledTimes(1);
  });
});
