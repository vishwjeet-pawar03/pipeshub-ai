import React from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { Theme } from '@radix-ui/themes';

import '@/lib/__tests__/test-i18n';
import type { ArtifactListItem } from '../../types';
import { ArtifactsListView } from '../artifacts-list-view';
import { ArtifactsGridView } from '../artifacts-grid-view';

vi.mock('@/app/components/ui', () => ({ FileIcon: () => null }));
vi.mock('@/app/components/ui/MaterialIcon', () => ({ MaterialIcon: () => null }));

const PAGINATION = { page: 1, limit: 50, totalCount: 3, totalPages: 1 };

function artifact(overrides: Partial<ArtifactListItem>): ArtifactListItem {
  return { artifactId: 'a1', name: 'report.csv', artifactType: 'DATA_FILE', version: 1, ...overrides };
}

const owned = artifact({ artifactId: 'a1', name: 'mine.csv', role: 'OWNER' });
const shared = artifact({ artifactId: 'a2', name: 'shared.csv', role: 'READER' });
const unknown = artifact({ artifactId: 'a3', name: 'legacy.csv', role: null });

type Handlers = Record<'onPreview' | 'onDelete', (item: ArtifactListItem) => void>;

const views = {
  list: (handlers: Handlers) => (
    <ArtifactsListView
      items={[owned, shared, unknown]}
      sortBy="createdAtTimestamp"
      sortOrder="desc"
      onSort={vi.fn()}
      pagination={PAGINATION}
      onDownload={vi.fn()}
      onOpenChat={vi.fn()}
      onPageChange={vi.fn()}
      onPreview={handlers.onPreview}
      onDelete={handlers.onDelete}
    />
  ),
  grid: (handlers: Handlers) => (
    <ArtifactsGridView
      items={[owned, shared, unknown]}
      pagination={PAGINATION}
      onDownload={vi.fn()}
      onOpenChat={vi.fn()}
      onPageChange={vi.fn()}
      onPreview={handlers.onPreview}
      onDelete={handlers.onDelete}
    />
  ),
};

afterEach(() => cleanup());

describe.each(Object.entries(views))('%s view delete action', (_name, view) => {
  it('is offered only for artifacts the caller owns', () => {
    render(<Theme>{view({ onPreview: vi.fn(), onDelete: vi.fn() })}</Theme>);

    expect(screen.getAllByRole('button', { name: 'Delete' })).toHaveLength(1);
  });

  it('calls onDelete with the item without opening the preview', () => {
    const onPreview = vi.fn();
    const onDelete = vi.fn();
    render(<Theme>{view({ onPreview, onDelete })}</Theme>);

    fireEvent.click(screen.getByRole('button', { name: 'Delete' }));

    expect(onDelete).toHaveBeenCalledWith(owned);
    expect(onPreview).not.toHaveBeenCalled();
  });

  it('shows the translated artifact type', () => {
    render(<Theme>{view({ onPreview: vi.fn(), onDelete: vi.fn() })}</Theme>);

    expect(screen.getAllByText(/Data file/).length).toBeGreaterThan(0);
    expect(screen.queryByText(/DATA_FILE/)).toBeNull();
  });
});
