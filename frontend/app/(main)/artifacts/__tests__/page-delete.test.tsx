import React, { useSyncExternalStore } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { Theme } from '@radix-ui/themes';

import '@/lib/__tests__/test-i18n';
import { ErrorType } from '@/lib/api';
import { useToastStore } from '@/lib/store/toast-store';
import type { ArtifactListItem } from '../types';
import { useArtifactsStore } from '../store';
import ArtifactsPage from '../page';

// jsdom has no matchMedia; the artifacts header uses it for the mobile layout.
Object.defineProperty(window, 'matchMedia', {
  configurable: true,
  writable: true,
  value: (query: string) => ({
    matches: false,
    media: query,
    addEventListener: () => {},
    removeEventListener: () => {},
  }),
});

const search = vi.hoisted(() => ({ params: new URLSearchParams() }));
const router = vi.hoisted(() => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn(), prefetch: vi.fn() }));

vi.mock('next/navigation', () => ({
  useRouter: () => router,
  usePathname: () => '/artifacts',
  useSearchParams: () =>
    useSyncExternalStore(
      () => () => {},
      () => search.params,
    ),
}));

const api = vi.hoisted(() => ({
  list: vi.fn(),
  listVersions: vi.fn(),
  delete: vi.fn(),
  streamRecord: vi.fn(),
  streamDownloadRecord: vi.fn(),
}));

vi.mock('../api', () => ({
  ArtifactsApi: { list: api.list, listVersions: api.listVersions, delete: api.delete, get: vi.fn() },
}));

vi.mock('@/app/(main)/knowledge-base/api', () => ({
  KnowledgeBaseApi: { streamRecord: api.streamRecord, streamDownloadRecord: api.streamDownloadRecord },
}));

vi.mock('@/app/components/file-preview', () => ({
  FilePreviewSidebar: ({ file, onDownload }: { file: { name: string }; onDownload?: () => void }) => (
    <section aria-label="File preview">
      <p>previewing:{file.name}</p>
      <button type="button" onClick={onDownload}>Download preview</button>
    </section>
  ),
}));

const OWNED: ArtifactListItem = {
  artifactId: 'art-1',
  name: 'report.csv',
  artifactType: 'DATA_FILE',
  mimeType: 'text/csv',
  version: 3,
  role: 'OWNER',
};

function failure(statusCode: number) {
  return { type: ErrorType.SERVER_ERROR, message: 'boom', statusCode };
}

beforeEach(() => {
  search.params = new URLSearchParams();
  useArtifactsStore.setState({
    items: [],
    page: 1,
    search: '',
    artifactTypes: [],
    dateFrom: undefined,
    dateTo: undefined,
    sortBy: 'createdAtTimestamp',
    sortOrder: 'desc',
    viewMode: 'list',
    isLoading: false,
    error: null,
  });
  useToastStore.setState({ toasts: [] });
  api.list.mockResolvedValue({
    items: [OWNED],
    pagination: { page: 1, limit: 50, totalCount: 1, totalPages: 1 },
  });
  api.listVersions.mockResolvedValue([]);
  api.streamRecord.mockResolvedValue(new Blob(['a,b'], { type: 'text/csv' }));
  URL.createObjectURL = vi.fn(() => 'blob:preview');
  URL.revokeObjectURL = vi.fn();
});

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

async function renderPage() {
  render(
    <Theme>
      <ArtifactsPage />
    </Theme>,
  );
  await screen.findByRole('button', { name: OWNED.name });
}

async function openDeleteDialog() {
  fireEvent.click(screen.getByRole('button', { name: 'Delete' }));
  return screen.findByRole('alertdialog');
}

function toastTitles() {
  return useToastStore.getState().toasts.map((t) => `${t.variant}:${t.title}`);
}

describe('deleting an artifact from the gallery', () => {
  it('names the file and its versions in the confirmation', async () => {
    await renderPage();

    const dialog = await openDeleteDialog();

    expect(within(dialog).getByText(/report\.csv/)).toBeTruthy();
    expect(within(dialog).getByText(/all its versions/i)).toBeTruthy();
  });

  it('does nothing on cancel', async () => {
    await renderPage();
    const dialog = await openDeleteDialog();

    fireEvent.click(within(dialog).getByRole('button', { name: 'Cancel' }));

    await waitFor(() => expect(screen.queryByRole('alertdialog')).toBeNull());
    expect(api.delete).not.toHaveBeenCalled();
    expect(screen.getByRole('button', { name: OWNED.name })).toBeTruthy();
  });

  it('removes the row and confirms on success', async () => {
    api.delete.mockResolvedValue(undefined);
    await renderPage();
    const dialog = await openDeleteDialog();
    api.list.mockResolvedValue({ items: [], pagination: { page: 1, limit: 50, totalCount: 0, totalPages: 0 } });

    fireEvent.click(within(dialog).getByRole('button', { name: 'Delete' }));

    await waitFor(() => expect(screen.queryByRole('button', { name: OWNED.name })).toBeNull());
    expect(api.delete).toHaveBeenCalledWith('art-1');
    expect(toastTitles().some((t) => t.startsWith('success:'))).toBe(true);
    expect(screen.queryByRole('alertdialog')).toBeNull();
  });

  it('treats an artifact that is already gone as deleted', async () => {
    api.delete.mockRejectedValue(failure(404));
    await renderPage();
    const dialog = await openDeleteDialog();
    api.list.mockResolvedValue({ items: [], pagination: { page: 1, limit: 50, totalCount: 0, totalPages: 0 } });

    fireEvent.click(within(dialog).getByRole('button', { name: 'Delete' }));

    await waitFor(() => expect(screen.queryByRole('button', { name: OWNED.name })).toBeNull());
    expect(toastTitles().some((t) => t.startsWith('success:'))).toBe(true);
  });

  it.each([403, 500])('keeps the row when the server answers %i', async (status) => {
    api.delete.mockRejectedValue(failure(status));
    await renderPage();
    const dialog = await openDeleteDialog();

    fireEvent.click(within(dialog).getByRole('button', { name: 'Delete' }));

    await waitFor(() => expect(screen.queryByRole('alertdialog')).toBeNull());
    expect(screen.getByRole('button', { name: OWNED.name })).toBeTruthy();
    expect(toastTitles().some((t) => t.startsWith('success:'))).toBe(false);
  });

  it('closes the preview of the artifact it deletes', async () => {
    api.delete.mockResolvedValue(undefined);
    await renderPage();
    fireEvent.click(screen.getByRole('button', { name: OWNED.name }));
    await screen.findByText('previewing:report.csv');

    const dialog = await openDeleteDialog();
    fireEvent.click(within(dialog).getByRole('button', { name: 'Delete' }));

    await waitFor(() => expect(screen.queryByText('previewing:report.csv')).toBeNull());
  });
});

describe('downloading from the gallery', () => {
  it('shows one toast when the download fails', async () => {
    api.streamDownloadRecord.mockRejectedValue(failure(500));
    await renderPage();

    fireEvent.click(screen.getByRole('button', { name: 'Download' }));

    await waitFor(() => expect(toastTitles()).toHaveLength(1));
    expect(api.streamDownloadRecord).toHaveBeenCalledWith(
      'art-1',
      'report.csv',
      expect.objectContaining({ version: 3, suppressErrorToast: true }),
    );
  });
});
