import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { cleanup, render, screen, waitFor } from '@testing-library/react';

import type { ArtifactListItem } from '../../types';

const kb = vi.hoisted(() => ({ streamRecord: vi.fn() }));
vi.mock('@/app/(main)/knowledge-base/api', () => ({ KnowledgeBaseApi: kb }));
vi.mock('@/app/components/ui', () => ({ FileIcon: () => <span data-testid="file-icon" /> }));

import { ArtifactThumbnail } from '../artifact-thumbnail';

const IMAGE: ArtifactListItem = { artifactId: 'img-1', name: 'chart.png', artifactType: 'IMAGE', mimeType: 'image/png', version: 2 };

let observed: Element[] = [];
let callback: IntersectionObserverCallback | undefined;

beforeEach(() => {
  observed = [];
  callback = undefined;
  globalThis.IntersectionObserver = class {
    constructor(cb: IntersectionObserverCallback) { callback = cb; }
    observe(el: Element) { observed.push(el); }
    disconnect() {}
    unobserve() {}
  } as never;
  URL.createObjectURL = vi.fn(() => 'blob:thumb');
  URL.revokeObjectURL = vi.fn();
});

afterEach(() => {
  cleanup();
  vi.clearAllMocks();
});

const scrollIntoView = () => callback?.([{ isIntersecting: true } as IntersectionObserverEntry], {} as IntersectionObserver);

describe('ArtifactThumbnail', () => {
  it('shows the file icon for anything that is not an image, without fetching', () => {
    render(<ArtifactThumbnail item={{ ...IMAGE, name: 'a.csv', mimeType: 'text/csv', artifactType: 'SPREADSHEET' }} />);

    expect(screen.getByTestId('file-icon')).toBeTruthy();
    expect(kb.streamRecord).not.toHaveBeenCalled();
  });

  it('fetches an image only once it scrolls into view, quietly, at its current version', async () => {
    kb.streamRecord.mockResolvedValue(new Blob(['x'], { type: 'image/png' }));
    render(<ArtifactThumbnail item={IMAGE} />);
    expect(kb.streamRecord).not.toHaveBeenCalled();

    scrollIntoView();

    await waitFor(() => expect(screen.getByRole('img', { name: 'chart.png' })).toBeTruthy());
    expect(kb.streamRecord).toHaveBeenCalledWith('img-1', { version: 2, suppressErrorToast: true });
  });

  it('falls back to the file icon when the image cannot be loaded', async () => {
    kb.streamRecord.mockRejectedValue(new Error('gone'));
    render(<ArtifactThumbnail item={IMAGE} />);

    scrollIntoView();

    await waitFor(() => expect(kb.streamRecord).toHaveBeenCalled());
    expect(screen.getByTestId('file-icon')).toBeTruthy();
    expect(screen.queryByRole('img')).toBeNull();
  });

  it('releases the image when it goes away', async () => {
    kb.streamRecord.mockResolvedValue(new Blob(['x'], { type: 'image/png' }));
    const { unmount } = render(<ArtifactThumbnail item={IMAGE} />);
    scrollIntoView();
    await waitFor(() => expect(screen.getByRole('img')).toBeTruthy());

    unmount();

    expect(URL.revokeObjectURL).toHaveBeenCalledWith('blob:thumb');
  });
});
