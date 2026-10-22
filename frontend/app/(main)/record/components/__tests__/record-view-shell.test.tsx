import React from 'react';
import { afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import { cleanup, render, screen, waitFor } from '@testing-library/react';
import { Theme } from '@radix-ui/themes';
import '@/lib/__tests__/test-i18n';

const env = vi.hoisted(() => ({ mobile: true }));
vi.mock('@/lib/hooks/use-is-mobile', () => ({
  useIsMobile: () => env.mobile,
  useIsBelowDesktop: () => env.mobile,
}));
vi.mock('next/navigation', () => ({ useRouter: () => ({ back: () => {}, push: () => {} }) }));
vi.mock('@/app/components/theme-provider', () => ({ useThemeAppearance: () => ({ appearance: 'light' }) }));
vi.mock('@/app/components/file-preview/renderers/file-preview-renderer', () => ({
  FilePreviewRenderer: () => <div data-testid="preview" />,
}));
vi.mock('../record-metadata-panel', () => ({ RecordMetadataPanel: () => <div data-testid="metadata" /> }));
vi.mock('../zoom-action-bar', () => ({ ZoomActionBar: () => <div data-testid="zoom-bar" /> }));
vi.mock('@/app/components/ui/auth-guard', () => ({ LoadingScreen: () => <div>loading</div> }));
vi.mock('@/app/(main)/knowledge-base/api', () => ({
  KnowledgeBaseApi: {
    getRecordDetails: vi.fn(async () => ({
      record: { id: 'r1', recordName: 'Plan.pdf', recordType: 'FILE', mimeType: 'application/pdf', origin: 'UPLOAD' },
    })),
    streamRecord: vi.fn(async () => new Blob(['x'], { type: 'application/pdf' })),
  },
}));

import { RecordViewShell } from '../record-view-shell';

beforeAll(() => {
  URL.createObjectURL = () => 'blob:x';
  URL.revokeObjectURL = () => {};
});
afterEach(cleanup);

const renderShell = () =>
  render(
    <Theme>
      <RecordViewShell recordId="r1" />
    </Theme>,
  );

describe('RecordViewShell on phones', () => {
  it('hides the zoom bar and keeps metadata behind a collapsible section', async () => {
    env.mobile = true;
    renderShell();
    await waitFor(() => expect(screen.getByTestId('preview')).toBeTruthy());
    expect(screen.queryByTestId('zoom-bar')).toBeNull();
    expect(screen.queryByTestId('metadata')).toBeNull();
    screen.getByRole('button', { name: /Record Information/ }).click();
    await waitFor(() => expect(screen.getByTestId('metadata')).toBeTruthy());
  });

  it('shows the zoom bar and metadata side by side on desktop', async () => {
    env.mobile = false;
    renderShell();
    await waitFor(() => expect(screen.getByTestId('metadata')).toBeTruthy());
    expect(screen.getByTestId('zoom-bar')).toBeTruthy();
  });
});
