import React from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { Theme } from '@radix-ui/themes';
import '@/lib/__tests__/test-i18n';
import { ConfigSuccessDialog } from '../config-success-dialog';

afterEach(cleanup);

function renderDialog(props: Partial<React.ComponentProps<typeof ConfigSuccessDialog>> = {}) {
  const onDoLater = vi.fn();
  const onStartSyncing = vi.fn();
  render(
    <Theme>
      <ConfigSuccessDialog open connectorName="Drive" onStartSyncing={onStartSyncing} onDoLater={onDoLater} {...props} />
    </Theme>,
  );
  return { onDoLater, onStartSyncing };
}

describe('ConfigSuccessDialog', () => {
  it('is a dialog named by its title', () => {
    renderDialog();
    expect(screen.getByRole('dialog', { name: 'Instance is configured and ready to sync!' })).toBeTruthy();
  });

  it('shows extra content above the buttons', () => {
    renderDialog({ children: <p>Before you start</p> });
    const extra = screen.getByText('Before you start');
    const start = screen.getByRole('button', { name: 'Start syncing now' });
    expect(extra.compareDocumentPosition(start) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });

  it('treats Escape as doing it later', () => {
    const { onDoLater, onStartSyncing } = renderDialog();
    fireEvent.keyDown(screen.getByRole('dialog'), { key: 'Escape' });
    expect(onDoLater).toHaveBeenCalledTimes(1);
    expect(onStartSyncing).not.toHaveBeenCalled();
  });

  it('renders nothing while closed', () => {
    renderDialog({ open: false });
    expect(screen.queryByRole('dialog')).toBeNull();
  });
});
