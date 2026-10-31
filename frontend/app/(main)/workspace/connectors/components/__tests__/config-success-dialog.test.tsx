import React from 'react';
import { afterEach, describe, expect, it } from 'vitest';
import { cleanup, render, screen } from '@testing-library/react';
import '@/lib/__tests__/test-i18n';
import { ConfigSuccessDialog } from '../config-success-dialog';

afterEach(cleanup);

describe('ConfigSuccessDialog', () => {
  it('shows extra content above the buttons', () => {
    render(
      <ConfigSuccessDialog open connectorName="Drive" onStartSyncing={() => {}} onDoLater={() => {}}>
        <p>Before you start</p>
      </ConfigSuccessDialog>,
    );
    const extra = screen.getByText('Before you start');
    const start = screen.getByRole('button', { name: 'Start syncing now' });
    expect(extra.compareDocumentPosition(start) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });
});
