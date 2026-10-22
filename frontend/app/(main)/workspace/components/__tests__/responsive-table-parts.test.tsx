import React from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { cleanup, render, screen } from '@testing-library/react';
import { Theme } from '@radix-ui/themes';

import '@/lib/__tests__/test-i18n';

const mobile = vi.hoisted(() => ({ value: true }));
vi.mock('@/lib/hooks/use-is-mobile', () => ({ useIsMobile: () => mobile.value }));
vi.mock('@/app/components/ui/MaterialIcon', () => ({ MaterialIcon: () => null }));

import { EntityDataTable } from '../entity-data-table';
import { SettingsRow } from '../settings-row';

afterEach(() => cleanup());

describe('EntityDataTable', () => {
  const columns = [
    { key: 'a', label: 'ColA', width: '200px', render: () => 'a' },
    { key: 'b', label: 'ColB', width: '100px', hideOnMobile: true, render: () => 'b' },
  ];
  const renderTable = () =>
    render(
      <Theme>
        <EntityDataTable
          columns={columns}
          data={[{ id: '1' }]}
          getItemId={(i) => i.id}
          selectedIds={new Set()}
          onSelectionChange={() => {}}
        />
      </Theme>,
    );

  it('scrolls horizontally and drops hideOnMobile columns on phones', () => {
    mobile.value = true;
    renderTable();
    expect(screen.getByTestId('entity-data-table').style.overflowX).toBe('auto');
    expect(screen.queryByText('ColB')).toBeNull();
    expect(screen.getByText('ColA')).toBeTruthy();
  });

  it('keeps every column on desktop', () => {
    mobile.value = false;
    renderTable();
    expect(screen.getByText('ColB')).toBeTruthy();
  });
});

describe('SettingsRow', () => {
  it('stacks at the smallest breakpoint', () => {
    const { container } = render(
      <Theme>
        <SettingsRow label="L">
          <input />
        </SettingsRow>
      </Theme>,
    );
    expect(container.querySelector('.rt-r-fd-column')).toBeTruthy();
  });
});
