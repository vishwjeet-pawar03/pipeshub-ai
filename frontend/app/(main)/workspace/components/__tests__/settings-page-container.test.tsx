import React from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { cleanup, render } from '@testing-library/react';

import { PAGE_PADDING_X, CATALOG_MAX_WIDTH } from '@/app/components/ui/responsive';

const mobile = vi.hoisted(() => ({ value: false }));
vi.mock('@/lib/hooks/use-is-mobile', () => ({
  useIsMobile: () => mobile.value,
  useIsBelowDesktop: () => mobile.value,
}));

import { SettingsPageContainer } from '../settings-page-container';

afterEach(() => cleanup());

describe('SettingsPageContainer', () => {
  it('fills the content area with 100px sides on desktop', () => {
    const { getByTestId } = render(
      <SettingsPageContainer>
        <span data-testid="child" />
      </SettingsPageContainer>,
    );
    const el = getByTestId('child').parentElement as HTMLElement;
    expect(el.style.paddingInline).toBe('100px');
    expect(el.style.maxWidth).toBe('');
    expect(el.style.paddingTop).toBe('64px');
  });

  it('accepts a max width, uses fluid padding and clears the hamburger on mobile', () => {
    mobile.value = true;
    const { getByTestId } = render(
      <SettingsPageContainer maxWidth={CATALOG_MAX_WIDTH}>
        <span data-testid="child" />
      </SettingsPageContainer>,
    );
    const el = getByTestId('child').parentElement as HTMLElement;
    expect(el.style.maxWidth).toBe('1280px');
    expect(el.style.paddingInline).toBe(PAGE_PADDING_X);
    expect(el.style.paddingTop).toBe('max(56px, var(--space-8))');
    mobile.value = false;
  });
});
