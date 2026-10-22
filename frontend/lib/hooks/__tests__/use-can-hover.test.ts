import { describe, it, expect, afterEach, vi } from 'vitest';
import { renderHook } from '@testing-library/react';
import { useCanHover } from '../use-can-hover';

afterEach(() => vi.unstubAllGlobals());

const stub = (matches: boolean) =>
  vi.stubGlobal('matchMedia', (q: string) => ({
    matches,
    media: q,
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
  }));

describe('useCanHover', () => {
  it('is false when hover is not supported', () => {
    stub(false);
    expect(renderHook(() => useCanHover()).result.current).toBe(false);
  });

  it('is true when hover and a fine pointer are available', () => {
    stub(true);
    expect(renderHook(() => useCanHover()).result.current).toBe(true);
  });
});
