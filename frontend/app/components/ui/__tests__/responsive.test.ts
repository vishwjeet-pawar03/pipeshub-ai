import { describe, it, expect } from 'vitest';
import { overlayWidth, touchTarget, PAGE_PADDING_X } from '../responsive';

describe('responsive helpers', () => {
  it('overlayWidth caps at the viewport minus gutters', () => {
    expect(overlayWidth(600)).toBe('min(600px, calc(100vw - 32px))');
  });

  it('touchTarget is undefined when disabled and 44px when enabled', () => {
    expect(touchTarget(false)).toBeUndefined();
    expect(touchTarget(true)).toEqual({ minWidth: 44, minHeight: 44 });
  });

  it('exposes the page padding token', () => {
    expect(PAGE_PADDING_X).toBe('clamp(var(--space-4), 4vw, 100px)');
  });
});
