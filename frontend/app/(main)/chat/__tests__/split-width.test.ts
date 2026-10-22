import { describe, it, expect } from 'vitest';
import { clampSplitWidth } from '../constants';

describe('clampSplitWidth', () => {
  it('caps a width saved on a wider screen at 60% of this viewport', () => {
    expect(clampSplitWidth(900, 1000)).toBe(600);
  });

  it('keeps the saved width when the viewport has room for it', () => {
    expect(clampSplitWidth(900, 1920)).toBe(900);
  });
});
