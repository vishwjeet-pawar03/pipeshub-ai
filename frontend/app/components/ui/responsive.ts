import type React from 'react';

export const PAGE_PADDING_X = 'clamp(var(--space-4), 4vw, 100px)';
export const CATALOG_MAX_WIDTH = 1280;
export const SETTINGS_MAX_WIDTH = 960;
export const TOUCH_TARGET_PX = 44;

/** Overlay width that never exceeds the viewport minus 16px gutters. */
export const overlayWidth = (px: number): string => `min(${px}px, calc(100vw - 32px))`;

export const touchTarget = (enabled: boolean): React.CSSProperties | undefined =>
  enabled ? { minWidth: TOUCH_TARGET_PX, minHeight: TOUCH_TARGET_PX } : undefined;
