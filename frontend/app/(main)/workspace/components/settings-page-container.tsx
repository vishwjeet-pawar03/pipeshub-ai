'use client';

import React from 'react';
import { Box } from '@radix-ui/themes';
import { useIsBelowDesktop, useIsMobile } from '@/lib/hooks/use-is-mobile';
import { PAGE_PADDING_X } from '@/app/components/ui/responsive';

/** Top padding that clears the fixed mobile hamburger. */
export const MOBILE_PAGE_PADDING_TOP = 'max(56px, var(--space-8))';

/** Desktop side padding of settings pages, as they were before they became fluid. */
const DESKTOP_PADDING_X = 100;

/**
 * Inline padding for a full-width scroll surface whose content is capped at `maxWidth`
 * (keeps the scrollbar at the viewport edge, unlike a centered max-width box).
 */
export const pageInlinePadding = (maxWidth: number): string =>
  `max(${PAGE_PADDING_X}, calc((100% - ${maxWidth}px) / 2))`;

export interface SettingsPageContainerProps {
  children: React.ReactNode;
  /** Caps the box width; settings pages fill the content area when it is omitted. */
  maxWidth?: number;
}

export function SettingsPageContainer({ children, maxWidth }: SettingsPageContainerProps) {
  const isMobile = useIsMobile();
  const isBelowDesktop = useIsBelowDesktop();
  return (
    <Box
      style={{
        width: '100%',
        maxWidth,
        marginInline: 'auto',
        paddingInline: isBelowDesktop ? PAGE_PADDING_X : DESKTOP_PADDING_X,
        paddingTop: isMobile ? MOBILE_PAGE_PADDING_TOP : 64,
        paddingBottom: 64,
      }}
    >
      {children}
    </Box>
  );
}
