'use client';

import { useState, useEffect } from 'react';

export const MOBILE_MAX_PX = 768;
export const BELOW_DESKTOP_MAX_PX = 1023;

/** Tracks a max-width media query. The first render already reads matchMedia (no desktop-first flash). */
function useMaxWidth(px: number): boolean {
  const query = `(max-width: ${px}px)`;
  const [matches, setMatches] = useState(
    () => typeof window !== 'undefined' && window.matchMedia(query).matches,
  );

  useEffect(() => {
    const mq = window.matchMedia(query);
    setMatches(mq.matches);

    const handler = (e: MediaQueryListEvent) => setMatches(e.matches);
    mq.addEventListener('change', handler);
    return () => mq.removeEventListener('change', handler);
  }, [query]);

  return matches;
}

/** True when the viewport width is ≤768px. */
export const useIsMobile = (): boolean => useMaxWidth(MOBILE_MAX_PX);

/** True when the viewport width is ≤1023px (phones and tablets). */
export const useIsBelowDesktop = (): boolean => useMaxWidth(BELOW_DESKTOP_MAX_PX);
