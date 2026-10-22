'use client';

import { useState, useEffect } from 'react';

const QUERY = '(hover: hover) and (pointer: fine)';

/** True when the primary input can hover (mouse/trackpad). Defaults to true without a window or matchMedia. */
export function useCanHover(): boolean {
  const [canHover, setCanHover] = useState(
    () => typeof window === 'undefined' || (window.matchMedia?.(QUERY)?.matches ?? true),
  );

  useEffect(() => {
    const mq = window.matchMedia?.(QUERY);
    if (!mq) return;
    setCanHover(mq.matches);

    const handler = (e: MediaQueryListEvent) => setCanHover(e.matches);
    mq.addEventListener('change', handler);
    return () => mq.removeEventListener('change', handler);
  }, []);

  return canHover;
}
