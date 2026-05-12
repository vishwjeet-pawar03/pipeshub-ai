'use client';

import { usePathname } from 'next/navigation';
import { useEffectiveAgentId } from '@/chat/hooks/use-effective-agent-id';

/**
 * True when the hub connector picker is shown in **main app chat** (pathname ends
 * with segment `chat`, e.g. `/chat` or a base-path prefix) with **no effective agent**
 * (assistant or universal agent). False on URL/slot-scoped agent chat and on routes
 * that include `knowledge-base` (avoids false positives like `/knowledge-base/chat`).
 *
 * `usePathname()` is already pathname-only (no query string).
 */
export function useMainChatConnectorDefaultHint(): boolean {
  const pathname = usePathname() ?? '';
  const effectiveAgentId = useEffectiveAgentId();

  const normalized = pathname.replace(/\/+$/, '') || '/';
  const segments = normalized.split('/').filter(Boolean);
  const onChatRoute =
    segments.length > 0 &&
    segments[segments.length - 1] === 'chat' &&
    !segments.includes('knowledge-base');

  return onChatRoute && !effectiveAgentId;
}
