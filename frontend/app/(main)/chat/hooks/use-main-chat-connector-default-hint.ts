'use client';

import { usePathname } from 'next/navigation';
import { useEffectiveAgentId } from '@/chat/hooks/use-effective-agent-id';

/**
 * True when the hub connector picker is shown in **main app chat** (route `/chat`)
 * with **no effective agent** (assistant or universal agent). False on embedded
 * chat widgets (e.g. knowledge-base) and on URL/slot-scoped agent conversations.
 */
export function useMainChatConnectorDefaultHint(): boolean {
  const pathname = usePathname() ?? '';
  const effectiveAgentId = useEffectiveAgentId();

  const pathOnly = pathname.split('?')[0] ?? '';
  const normalized = pathOnly.replace(/\/+$/, '') || '/';
  const segments = normalized.split('/').filter(Boolean);
  const onChatRoute =
    segments.length > 0 && segments[segments.length - 1] === 'chat';

  return onChatRoute && !effectiveAgentId;
}
