'use client';

import { useEffect, useMemo, useState } from 'react';
import { useDemoDataStore } from './store';
import { checkRestrictedQuestionAccess, type RestrictedQuestionAccess } from './restricted-question';

/**
 * Whether the viewer can see the record behind the demo's restricted question,
 * so the chat landing can say up front that it will come back empty for them.
 * `null` until known, or when it cannot be told.
 */
export function useRestrictedQuestionAccess(): RestrictedQuestionAccess | null {
  const demoConnectors = useDemoDataStore((s) => s.demoConnectors);
  const idsKey = useMemo(
    () => demoConnectors.map((c) => c._key).filter((k): k is string => !!k).sort().join(','),
    [demoConnectors],
  );
  const [access, setAccess] = useState<RestrictedQuestionAccess | null>(null);

  useEffect(() => {
    let cancelled = false;
    setAccess(null);
    void checkRestrictedQuestionAccess(idsKey ? idsKey.split(',') : []).then((result) => {
      if (!cancelled) setAccess(result);
    });
    return () => {
      cancelled = true;
    };
  }, [idsKey]);

  return access;
}
