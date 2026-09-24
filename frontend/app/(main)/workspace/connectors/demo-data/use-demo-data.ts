'use client';

import { useCallback, useEffect, useState } from 'react';
import { useDemoDataStore } from './store';
import { isRemovalNoticeSnoozed, snoozeRemovalNotice } from './demo-data';

/**
 * True when the org has an active Demo connector (the Acme Corp sample
 * company). Looks it up on mount; call it once per page, not per item.
 */
export function useDemoDataActive(): boolean {
  const active = useDemoDataStore((s) => s.demoConnectors.length > 0);
  const load = useDemoDataStore((s) => s.loadDemoConnectors);

  useEffect(() => {
    void load();
  }, [load]);

  return active;
}

/**
 * Whether a record from `connectorId` is Acme Corp sample data. Reads what the
 * page already looked up, so it is cheap to call for every citation.
 */
export function useIsDemoSource(connectorId: string | undefined): boolean {
  return useDemoDataStore(
    (s) => !!connectorId && s.demoConnectors.some((c) => c._key === connectorId),
  );
}

/**
 * Whether to offer removing the demo data: to an admin, while it is active,
 * once some other connector has indexed records — the point where answers
 * start mixing Acme Corp with the company's own data.
 */
export function useDemoRemovalNotice(isAdmin: boolean | null) {
  const demoConnectors = useDemoDataStore((s) => s.demoConnectors);
  const realDataIndexed = useDemoDataStore((s) => s.realDataIndexed);
  const checkRealData = useDemoDataStore((s) => s.checkRealData);
  const [, setSnoozeTick] = useState(0);

  const hasDemo = demoConnectors.length > 0;
  const noticeKey = demoConnectors[0]?._key;

  useEffect(() => {
    if (isAdmin === true && hasDemo) void checkRealData();
  }, [isAdmin, hasDemo, checkRealData]);

  const snooze = useCallback(() => {
    if (noticeKey) snoozeRemovalNotice(noticeKey);
    setSnoozeTick((n) => n + 1);
  }, [noticeKey]);

  const show =
    isAdmin === true &&
    hasDemo &&
    realDataIndexed === true &&
    !!noticeKey &&
    !isRemovalNoticeSnoozed(noticeKey);

  return { show, demoConnectors, snooze };
}
