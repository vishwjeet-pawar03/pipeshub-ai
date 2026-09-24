'use client';

import { create } from 'zustand';
import { devtools } from 'zustand/middleware';
import { ConnectorsApi } from '../api';
import type { Connector, ConnectorScope } from '../types';
import { demoConnectorsIn, hasIndexedRecords, otherConnectorsIn } from './demo-data';

interface DemoDataState {
  /** Active Demo connector instances (the Acme Corp sample company); empty until found. */
  demoConnectors: Connector[];
  /**
   * Whether some other connector already has indexed records, which is when
   * answers start mixing Acme Corp with the company's own data. `null` until checked.
   */
  realDataIndexed: boolean | null;
  loadDemoConnectors: () => Promise<void>;
  checkRealData: () => Promise<void>;
  /** Forget what was found, e.g. once the demo data has been removed. */
  reset: () => void;
}

// One lookup at a time, however many components ask.
let demoLookup: Promise<void> | null = null;
let realDataLookup: Promise<void> | null = null;

async function connectorsIn(scope: ConnectorScope): Promise<Connector[]> {
  try {
    return (await ConnectorsApi.getActiveConnectors(scope)).connectors ?? [];
  } catch {
    return [];
  }
}

export const useDemoDataStore = create<DemoDataState>()(
  devtools(
    (set, get) => ({
      demoConnectors: [],
      realDataIndexed: null,

      // Only a positive answer is remembered. Chat is the landing page, so the
      // first lookup of a session usually runs before anyone has turned the
      // demo on; remembering that "no", or a failed lookup, would hide the demo
      // for the rest of the session.
      loadDemoConnectors: () => {
        if (get().demoConnectors.length > 0) return Promise.resolve();
        demoLookup ??= ConnectorsApi.getActiveConnectors('team')
          .then((res) => set({ demoConnectors: demoConnectorsIn(res.connectors ?? []) }))
          .catch(() => undefined)
          .finally(() => {
            demoLookup = null;
          });
        return demoLookup;
      },

      // Same rule: once real data is there it stays there, while "none yet" is
      // asked again next time.
      checkRealData: () => {
        if (get().realDataIndexed === true) return Promise.resolve();
        realDataLookup ??= (async () => {
          const candidates = otherConnectorsIn([
            ...(await connectorsIn('team')),
            ...(await connectorsIn('personal')),
          ]);
          for (const connector of candidates) {
            try {
              const stats = await ConnectorsApi.getConnectorStats(connector._key as string);
              if (hasIndexedRecords(stats.data)) {
                set({ realDataIndexed: true });
                return;
              }
            } catch {
              // One connector's stats failing says nothing about the others.
            }
          }
          set({ realDataIndexed: false });
        })().finally(() => {
          realDataLookup = null;
        });
        return realDataLookup;
      },

      reset: () => {
        demoLookup = null;
        realDataLookup = null;
        set({ demoConnectors: [], realDataIndexed: null });
      },
    }),
    { name: 'demo-data-store' },
  ),
);
