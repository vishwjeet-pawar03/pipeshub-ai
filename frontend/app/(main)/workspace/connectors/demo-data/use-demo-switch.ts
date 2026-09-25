'use client';

import { useCallback, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useToastStore } from '@/lib/store/toast-store';
import { useDemoDataStore } from './store';

/** Show or hide the demo for this person, with a toast when it cannot be saved. */
export function useDemoSwitch() {
  const { t } = useTranslation();
  const addToast = useToastStore((s) => s.addToast);
  const setIncludeInStore = useDemoDataStore((s) => s.setInclude);
  const [busy, setBusy] = useState(false);

  const setInclude = useCallback(
    async (include: boolean | null) => {
      setBusy(true);
      try {
        await setIncludeInStore(include);
      } catch {
        addToast({ variant: 'error', title: t('demoData.switch.errorTitle'), description: t('demoData.switch.errorDescription') });
      } finally {
        setBusy(false);
      }
    },
    [addToast, setIncludeInStore, t],
  );

  return { setInclude, busy };
}
