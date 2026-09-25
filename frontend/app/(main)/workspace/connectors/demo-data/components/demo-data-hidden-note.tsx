'use client';

import React from 'react';
import { Link, Text } from '@radix-ui/themes';
import { useTranslation } from 'react-i18next';
import { useDemoSwitch } from '../use-demo-switch';

/** Chat landing line while this person has the demo hidden: says so, and brings it back. */
export function DemoDataHiddenNote() {
  const { t } = useTranslation();
  const { setInclude, busy } = useDemoSwitch();
  return (
    <Text size="1" style={{ color: 'var(--slate-11)', textAlign: 'center', marginTop: 'var(--space-5)' }}>
      {t('chat.demoHidden')}{' '}
      <Link asChild size="1" weight="medium">
        <button type="button" disabled={busy} onClick={() => void setInclude(true)} style={{ background: 'none', border: 0, padding: 0, cursor: 'pointer' }}>
          {t('chat.demoShow')}
        </button>
      </Link>
    </Text>
  );
}
