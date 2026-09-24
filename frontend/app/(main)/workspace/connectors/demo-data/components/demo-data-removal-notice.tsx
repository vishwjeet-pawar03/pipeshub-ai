'use client';

import React, { useState } from 'react';
import { Button, Callout, Flex, Text } from '@radix-ui/themes';
import { useTranslation } from 'react-i18next';
import { MaterialIcon } from '@/app/components/ui/MaterialIcon';
import { useDemoDataActive, useDemoRemovalNotice } from '../use-demo-data';
import { RemoveDemoDataDialog } from './remove-demo-data-dialog';

interface DemoDataRemovalNoticeProps {
  isAdmin: boolean | null;
}

/**
 * Tells an admin that answers still include the Acme Corp demo data once the
 * company's own data has arrived, and offers to remove it. Nothing is removed
 * without asking; "Keep for now" hides the notice for a week in this browser.
 */
export function DemoDataRemovalNotice({ isAdmin }: DemoDataRemovalNoticeProps) {
  const { t } = useTranslation();
  useDemoDataActive();
  const { show, demoConnectors, snooze } = useDemoRemovalNotice(isAdmin);
  const [dialogOpen, setDialogOpen] = useState(false);

  if (!show && !dialogOpen) return null;

  const connectorIds = demoConnectors.map((c) => c._key).filter((id): id is string => !!id);

  return (
    <>
      {show && (
        <Callout.Root color="orange" variant="surface" size="1" style={{ width: '100%' }}>
          <Callout.Icon>
            <MaterialIcon name="info" size={16} />
          </Callout.Icon>
          <Flex direction="column" gap="2">
            <Text size="2" weight="medium">
              {t('demoData.removalNotice.title')}
            </Text>
            <Text size="2">{t('demoData.removalNotice.description')}</Text>
            <Flex gap="2" wrap="wrap">
              <Button size="1" color="orange" onClick={() => setDialogOpen(true)}>
                {t('demoData.removalNotice.remove')}
              </Button>
              <Button size="1" variant="soft" color="gray" onClick={snooze}>
                {t('demoData.removalNotice.keep')}
              </Button>
            </Flex>
          </Flex>
        </Callout.Root>
      )}
      <RemoveDemoDataDialog open={dialogOpen} onOpenChange={setDialogOpen} connectorIds={connectorIds} />
    </>
  );
}
