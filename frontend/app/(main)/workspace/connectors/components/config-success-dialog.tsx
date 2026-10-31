'use client';

import React from 'react';
import { useTranslation } from 'react-i18next';
import { Button, Dialog, Flex } from '@radix-ui/themes';

// ========================================
// Props
// ========================================

interface ConfigSuccessDialogProps {
  open: boolean;
  connectorName: string;
  onStartSyncing: () => void;
  onDoLater: () => void;
  /** Shown under the description, before the buttons. */
  children?: React.ReactNode;
}

// ========================================
// ConfigSuccessDialog
// ========================================

export function ConfigSuccessDialog({
  open,
  connectorName,
  onStartSyncing,
  onDoLater,
  children,
}: ConfigSuccessDialogProps) {
  const { t } = useTranslation();

  return (
    // Escape or a click outside means the same as "I'll do it later".
    <Dialog.Root open={open} onOpenChange={(next) => !next && onDoLater()}>
      <Dialog.Content maxWidth="420px" style={{ padding: 'var(--space-5)' }}>
        <Flex direction="column" gap="3">
          <Dialog.Title size="4" weight="bold" style={{ color: 'var(--gray-12)', margin: 0 }}>
            {t('workspace.connectors.successDialog.title')}
          </Dialog.Title>

          <Dialog.Description size="2" style={{ color: 'var(--gray-11)', lineHeight: '20px' }}>
            {t('workspace.connectors.successDialog.description', { name: connectorName })}{' '}
            {t('workspace.connectors.successDialog.descriptionSuffix')}
          </Dialog.Description>

          {children}

          <Flex align="center" gap="3" style={{ marginTop: 'var(--space-1)' }}>
            <Button variant="outline" color="gray" size="2" onClick={onDoLater} style={{ cursor: 'pointer' }}>
              {t('workspace.connectors.successDialog.later')}
            </Button>
            <Button variant="solid" size="2" onClick={onStartSyncing} style={{ cursor: 'pointer' }}>
              {t('workspace.connectors.successDialog.startSync')}
            </Button>
          </Flex>
        </Flex>
      </Dialog.Content>
    </Dialog.Root>
  );
}
