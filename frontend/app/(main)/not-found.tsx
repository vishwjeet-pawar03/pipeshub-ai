'use client';

import Link from 'next/link';
import { Button, Flex, Heading, Text } from '@radix-ui/themes';
import { useTranslation } from 'react-i18next';

export default function NotFound() {
  const { t } = useTranslation();
  return (
    <Flex
      direction="column"
      align="center"
      justify="center"
      gap="3"
      className="viewport-height"
      style={{ padding: 'var(--space-5)', textAlign: 'center' }}
    >
      <Heading size="5">{t('common.errorBoundary.notFoundTitle')}</Heading>
      <Text size="2" color="gray">{t('common.errorBoundary.notFoundBody')}</Text>
      <Button size="3" asChild>
        <Link href="/chat/">{t('common.errorBoundary.goToChat')}</Link>
      </Button>
    </Flex>
  );
}
