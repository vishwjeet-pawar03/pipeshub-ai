'use client';

import { Button, DropdownMenu, Flex, Text } from '@radix-ui/themes';
import { useTranslation } from 'react-i18next';
import { MaterialIcon } from '@/app/components/ui/MaterialIcon';
import { useIsMobile } from '@/lib/hooks/use-is-mobile';

export interface ArtifactsPagination {
  page: number;
  limit: number;
  totalCount: number;
  totalPages: number;
}

export function PaginationFooter({
  pagination,
  onPageChange,
  onLimitChange,
}: {
  pagination: ArtifactsPagination;
  onPageChange: (page: number) => void;
  /** When set, a per-page selector is shown (hidden on phones to save room). */
  onLimitChange?: (limit: number) => void;
}) {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  const { page, limit, totalCount, totalPages } = pagination;
  const start = totalCount === 0 ? 0 : (page - 1) * limit + 1;
  const end = Math.min(page * limit, totalCount);

  return (
    <Flex
      justify="between"
      align="center"
      wrap="wrap"
      gap="3"
      style={{
        padding: 'var(--space-2) var(--space-4)',
        borderTop: '1px solid var(--olive-3)',
        background: 'var(--olive-2)',
        flexShrink: 0,
      }}
    >
      <Text size="1" style={{ color: 'var(--slate-10)' }}>
        {t('artifacts.showing', { start, end, total: totalCount })}
      </Text>
      <Flex gap="2" align="center">
        <Button variant="ghost" size="1" color="gray" disabled={page <= 1} onClick={() => onPageChange(page - 1)}>
          <MaterialIcon name="chevron_left" size={16} />
          {t('common.previous')}
        </Button>
        <Text size="1" weight="medium" style={{ minWidth: 48, textAlign: 'center', color: 'var(--slate-11)' }}>
          {t('artifacts.pageOf', { page, total: Math.max(totalPages, 1) })}
        </Text>
        <Button
          variant="ghost"
          size="1"
          color="gray"
          disabled={page >= totalPages}
          onClick={() => onPageChange(page + 1)}
        >
          {t('common.next')}
          <MaterialIcon name="chevron_right" size={16} />
        </Button>
        {onLimitChange && !isMobile && (
          <DropdownMenu.Root>
            <DropdownMenu.Trigger>
              <Button variant="soft" size="1" color="gray">
                {limit}
                <MaterialIcon name="expand_less" size={16} />
              </Button>
            </DropdownMenu.Trigger>
            <DropdownMenu.Content align="end" sideOffset={4}>
              {[10, 25, 50, 100].map((value) => (
                <DropdownMenu.Item key={value} onClick={() => onLimitChange(value)}>
                  {value} per page
                </DropdownMenu.Item>
              ))}
            </DropdownMenu.Content>
          </DropdownMenu.Root>
        )}
      </Flex>
    </Flex>
  );
}
