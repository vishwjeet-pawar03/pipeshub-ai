'use client';

import React from 'react';
import { Flex, Text, Link } from '@radix-ui/themes';
import { useTranslation } from 'react-i18next';
import { SuggestionChip } from './suggestion-chip';
import { ChatSuggestion } from '@/chat/types';
import { buildConnectorsUrl } from '@/app/(main)/workspace/connectors/utils/build-connectors-url';
import { useRestrictedQuestionAccess } from '@/app/(main)/workspace/connectors/demo-data/use-restricted-question';

interface DemoSuggestionsProps {
  isAdmin: boolean | null;
  isMobile: boolean;
  onPick: (suggestion: ChatSuggestion) => void;
}

/**
 * New-chat landing extras while the Acme Corp demo connector is active: the
 * questions the sample data was written to answer, and the way out of the
 * demo (connect a real source).
 */
export function DemoSuggestions({ isAdmin, isMobile, onPick }: DemoSuggestionsProps) {
  const { t } = useTranslation();
  const access = useRestrictedQuestionAccess();
  const map = t('chat.demoSuggestions', { returnObjects: true }) as Record<
    string,
    { text: string; icons: ChatSuggestion['icons']; restricted?: boolean }
  >;
  const suggestions = Object.entries(map).map(([id, item]) => ({
    id,
    text: item.text,
    icons: item.icons,
    // Only for someone who will get nothing back; the pricing committee sees a plain chip.
    locked: !!item.restricted && access?.canSee === false,
  }));
  const lockedHint = access?.readerEmail
    ? t('chat.demoRestrictedHintSignIn', { email: access.readerEmail })
    : t('chat.demoRestrictedHint');

  return (
    <Flex direction="column" align="center" gap="3" style={{ width: '100%', marginTop: 'var(--space-5)' }}>
      <Text size="1" style={{ color: 'var(--slate-11)', textAlign: 'center' }}>
        {t('chat.demoBanner')}{' '}
        <Link href={buildConnectorsUrl(isAdmin)} size="1" weight="medium">
          {t('chat.demoConnectYourOwn')}
        </Link>
      </Text>
      <Flex
        direction={isMobile ? 'column' : 'row'}
        wrap="wrap"
        justify="center"
        gap="2"
        style={{ width: '100%' }}
      >
        {suggestions.map(({ locked, ...s }) => {
          const chip = (
            <SuggestionChip
              key={s.id}
              text={s.text}
              icons={s.icons}
              fullWidth={isMobile}
              locked={locked}
              onClick={() => onPick(s)}
            />
          );
          if (!locked) return chip;
          // Said before the click: once asked, the answer alone reads as a failure.
          return (
            <Flex key={s.id} direction="column" align="center" gap="1" style={{ width: isMobile ? '100%' : undefined }}>
              {chip}
              <Text size="1" style={{ color: 'var(--slate-10)', textAlign: 'center', maxWidth: 420 }}>
                {lockedHint}
              </Text>
            </Flex>
          );
        })}
      </Flex>
    </Flex>
  );
}
