import React from 'react';
import { describe, it, expect, afterEach, vi } from 'vitest';
import { render, screen, cleanup, fireEvent } from '@testing-library/react';
import { Theme } from '@radix-ui/themes';

import en from '@/lib/i18n/locales/en-US.json';
import { DemoSuggestions } from '../demo-suggestions';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string, opts?: { returnObjects?: boolean }) => {
      let cur: unknown = en;
      for (const part of key.split('.')) {
        if (typeof cur !== 'object' || cur === null || !(part in cur)) return key;
        cur = (cur as Record<string, unknown>)[part];
      }
      if (opts?.returnObjects) return cur;
      return typeof cur === 'string' ? cur : key;
    },
  }),
}));

afterEach(() => cleanup());

const questions = Object.values(en.chat.demoSuggestions).map((q) => q.text);

describe('DemoSuggestions', () => {
  it('offers every golden question and explains why the pricing one can come back empty', () => {
    render(
      <Theme>
        <DemoSuggestions isAdmin isMobile={false} onPick={() => {}} />
      </Theme>,
    );

    for (const q of questions) expect(screen.getByText(q)).toBeTruthy();
    expect(screen.getByText(en.chat.demoPricingHint)).toBeTruthy();
  });

  it('asks the question that was clicked', () => {
    const onPick = vi.fn();
    render(
      <Theme>
        <DemoSuggestions isAdmin={false} isMobile={false} onPick={onPick} />
      </Theme>,
    );

    fireEvent.click(screen.getByText(questions[4]));

    expect(onPick).toHaveBeenCalledWith(expect.objectContaining({ text: questions[4] }));
  });
});
