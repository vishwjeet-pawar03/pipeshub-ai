import React from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { Theme } from '@radix-ui/themes';
import '@/lib/__tests__/test-i18n';
import { AgentChatHeader } from '../agent-chat-header';

vi.mock('next/navigation', () => ({ useRouter: () => ({ push: vi.fn() }) }));

// The open menu measures itself; jsdom has no ResizeObserver.
globalThis.ResizeObserver ??= class {
  observe() {}
  unobserve() {}
  disconnect() {}
} as unknown as typeof ResizeObserver;

function renderHeader(props: Partial<React.ComponentProps<typeof AgentChatHeader>>) {
  return render(
    <Theme>
      <AgentChatHeader agentId="a1" displayName="Support bot" isMobile={false} {...props} />
    </Theme>,
  );
}

function openMenu() {
  const trigger = screen.getByRole('button', { name: /agent/i });
  fireEvent.keyDown(trigger, { key: 'Enter' });
  return trigger;
}

describe('AgentChatHeader', () => {
  afterEach(() => cleanup());

  it('lists the agent creator in the menu on a phone', () => {
    renderHeader({ isMobile: true, creatorName: 'Ada' });
    openMenu();

    expect(screen.getByText('Created by: Ada')).toBeTruthy();
  });

  it('keeps the creator out of the menu on desktop, where the chip shows it', () => {
    renderHeader({ creatorName: 'Ada' });
    openMenu();

    expect(screen.queryByText('Created by: Ada')).toBeNull();
  });

  it('stops the mobile title short of the share group', () => {
    const { container } = renderHeader({ isMobile: true, hasShareGroup: true });

    expect((container.querySelector('[style*="absolute"]') as HTMLElement).style.maxWidth).toBe(
      'calc(100vw - 52px - 200px)',
    );
  });
});
