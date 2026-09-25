import React from 'react';
import { vi } from 'vitest';
import { act, render, screen, within } from '@testing-library/react';
import { Theme } from '@radix-ui/themes';
import type { KnowledgeHubApiResponse, KnowledgeHubNode, NodePermissions } from '../types';

export const OWNER_PERMISSIONS: NodePermissions = {
  role: 'OWNER',
  canUpload: true,
  canCreateFolders: true,
  canEdit: true,
  canDelete: true,
  canManagePermissions: true,
};

export function hubNode(
  fields: Partial<KnowledgeHubNode> & Pick<KnowledgeHubNode, 'id' | 'name'>,
): KnowledgeHubNode {
  return {
    nodeType: 'record',
    parentId: null,
    origin: 'COLLECTION',
    hasChildren: false,
    permission: { role: 'OWNER', canEdit: true, canDelete: true },
    sharingStatus: 'private',
    ...fields,
  };
}

export function collection(id: string, name: string, extra: Partial<KnowledgeHubNode> = {}) {
  return hubNode({ id, name, nodeType: 'app', connector: 'KB', hasChildren: true, ...extra });
}

export function hubResponse(
  items: KnowledgeHubNode[],
  extra: Partial<KnowledgeHubApiResponse> = {},
): KnowledgeHubApiResponse {
  return {
    success: true,
    error: null,
    id: null,
    currentNode: null,
    parentNode: null,
    items,
    pagination: {
      page: 1,
      limit: 50,
      totalItems: items.length,
      totalPages: 1,
      hasNext: false,
      hasPrev: false,
    },
    permissions: OWNER_PERMISSIONS,
    ...extra,
  };
}

/** The contents of one collection (or a folder in it), as the folder API returns it. */
export function folderResponse(
  current: { id: string; name: string; nodeType: 'app' | 'folder' },
  trail: { id: string; name: string; nodeType: string }[],
  items: KnowledgeHubNode[],
  extra: Partial<KnowledgeHubApiResponse> = {},
): KnowledgeHubApiResponse {
  return hubResponse(items, {
    currentNode: current,
    breadcrumbs: trail,
    ...extra,
  });
}

/** Fake router whose `push`/`replace` really change what `useSearchParams` returns. */
export function createNavigation() {
  let params = new URLSearchParams();
  const listeners = new Set<() => void>();
  const setUrl = (url: string) => {
    const query = url.includes('?') ? url.slice(url.indexOf('?') + 1) : '';
    params = new URLSearchParams(query);
    listeners.forEach((listener) => listener());
  };
  return {
    getParams: () => params,
    subscribe(listener: () => void) {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    setUrl,
    reset(url = '/knowledge-base') {
      params = new URLSearchParams(url.includes('?') ? url.slice(url.indexOf('?') + 1) : '');
    },
  };
}

export function renderInTheme(ui: React.ReactElement) {
  return render(<Theme>{ui}</Theme>);
}

export function row(name: string) {
  return screen.getByRole('row', { name });
}

export function queryRow(name: string) {
  return screen.queryByRole('row', { name });
}

/** Opens a row's "…" menu (Radix opens on keyboard as well as pointer). */
export async function openRowMenu(name: string) {
  const buttons = within(row(name)).getAllByRole('button');
  const trigger = buttons[buttons.length - 1];
  await act(async () => {
    trigger.focus();
    trigger.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true }));
  });
  return screen.getByRole('menu');
}

/** jsdom lacks the layout APIs the page and Radix menus call on mount. */
export function installBrowserShims() {
  if (!window.matchMedia) {
    window.matchMedia = (query: string) =>
      ({
        matches: false,
        media: query,
        onchange: null,
        addEventListener: () => {},
        removeEventListener: () => {},
        addListener: () => {},
        removeListener: () => {},
        dispatchEvent: () => false,
      }) as MediaQueryList;
  }
  if (!('ResizeObserver' in window)) {
    class NoopResizeObserver {
      observe() {}
      unobserve() {}
      disconnect() {}
    }
    Object.defineProperty(window, 'ResizeObserver', { value: NoopResizeObserver, configurable: true });
  }
  URL.createObjectURL = vi.fn(() => 'blob:preview');
  URL.revokeObjectURL = vi.fn();
  if (!Element.prototype.scrollIntoView) Element.prototype.scrollIntoView = () => {};
  if (!Element.prototype.hasPointerCapture) Element.prototype.hasPointerCapture = () => false;
  if (!Element.prototype.releasePointerCapture) Element.prototype.releasePointerCapture = () => {};
}
