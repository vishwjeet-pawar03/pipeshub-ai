import type { Page } from '@playwright/test';
import { buildAguiConversation } from '../chat/agui-sse-builder';
import { ACTION_TIMEOUT, openNavIfMobile, type AuditRoute, type SeedIds } from './viewport-audit';
import { SEED_FILE_NAME } from './seed-responsive';

const CONV_ID = 'conv-responsive-audit-001';
const CONV_TITLE = 'Responsive audit conversation with a long title';
const MODEL_INFO = { modelKey: 'gpt-4o-mini', modelName: 'GPT-4o mini', chatMode: 'internal_search', modelFriendlyName: 'GPT-4o mini' };

/**
 * One stored conversation, shown in the sidebar list and opened by id, whose
 * answer cites the seeded record. Same wire shapes as chat-streaming.spec.ts.
 */
async function mockChat(page: Page, seed: SeedIds): Promise<void> {
  const base = buildAguiConversation({
    conversationId: CONV_ID,
    userMessageId: 'msg-responsive-user',
    botMessageId: 'msg-responsive-bot',
    question: CONV_TITLE,
    answer: 'The audit record is cited here [1]. A long unbroken token follows: responsive_audit_0123456789_0123456789_0123456789.',
    modelInfo: MODEL_INFO,
  });
  const now = new Date().toISOString();
  const citation = {
    citationId: 'cit-responsive-1',
    citationData: {
      _id: 'cit-responsive-1',
      content: 'Responsive audit record.',
      chunkIndex: 1,
      metadata: {
        recordName: SEED_FILE_NAME,
        recordId: seed.recordId,
        connector: 'KB',
        recordType: 'FILE',
        mimeType: 'text/plain',
        extension: 'txt',
        previewRenderable: true,
        origin: 'UPLOAD',
        orgId: 'org-e2e',
        recordVersion: 0,
      },
      citationType: 'vectordb|document',
      createdAt: now,
      updatedAt: now,
    },
  };
  const conversation = {
    ...base,
    messages: base.messages.map((m) => (m.messageType === 'bot_response' ? { ...m, citations: [citation] } : m)),
  };
  const listItem = { ...conversation, id: CONV_ID, isOwner: true, messages: undefined };

  await page.route('**/api/v1/configurationManager/ai-models/available/llm', (route) =>
    route.fulfill({
      json: { status: 'success', message: 'Success', models: [{ ...MODEL_INFO, modelType: 'chat', provider: 'openAI', isMultimodal: false, isReasoning: false, isDefault: true }] },
    }),
  );
  await page.route('**/api/v1/conversations*', (route) => {
    if (route.request().method() !== 'GET') return route.fallback();
    const owned = new URL(route.request().url()).searchParams.get('source') !== 'shared';
    const list = owned ? [listItem] : [];
    return route.fulfill({
      json: {
        conversations: list,
        source: owned ? 'owned' : 'shared',
        pagination: { page: 1, limit: 20, totalCount: list.length, totalPages: 1, hasNextPage: false, hasPrevPage: false },
      },
    });
  });
  await page.route(
    (url) => url.pathname.replace(/\/$/, '') === `/api/v1/conversations/${CONV_ID}`,
    (route) => {
      if (route.request().method() !== 'GET') return route.fallback();
      return route.fulfill({
        json: { conversation: { ...conversation, id: CONV_ID, access: { isOwner: true, accessLevel: 'OWNER' } }, filters: {}, meta: {} },
      });
    },
  );
  // The made-up user ids on the mocked conversation would otherwise come back 400.
  await page.route('**/api/v1/users/by-ids', (route) =>
    route.request().method() === 'POST' ? route.fulfill({ json: [] }) : route.fallback(),
  );
}

const CHAT_CONVERSATION = `/chat/?conversationId=${CONV_ID}`;

function icon(page: Page, name: string) {
  return page.locator('span.material-icons-outlined, span.material-icons', { hasText: new RegExp(`^${name}$`) });
}

const NO_AGENT = (seed: SeedIds) => (seed.agentId ? undefined : 'no AI model on the audit stack');

/** Leaf `/workspace/*` pages that render in this edition (index redirects left out). */
const WORKSPACE_PAGES = [
  'actions/personal',
  'actions/team',
  'ai-models',
  'archived-chats',
  'authentication',
  'bots',
  'connectors/personal',
  'connectors/team',
  'developer-settings/oauth2',
  'developer-settings/personal-access-tokens',
  'general',
  'groups',
  'labs',
  'mail',
  'mcp-servers/personal',
  'mcp-servers/team',
  'profile',
  'prompts',
  'service-accounts',
  'services',
  'skills/personal',
  'teams',
  'users',
  'web-search',
];

export const ROUTES: AuditRoute[] = [
  { id: 'chat', path: '/chat/' },
  { id: 'chat-conversation', path: CHAT_CONVERSATION, setup: mockChat },
  { id: 'chat-agent', path: (s) => `/chat/?agentId=${s.agentId}`, skip: NO_AGENT },
  { id: 'kb-collections', path: '/knowledge-base/' },
  { id: 'kb-collection', path: (s) => `/knowledge-base/?nodeType=app&nodeId=${s.kbId}` },
  { id: 'kb-all-records', path: '/knowledge-base/?view=all-records' },
  {
    id: 'kb-grid',
    path: (s) => `/knowledge-base/?nodeType=app&nodeId=${s.kbId}`,
    action: async (page) => {
      await page.getByRole('radio').filter({ has: icon(page, 'grid_on') }).click({ timeout: ACTION_TIMEOUT });
    },
  },
  { id: 'kb-recently-deleted', path: (s) => `/knowledge-base/recently-deleted/?kbId=${s.kbId}` },
  { id: 'artifacts', path: '/artifacts/' },
  { id: 'projects', path: '/projects/' },
  { id: 'agents', path: '/agents/' },
  { id: 'agents-new', path: '/agents/new/' },
  { id: 'agents-edit', path: (s) => `/agents/edit/?agentKey=${s.agentId}`, skip: NO_AGENT },
  { id: 'record', path: (s) => `/record/?recordId=${s.recordId}` },
  {
    id: 'onboarding',
    path: '/onboarding/?step=ai-model',
    // The org is already set up, so the page would redirect away; report it as
    // not set up (read-only: the status is never written).
    setup: async (page) => {
      await page.route('**/api/v1/org/onboarding-status', (route) =>
        route.request().method() === 'GET'
          ? route.fulfill({ json: { status: 'notConfigured' } })
          : route.abort(),
      );
    },
  },
  ...WORKSPACE_PAGES.map((p) => ({ id: `workspace-${p.replace(/\//g, '-')}`, path: `/workspace/${p}/` })),

  // Interaction scenarios
  {
    id: 'drawer-open',
    path: '/chat/',
    // Phones must have the hamburger; wider screens may show the sidebar inline instead.
    action: async (page) => {
      const hamburger = page.getByRole('button', { name: 'Open sidebar', exact: true });
      if ((page.viewportSize()?.width ?? 0) <= 768 || (await hamburger.isVisible())) {
        await hamburger.click({ timeout: ACTION_TIMEOUT });
      }
    },
  },
  {
    id: 'notifications-open',
    path: '/chat/',
    action: async (page) => {
      await openNavIfMobile(page);
      await page.getByRole('button', { name: /^Inbox/ }).click({ timeout: ACTION_TIMEOUT });
    },
  },
  {
    id: 'workspace-menu-appearance',
    path: '/chat/',
    action: async (page) => {
      await openNavIfMobile(page);
      await icon(page, 'keyboard_arrow_up').last().click({ timeout: ACTION_TIMEOUT });
      await page.getByText('Appearance').first().click({ timeout: ACTION_TIMEOUT });
    },
  },
  {
    id: 'kb-select-two',
    path: (s) => `/knowledge-base/?nodeType=app&nodeId=${s.kbId}`,
    action: async (page) => {
      const boxes = page.getByRole('checkbox').filter({ visible: true });
      await boxes.nth(1).waitFor({ timeout: ACTION_TIMEOUT }).catch(() => undefined);
      const count = await boxes.count();
      if (count < 2) throw new Error(`needs two selectable rows, found ${count} checkboxes`);
      // The last two are rows; the first may be select-all.
      await boxes.nth(count - 2).click({ timeout: ACTION_TIMEOUT });
      await boxes.nth(count - 1).click({ timeout: ACTION_TIMEOUT });
    },
  },
  {
    id: 'chat-row-menu',
    path: CHAT_CONVERSATION,
    setup: mockChat,
    action: async (page) => {
      await openNavIfMobile(page);
      const row = page.getByRole('link', { name: CONV_TITLE }).filter({ visible: true }).first();
      // Recents may start collapsed.
      if (!(await row.isVisible())) await page.getByText('Recents', { exact: true }).click({ timeout: ACTION_TIMEOUT });
      await row.waitFor({ timeout: ACTION_TIMEOUT });
      // A touch screen cannot hover, so there the menu button must already show.
      if (!(await page.evaluate(() => matchMedia('(hover: none)').matches))) await row.hover({ timeout: ACTION_TIMEOUT });
      await icon(page, 'more_horiz').filter({ visible: true }).first().click({ timeout: ACTION_TIMEOUT });
    },
  },
  {
    id: 'agent-palette',
    path: '/agents/new/',
    // The palette is open by default today. Where a control opens it (named
    // "...palette..."), tap it so the open palette is what gets measured.
    action: async (page) => {
      const opener = page.getByRole('button', { name: /palette/i }).filter({ visible: true });
      if (await opener.count()) await opener.first().click({ timeout: ACTION_TIMEOUT });
    },
  },
  {
    id: 'invite-panel',
    // Opened by URL: the Invite button stays disabled until the org has SMTP,
    // and the panel itself does not need it.
    path: '/workspace/users/?panel=invite',
  },
  {
    id: 'search-chats',
    path: '/chat/',
    action: async (page) => {
      await openNavIfMobile(page);
      await page.getByText('Search Chats', { exact: true }).click({ timeout: ACTION_TIMEOUT });
    },
  },
  {
    id: 'citation-preview',
    path: CHAT_CONVERSATION,
    setup: mockChat,
    action: async (page) => {
      await page.getByText('Citation', { exact: true }).last().click({ timeout: ACTION_TIMEOUT });
      await page.getByRole('button', { name: 'Preview', exact: true }).first().click({ timeout: ACTION_TIMEOUT });
    },
  },
];
