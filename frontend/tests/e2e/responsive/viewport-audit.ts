import { request, type Browser, type Page, type TestInfo } from '@playwright/test';
import * as fs from 'fs';
import * as path from 'path';
import { test, expect } from '../fixtures/base.fixture';
import { apiBaseURL } from '../fixtures/api-context.fixture';
import { seedResponsiveData, type SeedIds } from './seed-responsive';

export type { SeedIds };

export const VIEWPORTS = [
  { name: 'mobile', width: 390, height: 844, isMobile: true, hasTouch: true },
  { name: 'tablet', width: 820, height: 1180, isMobile: true, hasTouch: true },
  { name: 'desktop', width: 1440, height: 900, isMobile: false, hasTouch: false },
] as const;

export interface AuditRoute {
  id: string;
  path: string | ((seed: SeedIds) => string);
  action?: (page: Page) => Promise<void>;
  /** Route mocks to install before the page loads. */
  setup?: (page: Page, seed: SeedIds) => Promise<void>;
  /** A reason to skip, when the audit stack cannot show this route. */
  skip?: (seed: SeedIds) => string | undefined;
}

const SETTLE_MS = 800;
// Actions look for one control; a missing one is a finding, not a 15 s wait.
export const ACTION_TIMEOUT = 5_000;

/**
 * Seed once per run, not once per worker: the first worker takes the lock and
 * the others wait for its result. The output dir is emptied at the start of
 * every run, so a lock from an earlier run never blocks this one.
 */
/**
 * The bearer token the signed-in app sends. Read from its own requests, because
 * where the app keeps its tokens in the browser storage differs between builds.
 */
async function appAccessToken(browser: Browser, testInfo: TestInfo): Promise<string> {
  const { baseURL, storageState } = testInfo.project.use;
  const context = await browser.newContext({ baseURL, storageState });
  try {
    const page = await context.newPage();
    const signed = page.waitForRequest((r) => r.url().includes('/api/v1/') && !!r.headers().authorization, {
      timeout: 30_000,
    });
    await page.goto('/chat/');
    return (await signed).headers().authorization.replace(/^Bearer /, '');
  } finally {
    await context.close();
  }
}

async function seedOnce(browser: Browser, testInfo: TestInfo): Promise<SeedIds> {
  const outputDir = path.dirname(testInfo.outputDir);
  const file = path.join(outputDir, 'responsive-seed.json');
  const lock = `${file}.lock`;
  fs.mkdirSync(outputDir, { recursive: true });
  try {
    fs.mkdirSync(lock);
  } catch {
    for (let i = 0; i < 240; i++) {
      if (fs.existsSync(file)) {
        const saved = JSON.parse(fs.readFileSync(file, 'utf-8')) as SeedIds | { error: string };
        if ('error' in saved) throw new Error(`seeding failed in another worker: ${saved.error}`);
        return saved;
      }
      await new Promise((r) => setTimeout(r, 500));
    }
    throw new Error('timed out waiting for another worker to seed the audit data');
  }
  const api = await request.newContext({
    baseURL: apiBaseURL(),
    extraHTTPHeaders: { Authorization: `Bearer ${await appAccessToken(browser, testInfo)}` },
  });
  try {
    const seed = await seedResponsiveData(api);
    fs.writeFileSync(file, JSON.stringify(seed));
    return seed;
  } catch (error) {
    fs.writeFileSync(file, JSON.stringify({ error: String(error) }));
    throw error;
  } finally {
    await api.dispose();
  }
}

/** Overflow and controls pushed past the viewport edges, measured in the page. */
function measure(): { docOverflowPx: number; offscreenInteractive: string[] } {
  const width = window.innerWidth;
  const scroller = document.scrollingElement ?? document.documentElement;
  const found = new Set<string>();
  const controls = document.querySelectorAll<HTMLElement>(
    'button, a[href], input, select, textarea, [role=button], [role=tab]',
  );
  for (const el of controls) {
    if (el.closest('[aria-hidden="true"]') || (el as HTMLInputElement).type === 'hidden') continue;
    if (!el.checkVisibility({ opacityProperty: true, visibilityProperty: true })) continue;
    const r = el.getBoundingClientRect();
    if (r.width === 0 || r.height === 0) continue;
    if (r.right <= width + 1 && r.left >= -1) continue;
    let scrollable = false;
    for (let p = el.parentElement; p && !scrollable; p = p.parentElement) {
      const ox = getComputedStyle(p).overflowX;
      scrollable = ox === 'auto' || ox === 'scroll';
    }
    if (scrollable) continue;
    found.add(el.getAttribute('aria-label') || el.textContent?.trim().slice(0, 40) || el.tagName);
  }
  return { docOverflowPx: scroller.scrollWidth - width, offscreenInteractive: [...found].sort() };
}

/** Opens the mobile navigation drawer when the page shows its hamburger. */
export async function openNavIfMobile(page: Page): Promise<void> {
  const hamburger = page.getByRole('button', { name: 'Open sidebar', exact: true });
  if (await hamburger.isVisible()) {
    await hamburger.click({ timeout: ACTION_TIMEOUT });
    await page.waitForTimeout(400);
  }
}

/**
 * Registers one test per route and viewport. Layout findings are soft, so one
 * run reports every screen; uncaught page errors fail the test.
 * With EVIDENCE_DIR set, each test saves `<id>--<viewport>.png` there and
 * appends its numbers to `metrics.jsonl`.
 */
export function runViewportAudit(routes: AuditRoute[]): void {
  let seed: SeedIds;

  test.beforeAll(async ({ browser }, testInfo) => {
    seed = await seedOnce(browser, testInfo);
  });

  for (const vp of VIEWPORTS) {
    test.describe(vp.name, () => {
      test.use({
        viewport: { width: vp.width, height: vp.height },
        isMobile: vp.isMobile,
        hasTouch: vp.hasTouch,
        deviceScaleFactor: 1,
      });

      for (const route of routes) {
        test(route.id, async ({ page }) => {
          const skipReason = route.skip?.(seed);
          test.skip(!!skipReason, skipReason);

          const pageErrors: string[] = [];
          page.on('pageerror', (error) => pageErrors.push(error.message));

          await route.setup?.(page, seed);
          await page.goto(typeof route.path === 'function' ? route.path(seed) : route.path);
          // Long-lived requests (notification sockets, polling) can keep the
          // network busy, so idle is a best effort and the settle delay follows.
          await page.waitForLoadState('networkidle', { timeout: 15_000 }).catch(() => undefined);
          await page.waitForTimeout(SETTLE_MS);

          // Pages this stack does not serve (a feature turned off) say so instead of measuring Next's 404.
          const notFound = await page.getByText('This page could not be found.', { exact: true }).isVisible();
          test.skip(notFound, 'this stack answers 404 for the page (the feature is not enabled here)');

          let actionError: string | undefined;
          if (route.action) {
            try {
              await route.action(page);
            } catch (error) {
              actionError = (error instanceof Error ? error.message : String(error)).split('\n')[0];
            }
            await page.waitForTimeout(SETTLE_MS);
          }

          const { docOverflowPx, offscreenInteractive } = await page.evaluate(measure);

          const evidenceDir = process.env.EVIDENCE_DIR;
          if (evidenceDir) {
            fs.mkdirSync(evidenceDir, { recursive: true });
            await page.screenshot({ path: path.join(evidenceDir, `${route.id}--${vp.name}.png`), fullPage: true });
            const row = { id: route.id, viewport: vp.name, docOverflowPx, offscreenInteractive, pageErrors, actionError };
            fs.appendFileSync(path.join(evidenceDir, 'metrics.jsonl'), `${JSON.stringify(row)}\n`);
          }

          expect.soft(actionError, 'the scenario could not be completed').toBeUndefined();
          expect.soft(docOverflowPx, 'the page scrolls sideways').toBeLessThanOrEqual(0);
          expect.soft(offscreenInteractive, 'controls outside the viewport').toEqual([]);
          expect(pageErrors, 'uncaught errors in the page').toEqual([]);
        });
      }
    });
  }
}
