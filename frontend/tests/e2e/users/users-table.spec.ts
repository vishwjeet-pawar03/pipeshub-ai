import { test, expect } from '../fixtures/base.fixture';
import type { Page } from '@playwright/test';

/**
 * Choose a page size from the pagination control. The collapsed control shows only
 * the current number beside an expand icon; "N per page" exists only in the open menu.
 */
async function choosePageSize(page: Page, size: number): Promise<void> {
  const trigger = page
    .locator('span.material-icons-outlined')
    .filter({ hasText: 'expand_less' })
    .last();
  await expect(trigger, 'the page-size control should be shown').toBeVisible({ timeout: 10_000 });
  await trigger.click();
  await page.getByRole('menuitem', { name: `${size} per page` }).click();
}

/** Assert the pagination line reflects the chosen page size, then the row count as a backstop. */
async function expectPageSize(page: Page, size: number): Promise<void> {
  const showing = page.getByText(/^Showing \d+-\d+ of \d+/).first();
  await expect
    .poll(
      async () => {
        const match = ((await showing.textContent()) ?? '').match(/Showing (\d+)-(\d+) of (\d+)/);
        if (!match) return 'no pagination line';
        const [from, to, total] = match.slice(1).map(Number);
        return from === 1 && to === Math.min(size, total) ? 'ok' : `Showing ${from}-${to} of ${total}`;
      },
      { timeout: 10_000, message: `the list should show the first ${size} items` },
    )
    .toBe('ok');
  expect(await page.locator('[role="row"]').count()).toBeLessThanOrEqual(size);
}

test.describe('Users Table', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/workspace/users/');
    await page.waitForTimeout(3_000);
  });

  test('page loads with users table @smoke', async ({ page }) => {
    const heading = page.locator('text=/Users/i').first();
    await expect(heading).toBeVisible({ timeout: 5_000 });

    // Table rows should be present
    const rows = page.locator('[role="row"]');
    await expect(rows.first()).toBeVisible({ timeout: 10_000 });
  });

  test('displays pagination with showing count', async ({ page }) => {
    const showing = page.locator('text=/Showing/i').first();
    await expect(showing).toBeVisible({ timeout: 10_000 });
  });

  test('pagination: navigate to next page', async ({ page }) => {
    const showingText = await page.locator('text=/Showing/').first().textContent() ?? '';
    const match = showingText.match(/(\d+)\s*[-–]\s*(\d+)\s+.*?(\d+)/);
    const to = match ? parseInt(match[2], 10) : 0;
    const total = match ? parseInt(match[3], 10) : 0;

    if (to >= total) {
      test.skip(true, 'All items fit on one page — nothing to paginate');
      return;
    }

    const nextButton = page.locator('text="Next"').first();
    const showingBefore = showingText;
    await nextButton.click();
    await page.waitForTimeout(1_000);
    const showingAfter = await page.locator('text=/Showing/').first().textContent();
    expect(showingAfter).not.toBe(showingBefore);
  });

  test('pagination: navigate to previous page', async ({ page }) => {
    const showingText = await page.locator('text=/Showing/').first().textContent() ?? '';
    const match = showingText.match(/(\d+)\s*[-–]\s*(\d+)\s+.*?(\d+)/);
    const to = match ? parseInt(match[2], 10) : 0;
    const total = match ? parseInt(match[3], 10) : 0;

    if (to >= total) {
      test.skip(true, 'All items fit on one page — nothing to paginate');
      return;
    }

    // Go to page 2 first
    const nextButton = page.locator('text="Next"').first();
    await nextButton.click();
    await page.waitForTimeout(1_000);

    const prevButton = page.locator('text="Previous"').first();
    await prevButton.click();
    await page.waitForTimeout(1_000);

    const showing = await page.locator('text=/Showing/').first().textContent();
    expect(showing).toContain('1');
  });

  test('pagination: change limit to 25', async ({ page }) => {
    await choosePageSize(page, 25);
    await expectPageSize(page, 25);
  });

  test('pagination: change limit to 50', async ({ page }) => {
    await choosePageSize(page, 50);
    await expectPageSize(page, 50);
  });

  test('pagination: change limit to 100', async ({ page }) => {
    await choosePageSize(page, 100);
    await expectPageSize(page, 100);
  });

  test('search filters users by name/email', async ({ page }) => {
    const searchInput = page.locator('input[placeholder*="Search"]');
    await expect(searchInput).toBeVisible({ timeout: 10_000 });
    // The logged-in admin always exists, so searching for them must find them.
    const email = process.env.TEST_USER_EMAIL ?? '';
    expect(email, 'TEST_USER_EMAIL must be set for the e2e suite').not.toBe('');
    await searchInput.fill(email);

    const rows = page.locator('[role="row"]').filter({ hasText: email });
    await expect(rows.first()).toBeVisible({ timeout: 10_000 });
  });

  test('search with no results shows empty state', async ({ page }) => {
    const searchInput = page.locator('input[placeholder*="Search"]');
    await expect(searchInput).toBeVisible({ timeout: 10_000 });
    await searchInput.fill('zzz-nonexistent-user-zzz');
    await page.waitForTimeout(1_000);

    const rows = page.locator('[role="row"]');
    const count = await rows.count();
    expect(count).toBe(0);
  });

  test('clear search shows all users again', async ({ page }) => {
    const searchInput = page.locator('input[placeholder*="Search"]');
    await expect(searchInput).toBeVisible({ timeout: 10_000 });
    await searchInput.fill('e2e-user');
    await page.waitForTimeout(1_000);
    const filteredCount = await page.locator('[role="row"]').count();

    await searchInput.clear();
    await page.waitForTimeout(1_000);
    const allCount = await page.locator('[role="row"]').count();

    expect(allCount).toBeGreaterThanOrEqual(filteredCount);
  });
});
