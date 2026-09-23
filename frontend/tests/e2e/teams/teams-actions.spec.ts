import type { APIRequestContext, Page } from '@playwright/test';
import { test, expect } from '../fixtures/api-context.fixture';
import { postWithRetry } from '../helpers/api-retry.helper';
import { getRows, waitForTableLoaded } from '../helpers/entity-table.helper';

// Each test creates the team it acts on, so it no longer depends on another
// spec having created "E2E Test Create Team" first.
const created: string[] = [];

async function createTeam(apiContext: APIRequestContext, label: string): Promise<string> {
  const name = `E2E Team Actions ${label} ${Date.now()}`;
  const response = await postWithRetry(apiContext, '/api/v1/teams', { name });
  if (!response.ok()) {
    throw new Error(`POST /api/v1/teams failed [${response.status()}]: ${await response.text()}`);
  }
  const data = await response.json();
  // The API answers {status, message, data: {id, ...}}, so the id is under `data`.
  const id: string | undefined =
    data.data?.id ?? data.data?._key ?? data.team?.id ?? data.id ?? data.team?._key ?? data._key;
  // Without an id the team could never be cleaned up.
  if (!id) throw new Error(`POST /api/v1/teams returned no id: ${JSON.stringify(data)}`);
  created.push(id);
  return name;
}

async function openTeam(page: Page, name: string): Promise<void> {
  await page.goto('/workspace/teams/');
  await page.locator('input[placeholder*="Search"]').first().fill(name);
  const row = getRows(page).filter({ hasText: name }).first();
  await expect(row, `team "${name}" should be listed`).toBeVisible({ timeout: 15_000 });
  await row.click();
}

test.describe('Teams Actions', () => {
  test.afterAll(async ({ apiContext }) => {
    // 404 is expected for the team the delete test already removed.
    for (const id of created) {
      const response = await apiContext.delete(`/api/v1/teams/${id}`);
      if (!response.ok() && response.status() !== 404) {
        throw new Error(`cleanup of team ${id} failed [${response.status()}]: ${await response.text()}`);
      }
    }
  });

  test('clicking a row opens team detail', async ({ page, apiContext }) => {
    const name = await createTeam(apiContext, 'detail');
    await page.goto('/workspace/teams/');
    await waitForTableLoaded(page, 15_000);
    await openTeam(page, name);

    await expect
      .poll(
        async () =>
          page.url().includes('panel=detail') ||
          page.url().includes('teamId=') ||
          (await page
            .locator('[data-side-panel], [role="complementary"]')
            .first()
            .isVisible()
            .catch(() => false)),
        { timeout: 5_000 },
      )
      .toBe(true);
  });

  test('edit team name', async ({ page, apiContext }) => {
    const name = await createTeam(apiContext, 'edit');
    const renamed = `${name} Updated`;
    await openTeam(page, name);

    await page.getByRole('button', { name: 'Edit Team' }).click();
    const nameInput = page.locator(`input[value="${name}"]`).first();
    await expect(nameInput).toBeEditable({ timeout: 5_000 });
    await nameInput.fill(renamed);
    await page.getByRole('button', { name: 'Save Edits' }).click();

    await expect(page.getByText('Team updated!').first()).toBeVisible({ timeout: 10_000 });
    await page.goto('/workspace/teams/');
    const search = page.locator('input[placeholder*="Search"]').first();
    await search.fill(renamed);

    // Searching the old name on failure, because "not listed under the new
    // name" has two very different causes and the assertion alone cannot say
    // which: the save reported success but did not change the name, or the
    // team stopped matching search altogether. One is a save bug and the other
    // a listing bug, and a nightly failure that names neither sends whoever
    // picks it up to read the wrong code.
    const renamedRow = getRows(page).filter({ hasText: renamed }).first();
    try {
      await expect(renamedRow).toBeVisible({ timeout: 15_000 });
    } catch (failure) {
      await search.fill(name);
      // Waits, and excludes the new name. `isVisible` returns at once, while the
      // list only refetches when the search text changes -- so an immediate read
      // sees the previous empty state and would report "stopped matching search"
      // every time, including when the opposite is true. And `hasText` is a
      // substring: the renamed team still contains the original name as a
      // prefix, so a row showing the new name would count as the old one.
      const underOldName = await getRows(page)
        .filter({ hasText: name, hasNotText: renamed })
        .first()
        .waitFor({ state: 'visible', timeout: 15_000 })
        .then(() => true)
        .catch(() => false);
      throw new Error(
        `the team is not listed as "${renamed}" after a save that reported success. ` +
          (underOldName
            ? `It is still listed under its old name, so the save did not change the name.`
            : `It is not listed under its old name either, so it has stopped matching search.`) +
          `\n\nOriginal failure: ${(failure as Error).message}`
      );
    }
  });

  test('delete team', async ({ page, apiContext }) => {
    const name = await createTeam(apiContext, 'delete');
    await openTeam(page, name);

    // Delete is offered only in edit mode.
    await page.getByRole('button', { name: 'Edit Team' }).click();
    await page.getByRole('button', { name: 'Delete Team' }).click();

    await expect(page.getByText('Team deleted').first()).toBeVisible({ timeout: 10_000 });
    await page.goto('/workspace/teams/');
    await page.locator('input[placeholder*="Search"]').first().fill(name);
    await expect(getRows(page).filter({ hasText: name })).toHaveCount(0, { timeout: 15_000 });
  });
});
