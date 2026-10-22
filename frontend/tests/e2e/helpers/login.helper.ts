import type { Page } from '@playwright/test';

/**
 * Some login forms ask for the email first and show the password field only
 * after Continue. Click Continue when the password field is not there yet.
 */
export async function revealPasswordField(page: Page): Promise<void> {
  const password = page.locator('#password-field');
  const next = page.getByRole('button', { name: 'Continue', exact: true });
  await password.or(next).first().waitFor({ timeout: 15_000 });
  if (!(await password.isVisible())) await next.click();
}

/**
 * Perform a full login flow via the browser UI.
 * Useful for unauthenticated test suites that need a fresh login.
 */
export async function loginViaUI(
  page: Page,
  email: string,
  password: string
): Promise<void> {
  await page.goto('/login/');
  await page.waitForSelector('#email-field', { timeout: 15_000 });
  await page.fill('#email-field', email);
  await revealPasswordField(page);
  await page.fill('#password-field', password);
  await page.click('button[type="submit"]');
  await page.waitForURL('**/chat/**', { timeout: 15_000 });
}
