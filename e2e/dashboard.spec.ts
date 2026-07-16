import { test, expect } from '@playwright/test';

test.describe('Dashboard Page', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should display dashboard title and subtitle', async ({ page }) => {
    await expect(page.locator('h1')).toContainText('Great Western Trail Randomizers');
    await expect(page.locator('p.dashboard__subtitle')).toBeVisible();
  });

  test('should display two randomizer cards', async ({ page }) => {
    const cards = page.locator('mat-card.dashboard-card');
    await expect(cards).toHaveCount(2);
  });

  test('should have argentina card with correct content', async ({ page }) => {
    const argentineCard = page.locator('mat-card-title').filter({ hasText: /argentina/i }).first();
    await expect(argentineCard).toBeVisible();

    const link = page.locator('a[routerLink="/gwt-argentina"]');
    await expect(link).toBeVisible();
  });

  test('should have second edition card with correct content', async ({ page }) => {
    const secondEditionCard = page.locator('mat-card-title').filter({ hasText: /second.edition/i }).first();
    await expect(secondEditionCard).toBeVisible();

    const link = page.locator('a[routerLink="/gwt-2nd-edition"]');
    await expect(link).toBeVisible();
  });

  test('should navigate to argentina app', async ({ page }) => {
    await page.locator('a[routerLink="/gwt-argentina"]').click();
    await expect(page).toHaveURL('/gwt-argentina');
    await expect(page.locator('app-gwt-argentina')).toBeVisible();
  });

  test('should navigate to second edition app', async ({ page }) => {
    await page.locator('a[routerLink="/gwt-2nd-edition"]').click();
    await expect(page).toHaveURL('/gwt-2nd-edition');
    await expect(page.locator('app-gwt-second-edition')).toBeVisible();
  });

  test('should display language selector', async ({ page }) => {
    const languageButton = page.locator('button[aria-label="Language Selection"]');
    await expect(languageButton).toBeVisible();
  });

  test('should display footer', async ({ page }) => {
    const footer = page.locator('app-page-footer');
    await expect(footer).toBeVisible();
  });
});
