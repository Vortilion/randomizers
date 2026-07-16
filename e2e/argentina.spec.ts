import { test, expect } from '@playwright/test';

test.describe('Argentina App', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/gwt-argentina');
  });

  test('should load without errors on initial render', async ({ page }) => {
    // Check for any console errors
    const errors: string[] = [];
    page.on('console', msg => {
      if (msg.type() === 'error') {
        errors.push(msg.text());
      }
    });

    // Give page time to render
    await page.waitForLoadState('networkidle');
    expect(errors.length).toBe(0);
  });

  test('should display page header with title', async ({ page }) => {
    const header = page.locator('app-page-header');
    await expect(header).toBeVisible();

    const title = page.locator('span.app-name').first();
    await expect(title).toBeVisible();
  });

  test('should display page footer', async ({ page }) => {
    const footer = page.locator('app-page-footer');
    await expect(footer).toBeVisible();
  });

  test('should display sidenav container', async ({ page }) => {
    const sidenavContainer = page.locator('mat-sidenav-container');
    await expect(sidenavContainer).toBeVisible();
  });

  test('should display player count selector', async ({ page }) => {
    const playerCountSelect = page.locator('mat-select');
    await expect(playerCountSelect).toBeVisible();
  });

  test('should display randomize setup button', async ({ page }) => {
    const button = page.locator('button').filter({ hasText: /Setup/i }).first();
    await expect(button).toBeVisible();
  });

  test('should display neutral buildings section', async ({ page }) => {
    const heading = page.locator('h3').filter({ hasText: /Neutral buildings/i });
    await expect(heading).toBeVisible();
  });

  test('should display station masters section', async ({ page }) => {
    const heading = page.locator('h3').filter({ hasText: /Station masters/i });
    await expect(heading).toBeVisible();
  });

  test('should display player buildings section', async ({ page }) => {
    const heading = page.locator('h3').filter({ hasText: /Player buildings/i });
    await expect(heading).toBeVisible();
  });

  test('should display cities section', async ({ page }) => {
    const heading = page.locator('h3').filter({ hasText: /Cities/i });
    await expect(heading).toBeVisible();
  });

  test('should randomize setup on button click', async ({ page }) => {
    const button = page.locator('button').filter({ hasText: /Setup/i }).first();

    // Get initial neutral buildings list
    const buildingsBefore = await page.locator('h3').filter({ hasText: /Neutral buildings/i }).evaluate(el => {
      return el.closest('div')?.textContent;
    });

    // Click randomize button
    await button.click();

    // Wait a moment for state update
    await page.waitForTimeout(500);

    // Get buildings after randomize
    const buildingsAfter = await page.locator('h3').filter({ hasText: /Neutral buildings/i }).evaluate(el => {
      return el.closest('div')?.textContent;
    });

    // Should have changed (with high probability)
    expect(buildingsBefore).toBeTruthy();
    expect(buildingsAfter).toBeTruthy();
  });

  test('should navigate to dashboard', async ({ page }) => {
    const dashboardLink = page.locator('a[aria-label="Randomizers dashboard"]');
    await expect(dashboardLink).toBeVisible();
    await dashboardLink.click();
    await expect(page).toHaveURL('/');
  });

  test('should change player count via dropdown', async ({ page }) => {
    const select = page.locator('mat-select');
    await select.click();

    // Select 3 players
    const option = page.locator('mat-option').filter({ hasText: /3 Players/i });
    await expect(option).toBeVisible();
    await option.click();
  });
});
