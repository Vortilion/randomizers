import { test, expect } from '@playwright/test';

test.describe('Second Edition App', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/gwt-2nd-edition');
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

  test('should display player count selector', async ({ page }) => {
    const heading = page.locator('h3').filter({ hasText: /Number of players/i });
    await expect(heading).toBeVisible();

    const select = page.locator('mat-select');
    await expect(select).toBeVisible();
  });

  test('should display variant options section', async ({ page }) => {
    const heading = page.locator('h3').filter({ hasText: /Variant/i });
    await expect(heading).toBeVisible();
  });

  test('should display simmental toggle', async ({ page }) => {
    const toggle = page.locator('mat-slide-toggle').filter({ hasText: /Simmental/i });
    await expect(toggle).toBeVisible();
  });

  test('should display brahman toggle', async ({ page }) => {
    const toggle = page.locator('mat-slide-toggle').filter({ hasText: /Brahman/i });
    await expect(toggle).toBeVisible();
  });

  test('should display rails to the north toggle', async ({ page }) => {
    const toggle = page.locator('mat-slide-toggle').filter({ hasText: /Rails To The North/i });
    await expect(toggle).toBeVisible();
  });

  test('should display extension section', async ({ page }) => {
    const heading = page.locator('h3').filter({ hasText: /Extension/i });
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

  test('should randomize setup on button click', async ({ page }) => {
    const button = page.locator('button').filter({ hasText: /Setup/i }).first();
    await expect(button).toBeVisible();

    // Get initial content
    const contentBefore = await page.locator('mat-toolbar').textContent();

    // Click randomize button
    await button.click();

    // Wait for state update
    await page.waitForTimeout(500);

    // Button should still be visible
    await expect(button).toBeVisible();
  });

  test('should toggle simmental variant', async ({ page }) => {
    const toggle = page.locator('mat-slide-toggle').filter({ hasText: /Simmental/i });
    const checkbox = toggle.locator('input');

    const checkedBefore = await checkbox.isChecked();
    await toggle.click();
    const checkedAfter = await checkbox.isChecked();

    expect(checkedBefore).not.toBe(checkedAfter);
  });

  test('should toggle brahman variant', async ({ page }) => {
    const toggle = page.locator('mat-slide-toggle').filter({ hasText: /Brahman/i });
    const checkbox = toggle.locator('input');

    const checkedBefore = await checkbox.isChecked();
    await toggle.click();
    const checkedAfter = await checkbox.isChecked();

    expect(checkedBefore).not.toBe(checkedAfter);
  });

  test('should change player count via dropdown', async ({ page }) => {
    const select = page.locator('mat-select');
    await select.click();

    // Select 3 players option
    const option = page.locator('mat-option').filter({ hasText: /3/ }).first();
    await expect(option).toBeVisible();
    await option.click();

    // Verify selection closed
    await expect(page.locator('mat-option')).toHaveCount(0);
  });

  test('should navigate to dashboard', async ({ page }) => {
    const dashboardLink = page.locator('a[aria-label="Randomizers dashboard"]');
    await expect(dashboardLink).toBeVisible();
    await dashboardLink.click();
    await expect(page).toHaveURL('/');
  });

  test('should display setup steps section', async ({ page }) => {
    const heading = page.locator('h3').filter({ hasText: /Further setup steps/i });
    await expect(heading).toBeVisible();
  });

  test('should display page footer', async ({ page }) => {
    const footer = page.locator('app-page-footer');
    await expect(footer).toBeVisible();
  });
});
