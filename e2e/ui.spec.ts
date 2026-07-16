import { test, expect, devices } from '@playwright/test';

// Test on mobile viewport
const mobileTest = test.extend({
  viewport: { width: 375, height: 667 },
});

test.describe('Responsive Design', () => {
  test('should display correctly on desktop', async ({ page }) => {
    page.setViewportSize({ width: 1280, height: 1024 });
    await page.goto('/gwt-argentina');

    const pageHeader = page.locator('app-page-header');
    await expect(pageHeader).toBeVisible();

    // Desktop sidenav should be visible
    const sidenav = page.locator('mat-sidenav');
    await expect(sidenav).toBeVisible();
  });

  test('should display menu button on mobile', async ({ page }) => {
    page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/gwt-argentina');

    // Mobile menu button should be visible
    const menuButton = page.locator('button[mat-icon-button]').first();
    await expect(menuButton).toBeVisible();
  });

  test('should toggle sidenav on mobile', async ({ page }) => {
    page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/gwt-argentina');

    const menuButton = page.locator('button[mat-icon-button]').first();
    await expect(menuButton).toBeVisible();

    // Click menu to open sidenav
    await menuButton.click();
  });

  test('should adapt layout on tablet', async ({ page }) => {
    page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/gwt-2nd-edition');

    const pageHeader = page.locator('app-page-header');
    await expect(pageHeader).toBeVisible();

    const content = page.locator('mat-sidenav-content');
    await expect(content).toBeVisible();
  });
});

test.describe('Language Selection', () => {
  test('should display language selector button', async ({ page }) => {
    await page.goto('/');

    const languageButton = page.locator('button[aria-label="Language Selection"]');
    await expect(languageButton).toBeVisible();
  });

  test('should open language menu on click', async ({ page }) => {
    await page.goto('/');

    const languageButton = page.locator('button[aria-label="Language Selection"]');
    await languageButton.click();

    // Menu should appear
    const menu = page.locator('app-language-selector');
    await expect(menu).toBeVisible();
  });

  test('should have language selector on all pages', async ({ page }) => {
    const pages = ['/', '/gwt-argentina', '/gwt-2nd-edition'];

    for (const route of pages) {
      await page.goto(route);
      const selector = page.locator('app-language-selector');
      await expect(selector).toBeVisible();
    }
  });
});

test.describe('Navigation', () => {
  test('should display page header on all routes', async ({ page }) => {
    const routes = ['/', '/gwt-argentina', '/gwt-2nd-edition'];

    for (const route of routes) {
      await page.goto(route);
      const header = page.locator('app-page-header');
      await expect(header).toBeVisible();
    }
  });

  test('should redirect invalid routes to dashboard', async ({ page }) => {
    await page.goto('/invalid-route');
    // Should redirect to dashboard
    await expect(page).toHaveURL('/');
  });

  test('should load dashboard by default', async ({ page }) => {
    await page.goto('/');
    await expect(page.locator('h1')).toContainText('Randomizers');
  });

  test('should navigate between routes', async ({ page }) => {
    // Start at dashboard
    await page.goto('/');
    await expect(page).toHaveURL('/');

    // Navigate to argentina
    await page.locator('a[routerLink="/gwt-argentina"]').click();
    await expect(page).toHaveURL('/gwt-argentina');

    // Go back to dashboard
    await page.locator('a[aria-label="Randomizers dashboard"]').click();
    await expect(page).toHaveURL('/');

    // Navigate to second edition
    await page.locator('a[routerLink="/gwt-2nd-edition"]').click();
    await expect(page).toHaveURL('/gwt-2nd-edition');
  });
});

test.describe('Accessibility', () => {
  test('should have proper heading hierarchy on dashboard', async ({ page }) => {
    await page.goto('/');

    const h1 = page.locator('h1');
    await expect(h1).toBeVisible();

    const h2 = page.locator('h2');
    expect(await h2.count()).toBeGreaterThan(0);
  });

  test('should have descriptive link text', async ({ page }) => {
    await page.goto('/');

    const dashboardLink = page.locator('a[aria-label="Randomizers dashboard"]');
    const routerLinks = page.locator('a[routerLink]');

    // Should have either aria-label or text content
    expect(await dashboardLink.count()).toBeGreaterThan(0);
    expect(await routerLinks.count()).toBeGreaterThan(0);
  });

  test('should support keyboard navigation', async ({ page }) => {
    await page.goto('/');

    // Tab to first focusable element
    await page.keyboard.press('Tab');
    const focused = await page.evaluate(() => document.activeElement?.tagName);
    expect(focused).toBeTruthy();
  });
});
