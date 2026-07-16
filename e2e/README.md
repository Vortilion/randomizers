# E2E Tests with Playwright

This directory contains end-to-end tests for the Randomizers application using Playwright.

## Setup

### 1. Install Playwright

```bash
npm install -D @playwright/test
```

### 2. Install Browsers (optional for initial setup)

```bash
npx playwright install
```

## Running Tests

### Run All Tests

```bash
npx playwright test
```

### Run Tests in Headed Mode (see the browser)

```bash
npx playwright test --headed
```

### Run Specific Test File

```bash
npx playwright test e2e/dashboard.spec.ts
```

### Run Tests in Debug Mode

```bash
npx playwright test --debug
```

### Run Tests with UI Mode (interactive)

```bash
npx playwright test --ui
```

### Generate HTML Report

After running tests:

```bash
npx playwright show-report
```

## Test Files

- **dashboard.spec.ts** - Tests for the main dashboard page
  - Navigation to randomizer apps
  - Card display and content
  - Language selector visibility
  - Footer display

- **argentina.spec.ts** - Tests for the Argentina randomizer app
  - Initial render without errors
  - Page header and footer
  - Player count selector
  - Setup randomization
  - Navigation back to dashboard

- **second-edition.spec.ts** - Tests for the Second Edition randomizer app
  - Initial render without errors
  - Variant toggles (Simmental, Brahman)
  - Extension options
  - Player count selection
  - Setup randomization

- **ui.spec.ts** - UI/UX and accessibility tests
  - Responsive design (desktop, tablet, mobile)
  - Language selection functionality
  - Navigation flow between routes
  - Accessibility (heading hierarchy, link text, keyboard navigation)

## Configuration

The Playwright configuration is defined in `playwright.config.ts`:

- Runs on Chromium, Firefox, and WebKit
- Base URL: `http://localhost:4200`
- Auto-starts dev server in non-CI environments
- Takes screenshots on failures
- Records traces for debugging

## CI/CD Integration

The tests are configured to run with:
- Single worker in CI mode
- 2 retries on failure
- Screenshots and traces on failures

## Tips

- Tests assume the app is running on `http://localhost:4200`
- All selectors use accessible roles and labels where possible
- Tests are isolated and can run in any order
- Use `--headed` flag to watch tests execute in real-time
- Use `--debug` for step-by-step debugging

## Troubleshooting

### Tests fail with connection refused
Make sure the dev server is running:
```bash
npm start
```

### Tests fail to find elements
Check that selectors match the current HTML structure. Run tests in headed mode to see what's happening.

### Performance issues
Run tests with fewer workers:
```bash
npx playwright test --workers=1
```
