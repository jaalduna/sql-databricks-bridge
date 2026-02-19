// e2e-playwright-test.js
const { chromium } = require('./frontend/node_modules/playwright');
const path = require('path');
const fs = require('fs');

const SCREENSHOT_DIR = path.join('C:', 'Users', '70088287', 'Documents', 'Projects', 'sql-databricks-bridge', 'debug-screenshots');
const BASE_URL = 'http://localhost:5173/sql-databricks-bridge/';

fs.mkdirSync(SCREENSHOT_DIR, { recursive: true });
function ssp(name) { return path.join(SCREENSHOT_DIR, name); }

(async () => {
  console.log('=== SQL-Databricks Bridge E2E Test ===\n');

  const browser = await chromium.launch({
    headless: true,
    args: ['--no-proxy-server'],
  });
  const context = await browser.newContext({ viewport: { width: 1280, height: 900 } });
  const page = await context.newPage();

  const consoleMessages = [];
  const networkErrors = [];
  const failedResponses = [];

  page.on('console', (msg) => {
    consoleMessages.push('[' + msg.type() + '] ' + msg.text());
    if (msg.type() === 'error') console.log('  CONSOLE ERROR:', msg.text());
  });
  page.on('pageerror', (err) => {
    consoleMessages.push('[pageerror] ' + err.message);
    console.log('  PAGE ERROR:', err.message);
  });
  page.on('requestfailed', (req) => {
    const entry = req.method() + ' ' + req.url() + ' - ' + (req.failure() ? req.failure().errorText : 'unknown');
    networkErrors.push(entry);
    console.log('  NETWORK FAIL:', entry);
  });

  page.on('response', (response) => {
    if (response.status() >= 400) {
      failedResponses.push(response.status() + ' ' + response.url());
      console.log('  HTTP ' + response.status() + ':', response.url());
    }
  });

  let stagesResp = null;
  let countriesResp = null;
  let dataAvailResp = null;

  page.on('response', async (response) => {
    const url = response.url();
    try {
      if (url.includes('/metadata/stages')) {
        stagesResp = { status: response.status(), body: await response.json() };
      }
      if (url.includes('/metadata/countries')) {
        countriesResp = { status: response.status(), body: await response.json() };
      }
      if (url.includes('/metadata/data-availability')) {
        dataAvailResp = { status: response.status(), body: await response.json() };
      }
    } catch (e) {}
  });

  try {
    // Step 1: Login page
    console.log('Step 1: Navigating to login page...');
    await page.goto(BASE_URL, { waitUntil: 'networkidle', timeout: 15000 });
    await page.screenshot({ path: ssp('01-login-page.png'), fullPage: true });
    console.log('  Screenshot saved: 01-login-page.png');

    // Step 2: Sign in
    console.log('\nStep 2: Clicking Sign in with Microsoft...');
    const btn = page.getByRole('button', { name: 'Sign in with Microsoft' });
    await btn.waitFor({ state: 'visible', timeout: 5000 });
    await btn.click();
    await page.waitForURL('**/dashboard', { timeout: 10000 });
    console.log('  Navigated to:', page.url());
    await page.waitForSelector('h1', { timeout: 10000 });
    await page.waitForTimeout(3000);

    // Step 3: Dashboard screenshot
    await page.screenshot({ path: ssp('02-dashboard.png'), fullPage: true });
    console.log('  Screenshot saved: 02-dashboard.png');

    // Step 4: Open Stage dropdown
    console.log('\nStep 3: Opening Stage dropdown...');
    const stageTrigger = page.locator('#stage');
    await stageTrigger.waitFor({ state: 'visible', timeout: 5000 });
    await stageTrigger.click();
    await page.waitForTimeout(1000);
    await page.screenshot({ path: ssp('03-stage-dropdown-open.png'), fullPage: true });
    console.log('  Screenshot saved: 03-stage-dropdown-open.png');

    const stageOpts = page.locator('[role="option"]');
    const stageCount = await stageOpts.count();
    console.log('  Stage dropdown options visible:', stageCount);
    for (let i = 0; i < stageCount; i++) {
      console.log('    - ' + await stageOpts.nth(i).textContent());
    }
    await page.keyboard.press('Escape');
    await page.waitForTimeout(300);

    // Step 5: Log stages API
    console.log('\nStep 4: /metadata/stages API response:');
    if (stagesResp) {
      console.log('  Status:', stagesResp.status);
      console.log('  Body:', JSON.stringify(stagesResp.body, null, 2));
    } else {
      console.log('  WARNING: No response captured');
    }

    // Step 6: Navigate to calibration
    console.log('\nStep 5: Navigating to Calibration page...');
    const calLink = page.getByRole('link', { name: /Calibraci/ });
    const calVisible = await calLink.isVisible().catch(() => false);
    if (calVisible) {
      await calLink.click();
      await page.waitForURL('**/calibration', { timeout: 10000 });
    } else {
      console.log('  Nav link not visible, navigating directly...');
      await page.goto('http://localhost:5173/sql-databricks-bridge/calibration', { waitUntil: 'networkidle', timeout: 10000 });
    }
    console.log('  Navigated to:', page.url());
    await page.waitForSelector('h1', { timeout: 10000 });
    await page.waitForTimeout(3000);

    // Step 7: Calibration screenshot
    await page.screenshot({ path: ssp('04-calibration-page.png'), fullPage: true });
    console.log('  Screenshot saved: 04-calibration-page.png');

    // Count country cards
    const calibrarBtns = page.getByRole('button', { name: 'Calibrar' });
    const cardCount = await calibrarBtns.count();
    console.log('  Country cards with Calibrar button:', cardCount);

    // Badge analysis
    console.log('\n  Data availability badge analysis:');
    const greenChecks = page.locator('svg.text-green-600');
    const redXs = page.locator('svg.text-red-500');
    console.log('    Green (available) badges:', await greenChecks.count());
    console.log('    Red (unavailable) badges:', await redXs.count());

    // Step 8: Log countries API
    console.log('\nStep 6: /metadata/countries API response:');
    if (countriesResp) {
      console.log('  Status:', countriesResp.status);
      console.log('  Body:', JSON.stringify(countriesResp.body, null, 2));
    } else {
      console.log('  WARNING: No response captured');
    }

    // Step 9: Log data-availability API
    console.log('\nStep 7: /metadata/data-availability API response:');
    if (dataAvailResp) {
      console.log('  Status:', dataAvailResp.status);
      console.log('  Body:', JSON.stringify(dataAvailResp.body, null, 2));
    } else {
      console.log('  WARNING: No response captured');
    }

    // Summary
    console.log('\n========== SUMMARY ==========');
    console.log('Total console messages:', consoleMessages.length);
    const errs = consoleMessages.filter((m) => m.startsWith('[error]') || m.startsWith('[pageerror]'));
    console.log('Console errors:', errs.length);
    errs.forEach((e) => console.log(' ', e));
    console.log('Network failures:', networkErrors.length);
    networkErrors.forEach((e) => console.log(' ', e));
    console.log('\nScreenshots saved to:', SCREENSHOT_DIR);

  } catch (err) {
    console.error('\nFATAL ERROR:', err.message);
    await page.screenshot({ path: ssp('99-error-state.png'), fullPage: true }).catch(() => {});
    console.log('  Error screenshot saved: 99-error-state.png');
  } finally {
    await browser.close();
    console.log('\nDone.');
  }
})();
