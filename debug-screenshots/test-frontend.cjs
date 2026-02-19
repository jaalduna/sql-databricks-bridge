const { chromium } = require('playwright');
const path = require('path');

const SCREENSHOT_DIR = path.resolve(__dirname);
const BASE_URL = 'http://localhost:5173/sql-databricks-bridge/';

async function main() {
  console.log('=== Playwright Frontend Test ===\n');

  const browser = await chromium.launch({
    headless: true,
    args: [
      '--host-resolver-rules=MAP localhost 127.0.0.1',
      '--no-proxy-server',
    ],
  });

  const context = await browser.newContext({ viewport: { width: 1400, height: 900 } });
  const page = await context.newPage();

  const consoleErrors = [];
  page.on('console', (msg) => { if (msg.type() === 'error') consoleErrors.push(msg.text()); });

  const networkErrors = [];
  page.on('requestfailed', (req) => {
    networkErrors.push(req.method() + ' ' + req.url() + ' - ' + (req.failure() ? req.failure().errorText : 'unknown'));
  });

  const apiResponses = {};

  page.on('response', async (response) => {
    const url = response.url();
    try {
      if (url.includes('/metadata/stages')) {
        const body = await response.json().catch(() => null);
        apiResponses['stages'] = { status: response.status(), body };
        console.log('[API] /metadata/stages => status ' + response.status());
        console.log('  body: ' + JSON.stringify(body, null, 2));
      }
      if (url.includes('/metadata/countries')) {
        const body = await response.json().catch(() => null);
        apiResponses['countries'] = { status: response.status(), body };
        console.log('[API] /metadata/countries => status ' + response.status());
        console.log('  body: ' + JSON.stringify(body, null, 2));
      }
      if (url.includes('/metadata/data-availability')) {
        const body = await response.json().catch(() => null);
        apiResponses['data-availability'] = { status: response.status(), body };
        console.log('[API] /metadata/data-availability => status ' + response.status());
        console.log('  body: ' + JSON.stringify(body, null, 2));
      }
    } catch (e) {}
  });

  try {
    console.log('\n--- Step 1: Navigate to login page ---');
    await page.goto(BASE_URL, { waitUntil: 'networkidle', timeout: 15000 });
    await page.screenshot({ path: path.join(SCREENSHOT_DIR, '01-login-page.png'), fullPage: true });
    console.log('Screenshot: 01-login-page.png');
    const bodyText = await page.textContent('body');
    console.log('Login page has Sign in button: ' + bodyText.includes('Sign in with Microsoft'));

    console.log('\n--- Step 2: Click Sign in with Microsoft ---');
    await page.click('button:has-text("Sign in with Microsoft")');
    await page.waitForURL('**/dashboard**', { timeout: 10000 }).catch(() => {
      console.log('  (waitForURL to /dashboard timed out)');
    });
    await page.waitForLoadState('networkidle', { timeout: 10000 }).catch(() => {});
    await page.waitForTimeout(2000);
    await page.screenshot({ path: path.join(SCREENSHOT_DIR, '02-dashboard.png'), fullPage: true });
    console.log('Screenshot: 02-dashboard.png');
    console.log('Current URL: ' + page.url());

    console.log('\n--- Step 3: Open Stage dropdown ---');
    const stageTrigger = page.locator('#stage');
    const stageTriggerVisible = await stageTrigger.isVisible().catch(() => false);
    console.log('Stage trigger (#stage) visible: ' + stageTriggerVisible);
    if (stageTriggerVisible) {
      await stageTrigger.click();
      await page.waitForTimeout(500);
      await page.screenshot({ path: path.join(SCREENSHOT_DIR, '03-stage-dropdown-open.png'), fullPage: true });
      console.log('Screenshot: 03-stage-dropdown-open.png');
      const stageOptions = page.locator('[role="option"]');
      const stageCount = await stageOptions.count();
      console.log('Number of stage options in dropdown: ' + stageCount);
      for (let i = 0; i < stageCount; i++) {
        const text = await stageOptions.nth(i).textContent();
        console.log('  Stage option ' + (i + 1) + ': ' + text);
      }
      await page.keyboard.press('Escape');
      await page.waitForTimeout(300);
    } else {
      await page.screenshot({ path: path.join(SCREENSHOT_DIR, '03-stage-NOT-FOUND.png'), fullPage: true });
    }

    console.log('\n--- Step 4: Open Country dropdown ---');
    const countryTrigger = page.locator('#country');
    const countryTriggerVisible = await countryTrigger.isVisible().catch(() => false);
    console.log('Country trigger (#country) visible: ' + countryTriggerVisible);
    if (countryTriggerVisible) {
      await countryTrigger.click();
      await page.waitForTimeout(500);
      await page.screenshot({ path: path.join(SCREENSHOT_DIR, '04-country-dropdown-open.png'), fullPage: true });
      console.log('Screenshot: 04-country-dropdown-open.png');
      const countryOptions = page.locator('[role="option"]');
      const countryCount = await countryOptions.count();
      console.log('Number of country options in dropdown: ' + countryCount);
      for (let i = 0; i < countryCount; i++) {
        const text = await countryOptions.nth(i).textContent();
        console.log('  Country option ' + (i + 1) + ': ' + text);
      }
      await page.keyboard.press('Escape');
      await page.waitForTimeout(300);
    }

    console.log('\n--- Step 5: Navigate to Calibration page ---');
    const calLink = page.locator('a').filter({ hasText: 'Calibraci\u00f3n' });
    const calVisible = await calLink.isVisible().catch(() => false);
    console.log('Calibracion nav link visible: ' + calVisible);
    if (calVisible) {
      await calLink.click();
      await page.waitForURL('**/calibration**', { timeout: 10000 }).catch(() => {
        console.log('  (waitForURL to /calibration timed out)');
      });
      await page.waitForLoadState('networkidle', { timeout: 10000 }).catch(() => {});
      await page.waitForTimeout(2000);
      await page.screenshot({ path: path.join(SCREENSHOT_DIR, '05-calibration-page.png'), fullPage: true });
      console.log('Screenshot: 05-calibration-page.png');
      console.log('Current URL: ' + page.url());

      const calibrarBtns = page.locator('button').filter({ hasText: 'Calibrar' });
      const cardCount = await calibrarBtns.count();
      console.log('Number of country cards (Calibrar buttons): ' + cardCount);

      const greenBadges = page.locator('.text-green-600');
      const redBadges = page.locator('.text-red-500');
      const greenCount = await greenBadges.count();
      const redCount = await redBadges.count();
      console.log('Green (available) badges: ' + greenCount);
      console.log('Red (unavailable) badges: ' + redCount);

      const badgeEls = page.locator('div.flex.items-center');
      const bCount = await badgeEls.count();
      console.log('Checking badge details...');
      for (let i = 0; i < bCount; i++) {
        const text = (await badgeEls.nth(i).textContent()).trim();
        if (text === 'Elegibilidad' || text === 'Pesaje') {
          const hasGreen = (await badgeEls.nth(i).locator('.text-green-600').count()) > 0;
          console.log('  ' + text + ' => ' + (hasGreen ? 'GREEN' : 'RED'));
        }
      }
    } else {
      console.log('Calibracion link not found');
      const navText = await page.locator('nav').first().textContent().catch(() => 'nav not found');
      console.log('Nav content: ' + navText);
      await page.screenshot({ path: path.join(SCREENSHOT_DIR, '05-calibration-NOT-FOUND.png'), fullPage: true });
    }

    console.log('\n\n========== SUMMARY ==========');
    console.log('\n--- API Responses ---');
    if (apiResponses['stages']) {
      var sl = (apiResponses['stages'].body && apiResponses['stages'].body.stages) ? apiResponses['stages'].body.stages : [];
      console.log('/metadata/stages: status ' + apiResponses['stages'].status + ', ' + sl.length + ' stages');
    } else {
      console.log('/metadata/stages: NOT captured');
    }
    if (apiResponses['countries']) {
      var cl = (apiResponses['countries'].body && apiResponses['countries'].body.countries) ? apiResponses['countries'].body.countries : [];
      console.log('/metadata/countries: status ' + apiResponses['countries'].status + ', ' + cl.length + ' countries');
    } else {
      console.log('/metadata/countries: NOT captured');
    }
    if (apiResponses['data-availability']) {
      console.log('/metadata/data-availability: status ' + apiResponses['data-availability'].status);
      console.log('  body: ' + JSON.stringify(apiResponses['data-availability'].body));
    } else {
      console.log('/metadata/data-availability: NOT captured');
    }

    console.log('\n--- Console Errors ---');
    if (consoleErrors.length === 0) {
      console.log('No console errors detected.');
    } else {
      consoleErrors.forEach((e, i) => console.log('  ' + (i + 1) + '. ' + e));
    }

    console.log('\n--- Network Errors ---');
    if (networkErrors.length === 0) {
      console.log('No network errors detected.');
    } else {
      networkErrors.forEach((e, i) => console.log('  ' + (i + 1) + '. ' + e));
    }
    console.log('\n=============================');

  } catch (err) {
    console.error('FATAL ERROR:', err);
    await page.screenshot({ path: path.join(SCREENSHOT_DIR, 'ERROR-screenshot.png'), fullPage: true }).catch(() => {});
  } finally {
    await browser.close();
    console.log('\nBrowser closed. Done.');
  }
}

main();
