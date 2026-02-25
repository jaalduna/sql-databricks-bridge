const { chromium } = require('playwright-core');

(async () => {
  const browser = await chromium.launch({
    args: ['--host-resolver-rules=MAP localhost 127.0.0.1', '--no-proxy-server']
  });
  const page = await browser.newPage();
  const dir = 'C:\\Users\\70088287\\Documents\\Projects\\sql-databricks-bridge\\debug-screenshots';

  const apiCalls = [];
  page.on('response', async (resp) => {
    const url = resp.url();
    if (url.includes('/api/v1/')) {
      apiCalls.push({ url, status: resp.status() });
    }
  });

  // 1. Login
  console.log('Navigating to login...');
  await page.goto('http://localhost:5173/sql-databricks-bridge/');
  await page.click('text=Sign in with Microsoft');
  await page.waitForURL('**/dashboard');
  console.log('Logged in, on dashboard');

  // 2. Wait for data to load
  await page.waitForTimeout(3000);
  await page.screenshot({ path: `${dir}/fix-01-dashboard.png`, fullPage: true });

  // Open stage dropdown
  await page.click('#stage');
  await page.waitForTimeout(500);
  await page.screenshot({ path: `${dir}/fix-02-stages-open.png`, fullPage: true });

  const stageOptions = await page.locator('[role="option"]').count();
  console.log(`Stage options: ${stageOptions}`);

  const stageTexts = await page.locator('[role="option"]').allTextContents();
  console.log(`Stage values: ${stageTexts.join(', ')}`);

  await page.keyboard.press('Escape');

  // 3. Calibration page
  console.log('Navigating to Calibration...');
  await page.click('text=Calibración');
  await page.waitForTimeout(4000);
  await page.screenshot({ path: `${dir}/fix-03-calibration.png`, fullPage: true });

  // Check page content
  const pageText = await page.textContent('main');
  const hasElegibilidad = pageText.includes('Elegibilidad');
  const hasCalibrar = pageText.includes('Calibrar');
  console.log(`Has Elegibilidad text: ${hasElegibilidad}`);
  console.log(`Has Calibrar buttons: ${hasCalibrar}`);

  console.log('\n--- API Calls ---');
  for (const call of apiCalls) {
    console.log(`${call.status} ${call.url}`);
  }

  await browser.close();
  console.log('\nDone!');
})();
