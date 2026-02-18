import { defineConfig } from "@playwright/test"

export default defineConfig({
  testDir: "./e2e",
  timeout: 300_000, // 5 min per test (syncs are slow)
  expect: { timeout: 10_000 },
  fullyParallel: false,
  retries: 0,
  reporter: "list",
  use: {
    baseURL: "http://[::1]:5173/sql-databricks-bridge/",
    headless: true,
    screenshot: "only-on-failure",
    trace: "retain-on-failure",
    // Windows corporate machines often have slow proxy auto-detection (WPAD)
    // and IPv6/IPv4 Happy Eyeballs negotiation. These flags bypass both issues
    // so Chromium connects directly without delays.
    launchOptions: {
      args: [
        "--no-proxy-server",
        "--host-resolver-rules=MAP localhost 127.0.0.1",
      ],
    },
  },
  webServer: undefined, // we start servers manually
})
