import { test, expect, Page } from "@playwright/test"

const API_BASE = "http://127.0.0.1:8000/api/v1"

/**
 * Warm up the browser's HTTP connection to the backend for API-only tests.
 *
 * On Windows, the first TCP connection from Chromium can be slow due to
 * proxy auto-detection (WPAD). The --no-proxy-server browser flag mitigates
 * this, but we still prime the connection with a lightweight fetch.
 */
async function warmUpForApi(page: Page): Promise<void> {
  await page.evaluate(async () => {
    await fetch("http://127.0.0.1:8000/api/v1/health/live")
  })
}

/**
 * Sign in to the frontend and wait for the dashboard's own API calls to
 * complete. This ensures the browser's connection pool to the backend is
 * fully established from the frontend's origin context.
 *
 * The dashboard page makes calls to /metadata/countries, /metadata/stages,
 * and /events on load. Waiting for those to return ensures the TCP socket
 * pool is primed for subsequent cross-origin requests to 127.0.0.1:8000.
 */
async function signInAndWarmUp(page: Page): Promise<void> {
  await page.goto("/")
  await page.getByRole("button", { name: /sign in/i }).click()
  await expect(page.getByText("Trigger Data Sync")).toBeVisible({ timeout: 10_000 })

  // Wait for the dashboard's own API calls to succeed.
  // This warms up the browser's socket pool to 127.0.0.1:8000 from
  // the frontend's origin. Timeout is generous for the first connection.
  await page.waitForResponse(
    (resp) => resp.url().includes("/events") && resp.status() === 200,
    { timeout: 240_000 }
  )
}

test.describe("Performance Validation", () => {
  test.describe.configure({ mode: "serial" })

  test("API /events responds under 5 seconds (cold start target)", async ({ page }) => {
    await warmUpForApi(page)

    const start = Date.now()
    const response = await page.request.get(`${API_BASE}/events`)
    const elapsed = Date.now() - start

    expect(response.ok()).toBeTruthy()
    const body = await response.json()
    expect(body.items.length).toBeGreaterThan(0)
    expect(body.total).toBeGreaterThan(0)

    console.log(`[PERF] /events cold start: ${elapsed}ms (target: <5000ms)`)
    expect(elapsed).toBeLessThan(5000)
  })

  test("API /events responds under 3 seconds on subsequent calls", async ({ page }) => {
    await warmUpForApi(page)
    // Prime page.request path
    await page.request.get(`${API_BASE}/events`)

    const times: number[] = []
    for (let i = 0; i < 3; i++) {
      const start = Date.now()
      const response = await page.request.get(`${API_BASE}/events`)
      const elapsed = Date.now() - start
      times.push(elapsed)
      expect(response.ok()).toBeTruthy()
    }

    const avg = Math.round(times.reduce((a, b) => a + b, 0) / times.length)
    const max = Math.max(...times)
    console.log(`[PERF] /events subsequent: avg=${avg}ms max=${max}ms (target: <3000ms)`)

    for (const t of times) {
      expect(t).toBeLessThan(3000)
    }
  })

  test("API /events/{id} detail responds under 3 seconds", async ({ page }) => {
    await warmUpForApi(page)

    const listResp = await page.request.get(`${API_BASE}/events`)
    const list = await listResp.json()
    expect(list.items.length).toBeGreaterThan(0)
    const jobId = list.items[0].job_id

    const start = Date.now()
    const detailResp = await page.request.get(`${API_BASE}/events/${jobId}`)
    const elapsed = Date.now() - start

    expect(detailResp.ok()).toBeTruthy()
    const detail = await detailResp.json()
    expect(detail.job_id).toBe(jobId)

    console.log(`[PERF] /events/${jobId}: ${elapsed}ms (target: <3000ms)`)
    expect(elapsed).toBeLessThan(3000)
  })

  test("Frontend history page loads data and API responds fast", async ({ page }) => {
    // Sign in and let the dashboard's API calls warm up the connection
    await signInAndWarmUp(page)

    // Navigate to History and wait for API response
    const apiResponsePromise = page.waitForResponse(
      (resp) => resp.url().includes("/events") && resp.status() === 200,
      { timeout: 10_000 }
    )

    const navStart = Date.now()
    await page.getByRole("link", { name: /history/i }).click()

    const apiResponse = await apiResponsePromise
    const apiElapsed = Date.now() - navStart

    const body = await apiResponse.json()
    expect(body.items.length).toBeGreaterThan(0)

    console.log(`[PERF] Frontend /events API response: ${apiElapsed}ms, items: ${body.items.length} (target: <5000ms)`)
    expect(apiElapsed).toBeLessThan(5000)

    // Wait for data to render in the table
    await expect(
      page.getByText(/completed|failed|Showing \d/).first()
    ).toBeVisible({ timeout: 5_000 })

    const renderElapsed = Date.now() - navStart
    console.log(`[PERF] Frontend history data rendered: ${renderElapsed}ms (target: <5000ms)`)
    expect(renderElapsed).toBeLessThan(5000)
  })

  test("Frontend history page refresh - API responds under 3 seconds", async ({ page }) => {
    // Sign in and let the dashboard's API calls warm up the connection
    await signInAndWarmUp(page)

    // Navigate to history and wait for initial load
    await page.getByRole("link", { name: /history/i }).click()
    await page.waitForResponse(
      (resp) => resp.url().includes("/events") && resp.status() === 200,
      { timeout: 10_000 }
    )

    // Wait for data to render
    await expect(
      page.getByText(/completed|failed|Showing \d/).first()
    ).toBeVisible({ timeout: 5_000 })

    // Now that the connection is warm and the page is loaded, measure
    // a direct API call from the browser's context. This tests the
    // "subsequent request" performance from the frontend's origin,
    // which is what users experience on page refresh.
    const result = await page.evaluate(async () => {
      const start = performance.now()
      const resp = await fetch("http://127.0.0.1:8000/api/v1/events?limit=20&offset=0")
      const elapsed = Math.round(performance.now() - start)
      const body = await resp.json()
      return { elapsed, items: body.items?.length ?? 0, status: resp.status }
    })

    console.log(`[PERF] Frontend refresh API: ${result.elapsed}ms, items: ${result.items} (target: <3000ms)`)
    expect(result.status).toBe(200)
    expect(result.items).toBeGreaterThan(0)
    expect(result.elapsed).toBeLessThan(3000)
  })
})
