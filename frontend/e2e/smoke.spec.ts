import { test, expect } from "@playwright/test"

test.describe("Smoke Tests", () => {
  test("frontend loads and shows login page", async ({ page }) => {
    await page.goto("/")
    await expect(page.getByText("SQL-Databricks Bridge")).toBeVisible({ timeout: 10_000 })
  })

  test("can sign in with dev auth and see dashboard", async ({ page }) => {
    await page.goto("/")
    await expect(page.getByText("SQL-Databricks Bridge")).toBeVisible({ timeout: 10_000 })

    // Click sign in (DevAuthProvider — VITE_AUTH_BYPASS=true)
    await page.getByRole("button", { name: /sign in/i }).click()

    // Should see dashboard content
    await expect(page.getByText("Trigger Data Sync")).toBeVisible({ timeout: 10_000 })
  })

  test("dashboard shows country and stage selectors", async ({ page }) => {
    await page.goto("/")
    await page.getByRole("button", { name: /sign in/i }).click()
    await expect(page.getByText("Trigger Data Sync")).toBeVisible({ timeout: 10_000 })

    // Verify country and stage selectors exist
    await expect(page.getByText("Country")).toBeVisible()
    await expect(page.getByText("Stage")).toBeVisible()
    await expect(page.getByRole("button", { name: /trigger sync/i })).toBeVisible()
  })

  test("event history page loads", async ({ page }) => {
    await page.goto("/")
    await page.getByRole("button", { name: /sign in/i }).click()
    await expect(page.getByText("Trigger Data Sync")).toBeVisible({ timeout: 10_000 })

    // Navigate to history (look for a link/nav item)
    const historyLink = page.getByRole("link", { name: /history/i })
    await expect(historyLink).toBeVisible()
    await historyLink.click()
    await expect(page).toHaveURL(/history/, { timeout: 10_000 })
  })

  test("backend API is reachable from frontend", async ({ page }) => {
    // Directly check the API health endpoint
    const response = await page.request.get("http://127.0.0.1:8000/api/v1/health/live")
    expect(response.ok()).toBeTruthy()
    const body = await response.json()
    expect(body.status).toBe("ok")
  })
})
