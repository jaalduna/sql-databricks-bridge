import { test, expect } from "@playwright/test"

test.describe("Chile Inicio Sync", () => {
  // Chile has 40 queries, with TOP 100 it takes ~2 minutes
  test.setTimeout(600_000)

  test("login, trigger Chile sync, and verify progress feedback", async ({ page }) => {
    // 1. Go to login page
    await page.goto("/")
    await expect(page.getByText("SQL-Databricks Bridge")).toBeVisible()

    // 2. Click sign in (DevAuthProvider — VITE_AUTH_BYPASS=true)
    await page.getByRole("button", { name: /sign in/i }).click()

    // 3. Should redirect to dashboard
    await expect(page.getByText("Trigger Data Sync")).toBeVisible({ timeout: 10_000 })

    // 4. Verify version is displayed
    await expect(page.getByText(/v\d+\.\d+\.\d+/)).toBeVisible()

    // 5. Select Chile country
    await page.getByRole("combobox", { name: /country/i }).click()
    await page.getByRole("option", { name: /chile/i }).click()

    // 6. Select Inicio stage
    await page.getByRole("combobox", { name: /stage/i }).click()
    await page.getByRole("option", { name: /inicio/i }).click()

    // 7. Verify tag preview shows (use first() since recent jobs may also show same tag)
    await expect(page.getByText(/chile-inicio-/).first()).toBeVisible()

    // 8. Click Trigger Sync
    const triggerBtn = page.getByRole("button", { name: /trigger sync/i })
    await expect(triggerBtn).toBeEnabled()
    await triggerBtn.click()

    // 9. Confirm dialog should appear
    await expect(page.getByText("Confirm Sync Trigger")).toBeVisible()
    await expect(page.getByText(/You are about to trigger a data sync/)).toBeVisible()

    // 10. Click confirm (Confirm & Start button)
    await page.getByRole("button", { name: /confirm/i }).click()

    // 11. Should navigate to event detail page
    await expect(page.getByText(/Back to History/)).toBeVisible({ timeout: 60_000 })

    // 12. Verify event metadata shows Chile
    await expect(page.getByText("Chile", { exact: true })).toBeVisible()

    // 13. Verify progress bar appears with percentage
    await expect(page.getByText(/Progress:/)).toBeVisible({ timeout: 15_000 })

    // 14. Verify the "Syncing:" current query indicator appears while running
    await expect(page.getByText(/Syncing:/)).toBeVisible({ timeout: 30_000 })

    // Take mid-sync screenshot to capture progress state
    await page.screenshot({ path: "e2e/chile-sync-in-progress.png", fullPage: true })

    // 15. Wait for the "Running" badge to disappear (job finished)
    //     The status badge is in the card header next to the country name
    await expect(page.getByText("Running")).not.toBeVisible({ timeout: 480_000 })

    // 16. Verify "Syncing:" indicator disappears when completed
    await expect(page.getByText(/Syncing:/)).not.toBeVisible()

    // 17. Verify Query Results table appears
    await expect(page.getByText("Query Results")).toBeVisible()

    // 18. Verify at least one query row is shown
    const rows = page.locator("table tbody tr")
    const rowCount = await rows.count()
    expect(rowCount).toBeGreaterThan(0)

    // 19. Take a screenshot of the final state
    await page.screenshot({ path: "e2e/chile-sync-complete.png", fullPage: true })
  })
})
