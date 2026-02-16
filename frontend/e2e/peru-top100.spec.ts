import { test, expect } from "@playwright/test"

test.describe("Peru TOP 100 Validation Sync", () => {
  test.setTimeout(600_000) // 10 min max

  test("trigger Peru Inicio sync and verify completion", async ({ page }) => {
    // 1. Go to app and sign in
    await page.goto("/")
    await expect(page.getByText("SQL-Databricks Bridge")).toBeVisible({ timeout: 10_000 })
    await page.getByRole("button", { name: /sign in/i }).click()
    await expect(page.getByText("Trigger Data Sync")).toBeVisible({ timeout: 10_000 })

    // 2. Select Peru country (Radix Select — click trigger, then option)
    await page.getByRole("combobox", { name: /country/i }).click()
    await page.getByRole("option", { name: /peru/i }).click()

    // 3. Select Inicio stage
    await page.getByRole("combobox", { name: /stage/i }).click()
    await page.getByRole("option", { name: /inicio/i }).click()

    // 4. Verify tag preview shows peru-inicio
    await expect(page.getByText(/peru-inicio-/).first()).toBeVisible()

    // 5. Take screenshot of pre-trigger state
    await page.screenshot({ path: "e2e/peru-top100-pre-trigger.png", fullPage: true })

    // 6. Click Trigger Sync
    const triggerBtn = page.getByRole("button", { name: /trigger sync/i })
    await expect(triggerBtn).toBeEnabled()
    await triggerBtn.click()

    // 7. Handle confirm dialog if present
    const confirmBtn = page.getByRole("button", { name: /confirm/i })
    if (await confirmBtn.isVisible({ timeout: 5_000 }).catch(() => false)) {
      await confirmBtn.click()
    }

    // 8. Should navigate to event detail page — wait for any event detail content
    await expect(page.getByText(/Back to History/)).toBeVisible({ timeout: 60_000 })

    // 9. Verify event metadata shows Peru
    await expect(page.getByText("Peru", { exact: true })).toBeVisible()

    // 10. Take screenshot showing sync in progress
    await page.screenshot({ path: "e2e/peru-top100-in-progress.png", fullPage: true })

    // 11. Wait for completion — target the MAIN status badge in the CardHeader
    //     (next to the country name), not query-level badges in the results table.
    //     The main badge has custom sizing classes "text-sm px-3 py-1".
    const mainStatusBadge = page.locator('[data-slot="badge"].px-3.py-1')
    await expect(mainStatusBadge).toHaveText(/Completed|Failed/i, { timeout: 480_000 })

    // 12. Take final screenshot
    await page.screenshot({ path: "e2e/peru-top100-complete.png", fullPage: true })

    // 13. Check if Query Results table is visible
    await expect(page.getByText("Query Results")).toBeVisible({ timeout: 10_000 })

    // 14. Log summary
    const finalStatus = await mainStatusBadge.textContent()
    const rows = page.locator("table tbody tr")
    const rowCount = await rows.count()
    console.log(`\nPeru TOP 100 sync finished with status: ${finalStatus}. Query result rows in table: ${rowCount}`)
  })
})
