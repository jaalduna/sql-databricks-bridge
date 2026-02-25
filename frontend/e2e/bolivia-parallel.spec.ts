import { test, expect } from "@playwright/test"

const API_BASE = "http://127.0.0.1:8000/api/v1"

test.describe("Bolivia Parallel Execution Verification", () => {
  test.setTimeout(600_000) // 10 min max

  test("trigger Bolivia sync and verify parallel execution via API", async ({
    request,
  }) => {
    // 1. Trigger Bolivia extraction via API (bypasses auth since AUTH_ENABLED=false)
    const triggerResp = await request.post(`${API_BASE}/trigger`, {
      data: {
        country: "bolivia",
        stage: "inicio",
      },
    })
    expect(triggerResp.status()).toBe(202)
    const triggerData = await triggerResp.json()
    const jobId = triggerData.job_id
    console.log(
      `\nTriggered Bolivia job: ${jobId}, queries: ${triggerData.queries_count}`
    )
    expect(triggerData.queries_count).toBeGreaterThan(1)

    // 2. Poll /events/{jobId} to capture parallel execution state
    let sawParallel = false
    let maxRunning = 0
    let lastRunningQueries: string[] = []
    let finalDetail: any = null
    const pollSnapshots: { ts: string; running: number; completed: number; failed: number; running_queries: string[] }[] = []

    for (let i = 0; i < 300; i++) {
      // poll every 2 seconds, max 10 min
      await new Promise((r) => setTimeout(r, 2000))

      const detailResp = await request.get(`${API_BASE}/events/${jobId}`)
      expect(detailResp.status()).toBe(200)
      const detail = await detailResp.json()

      const running = detail.queries_running ?? 0
      const completed = detail.queries_completed ?? 0
      const failed = detail.queries_failed ?? 0
      const runningQueries = detail.running_queries ?? []

      pollSnapshots.push({
        ts: new Date().toISOString(),
        running,
        completed,
        failed,
        running_queries: runningQueries,
      })

      if (running > maxRunning) {
        maxRunning = running
        lastRunningQueries = runningQueries
      }
      if (running > 1) {
        sawParallel = true
      }

      console.log(
        `  [poll ${i + 1}] status=${detail.status} running=${running} completed=${completed} failed=${failed} total_rows=${detail.total_rows_extracted ?? 0} queries=${JSON.stringify(runningQueries)}`
      )

      if (detail.status === "completed" || detail.status === "failed") {
        finalDetail = detail
        break
      }
    }

    // 3. Also check /events list endpoint has the new fields
    const listResp = await request.get(
      `${API_BASE}/events?country=bolivia&limit=5`
    )
    expect(listResp.status()).toBe(200)
    const listData = await listResp.json()
    expect(listData.items.length).toBeGreaterThan(0)
    const listItem = listData.items.find((it: any) => it.job_id === jobId)
    expect(listItem).toBeDefined()
    // Verify new fields exist on list endpoint
    expect(listItem).toHaveProperty("queries_running")
    expect(listItem).toHaveProperty("running_queries")
    expect(listItem).toHaveProperty("total_rows_extracted")

    // 4. Summary
    console.log(`\n=== PARALLEL EXECUTION SUMMARY ===`)
    console.log(`Job ID: ${jobId}`)
    console.log(`Final status: ${finalDetail?.status}`)
    console.log(`Total queries: ${triggerData.queries_count}`)
    console.log(`Completed: ${finalDetail?.queries_completed}`)
    console.log(`Failed: ${finalDetail?.queries_failed}`)
    console.log(`Total rows extracted: ${finalDetail?.total_rows_extracted}`)
    console.log(`Max concurrent running: ${maxRunning}`)
    console.log(
      `Parallel execution detected: ${sawParallel ? "YES" : "NO"}`
    )
    if (maxRunning > 1) {
      console.log(
        `Peak running queries: ${JSON.stringify(lastRunningQueries)}`
      )
    }
    console.log(`Poll snapshots: ${pollSnapshots.length}`)
    console.log(`===================================\n`)

    // 5. Assertions
    expect(finalDetail).not.toBeNull()
    expect(finalDetail.status).toBe("completed")
    expect(finalDetail.total_rows_extracted).toBeGreaterThan(0)
    expect(sawParallel).toBe(true) // This is the key assertion: we saw >1 query running at once
    expect(maxRunning).toBeGreaterThanOrEqual(2)
  })

  test("verify SQLite crash recovery data exists", async ({ request }) => {
    // After the previous test, the job should be in SQLite too.
    // We verify by checking events detail returns results array (from SQLite)
    const listResp = await request.get(
      `${API_BASE}/events?country=bolivia&limit=1`
    )
    const listData = await listResp.json()
    expect(listData.items.length).toBeGreaterThan(0)

    const latestJob = listData.items[0]
    const detailResp = await request.get(
      `${API_BASE}/events/${latestJob.job_id}`
    )
    const detail = await detailResp.json()

    console.log(
      `\nSQLite verification: job=${detail.job_id} status=${detail.status} results_count=${detail.results?.length ?? 0}`
    )

    // If job is completed, results should be populated from SQLite
    if (detail.status === "completed" || detail.status === "failed") {
      expect(detail.results.length).toBeGreaterThan(0)
      // Each result should have query_name and status
      for (const r of detail.results) {
        expect(r).toHaveProperty("query_name")
        expect(r).toHaveProperty("status")
        console.log(
          `  query=${r.query_name} status=${r.status} rows=${r.rows_extracted ?? 0} duration=${r.duration_seconds?.toFixed(1) ?? "?"}s`
        )
      }
    }
  })
})
