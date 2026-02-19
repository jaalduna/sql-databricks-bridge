import express from "express"
import cors from "cors"
import crypto from "crypto"

const app = express()
app.use(cors())
app.use(express.json())

const PORT = 5000

// ---------------------------------------------------------------------------
// Country metadata
// ---------------------------------------------------------------------------

const COUNTRIES = [
  {
    code: "argentina",
    queries: ["j_atoscompra_new", "hato_cabecalho", "productos", "clientes", "region_kantar", "canales"],
    queries_count: 6,
  },
  {
    code: "bolivia",
    queries: ["j_atoscompra_new", "hato_cabecalho", "productos", "clientes", "canales"],
    queries_count: 5,
  },
  {
    code: "brasil",
    queries: ["j_atoscompra_new", "hato_cabecalho", "produtos", "clientes", "region_kantar", "canales", "categorias"],
    queries_count: 7,
  },
  {
    code: "cam",
    queries: ["j_atoscompra_new", "hato_cabecalho", "productos", "clientes", "canales"],
    queries_count: 5,
  },
  {
    code: "chile",
    queries: ["j_atoscompra_new", "hato_cabecalho", "productos", "clientes", "region_kantar", "canales"],
    queries_count: 6,
  },
  {
    code: "colombia",
    queries: ["j_atoscompra_new", "hato_cabecalho", "productos", "clientes", "region_kantar", "canales"],
    queries_count: 6,
  },
  {
    code: "ecuador",
    queries: ["j_atoscompra_new", "hato_cabecalho", "productos", "clientes", "canales"],
    queries_count: 5,
  },
  {
    code: "mexico",
    queries: ["j_atoscompra_new", "hato_cabecalho", "productos", "clientes", "region_kantar", "canales", "categorias"],
    queries_count: 7,
  },
  {
    code: "peru",
    queries: ["j_atoscompra_new", "hato_cabecalho", "productos", "clientes", "region_kantar", "canales"],
    queries_count: 6,
  },
]

// ---------------------------------------------------------------------------
// Calibration pipeline steps (ordered)
// ---------------------------------------------------------------------------

const CALIBRATION_STEPS = [
  "sync_data",
  "copy_to_calibration",
  "merge_data",
  "simulate_kpis",
  "calculate_penetration",
  "download_csv",
]

function makeSteps(status = "pending") {
  return CALIBRATION_STEPS.map((name) => ({
    name,
    status,
    started_at: null,
    completed_at: null,
    error: null,
  }))
}

function makeCompletedSteps(baseTime) {
  let cursor = new Date(baseTime).getTime()
  return CALIBRATION_STEPS.map((name) => {
    const started = new Date(cursor).toISOString()
    cursor += Math.floor(Math.random() * 8000) + 2000
    const completed = new Date(cursor).toISOString()
    cursor += 500
    return { name, status: "completed", started_at: started, completed_at: completed, error: null }
  })
}

function makeFailedSteps(baseTime, failAtIndex) {
  let cursor = new Date(baseTime).getTime()
  return CALIBRATION_STEPS.map((name, i) => {
    if (i > failAtIndex) {
      return { name, status: "pending", started_at: null, completed_at: null, error: null }
    }
    const started = new Date(cursor).toISOString()
    cursor += Math.floor(Math.random() * 5000) + 1000
    if (i === failAtIndex) {
      return { name, status: "failed", started_at: started, completed_at: new Date(cursor).toISOString(), error: `Step "${name}" failed: simulated error` }
    }
    const completed = new Date(cursor).toISOString()
    cursor += 500
    return { name, status: "completed", started_at: started, completed_at: completed, error: null }
  })
}

function currentStepFromSteps(steps) {
  const running = steps.find((s) => s.status === "running")
  if (running) return running.name
  const pending = steps.find((s) => s.status === "pending")
  const allDone = steps.every((s) => s.status === "completed" || s.status === "failed")
  if (allDone) return null
  if (pending) return pending.name
  return null
}

// ---------------------------------------------------------------------------
// In-memory job store
// ---------------------------------------------------------------------------

/** @type {Map<string, object>} */
const jobs = new Map()

function uuid() {
  return crypto.randomUUID()
}

function isoNow() {
  return new Date().toISOString()
}

// ---------------------------------------------------------------------------
// Enrich summary helper
// ---------------------------------------------------------------------------

function enrichSummary(job) {
  const { results, _stepIndex, ...base } = job
  const running_queries = (results || []).filter(r => r.status === "running").map(r => r.query_name)
  const failed_queries = (results || []).filter(r => r.status === "failed").map(r => r.query_name)
  const current_query = running_queries[0] || null
  const queries_running = running_queries.length
  const total_rows_extracted = (results || []).reduce((sum, r) => sum + (r.rows_extracted || 0), 0)
  return {
    ...base,
    current_query,
    failed_queries,
    running_queries,
    queries_running,
    total_rows_extracted,
  }
}

// ---------------------------------------------------------------------------
// Seed helper
// ---------------------------------------------------------------------------

function seedCompletedJob(country, stage, period, daysAgo) {
  const countryMeta = COUNTRIES.find((c) => c.code === country)
  const queries = countryMeta ? countryMeta.queries : ["j_atoscompra_new", "productos"]
  const jobId = uuid()
  const created = new Date(Date.now() - daysAgo * 24 * 60 * 60 * 1000).toISOString()
  const started = new Date(new Date(created).getTime() + 2000).toISOString()
  const completed = new Date(new Date(started).getTime() + 30000 + Math.floor(Math.random() * 60000)).toISOString()

  const results = queries.map((q) => ({
    query_name: q,
    status: "completed",
    rows_extracted: Math.floor(Math.random() * 50000) + 1000,
    table_name: `kpi_prd_01.bridge.${country}_${q}`,
    duration_seconds: +(Math.random() * 30 + 2).toFixed(2),
    error: null,
    started_at: started,
  }))

  jobs.set(jobId, {
    job_id: jobId,
    status: "completed",
    country,
    stage,
    period,
    tag: `${country}-${stage}-seed`,
    queries_total: queries.length,
    queries_completed: queries.length,
    queries_failed: 0,
    created_at: created,
    started_at: started,
    completed_at: completed,
    triggered_by: "system-seed",
    error: null,
    results,
    steps: makeCompletedSteps(started),
    current_step: null,
  })
}

function seedFailedJob(country, stage, period, daysAgo) {
  const countryMeta = COUNTRIES.find((c) => c.code === country)
  const queries = countryMeta ? countryMeta.queries : ["j_atoscompra_new"]
  const jobId = uuid()
  const created = new Date(Date.now() - daysAgo * 24 * 60 * 60 * 1000).toISOString()

  const failedStarted = new Date(new Date(created).getTime() + 1000).toISOString()

  const results = queries.map((q, i) => {
    if (i === queries.length - 1) {
      return {
        query_name: q,
        status: "failed",
        rows_extracted: 0,
        table_name: null,
        duration_seconds: +(Math.random() * 5).toFixed(2),
        error: "Timeout: query exceeded 300s limit",
        started_at: failedStarted,
      }
    }
    return {
      query_name: q,
      status: "completed",
      rows_extracted: Math.floor(Math.random() * 10000) + 500,
      table_name: `kpi_prd_01.bridge.${country}_${q}`,
      duration_seconds: +(Math.random() * 20 + 1).toFixed(2),
      error: null,
      started_at: failedStarted,
    }
  })

  const failStepIndex = Math.min(2, CALIBRATION_STEPS.length - 1) // fail at merge_data step

  jobs.set(jobId, {
    job_id: jobId,
    status: "failed",
    country,
    stage,
    period,
    tag: `${country}-${stage}-seed`,
    queries_total: queries.length,
    queries_completed: queries.length - 1,
    queries_failed: 1,
    created_at: created,
    started_at: failedStarted,
    completed_at: new Date(new Date(created).getTime() + 45000).toISOString(),
    triggered_by: "system-seed",
    error: "1 query failed",
    results,
    steps: makeFailedSteps(failedStarted, failStepIndex),
    current_step: null,
  })
}

// ---------------------------------------------------------------------------
// Pre-seed jobs for data-availability testing
// ---------------------------------------------------------------------------

const CURRENT_PERIOD = "202602"
const PREVIOUS_PERIOD = "202601"

// -- Current period (202602) --
// Most countries: both elegibilidad + pesaje completed
const FULL_COUNTRIES = ["argentina", "bolivia", "brasil", "chile", "colombia", "mexico"]
for (const country of FULL_COUNTRIES) {
  seedCompletedJob(country, "elegibilidad", CURRENT_PERIOD, Math.floor(Math.random() * 5) + 1)
  seedCompletedJob(country, "pesaje", CURRENT_PERIOD, Math.floor(Math.random() * 3))
}

// Peru: only elegibilidad completed for current period (no pesaje) - tests gating
seedCompletedJob("peru", "elegibilidad", CURRENT_PERIOD, 2)

// Ecuador and CAM: failed elegibilidad for current period (no pesaje either)
seedFailedJob("ecuador", "elegibilidad", CURRENT_PERIOD, 2)
seedFailedJob("cam", "elegibilidad", CURRENT_PERIOD, 3)

// -- Previous period (202601) --
// Seed a few countries with completed data for previous period
for (const country of ["argentina", "chile", "colombia", "peru"]) {
  seedCompletedJob(country, "elegibilidad", PREVIOUS_PERIOD, 30 + Math.floor(Math.random() * 5))
  seedCompletedJob(country, "pesaje", PREVIOUS_PERIOD, 28 + Math.floor(Math.random() * 5))
}

// Bolivia had only elegibilidad in previous period
seedCompletedJob("bolivia", "elegibilidad", PREVIOUS_PERIOD, 32)

// ---------------------------------------------------------------------------
// Job progression simulation
// ---------------------------------------------------------------------------

function advanceJob(jobId) {
  const job = jobs.get(jobId)
  if (!job || job.status === "completed" || job.status === "failed" || job.status === "cancelled") return

  if (job.status === "pending") {
    job.status = "running"
    job.started_at = isoNow()
    // Start first non-completed step (respects skip_sync)
    if (job.steps && job.steps.length > 0) {
      const firstPending = job.steps.find((s) => s.status === "pending")
      if (firstPending) {
        firstPending.status = "running"
        firstPending.started_at = isoNow()
        job.current_step = firstPending.name
      }
    }
  }

  if (!job.steps) return

  const stepIdx = job._stepIndex ?? 0
  if (stepIdx >= job.steps.length) return

  const step = job.steps[stepIdx]

  // --- sync_data step: advance one query per tick ---
  if (step.name === "sync_data") {
    if (step.status === "pending") {
      step.status = "running"
      step.started_at = isoNow()
      job.current_step = step.name
    }

    const pendingQueries = job.results.filter((r) => r.status === "pending")
    if (pendingQueries.length > 0) {
      const next = pendingQueries[0]
      next.started_at = isoNow()
      // 10% chance of failure per query
      if (Math.random() < 0.1) {
        next.status = "failed"
        next.rows_extracted = 0
        next.duration_seconds = +(Math.random() * 3).toFixed(2)
        next.error = "Simulated query failure"
        job.queries_failed++
      } else {
        next.status = "completed"
        next.rows_extracted = Math.floor(Math.random() * 40000) + 500
        next.table_name = `kpi_prd_01.bridge.${job.country}_${next.query_name}`
        next.duration_seconds = +(Math.random() * 15 + 1).toFixed(2)
      }
      job.queries_completed++
      return // one query per tick during sync_data
    }

    // All queries done — complete sync_data step
    // If any query failed, fail the whole job at sync_data
    if (job.queries_failed > 0) {
      step.status = "failed"
      step.completed_at = isoNow()
      step.error = `${job.queries_failed} query(ies) failed during sync`
      job.status = "failed"
      job.completed_at = isoNow()
      job.current_step = null
      job.error = `Pipeline failed at step: sync_data (${job.queries_failed} query failures)`
      return
    }
    step.status = "completed"
    step.completed_at = isoNow()
    job._stepIndex = stepIdx + 1
    // Start next step
    if (stepIdx + 1 < job.steps.length) {
      job.steps[stepIdx + 1].status = "running"
      job.steps[stepIdx + 1].started_at = isoNow()
      job.current_step = job.steps[stepIdx + 1].name
    }
    return
  }

  // --- Non-sync steps: advance one step per tick ---
  if (step.status === "pending") {
    step.status = "running"
    step.started_at = isoNow()
    job.current_step = step.name
  } else if (step.status === "running") {
    // 5% chance of step failure
    if (Math.random() < 0.05) {
      step.status = "failed"
      step.completed_at = isoNow()
      step.error = `Step "${step.name}" failed: simulated error`
      job.current_step = null
      job.status = "failed"
      job.completed_at = isoNow()
      job.error = `Pipeline failed at step: ${step.name}`
      return
    }
    step.status = "completed"
    step.completed_at = isoNow()
    job._stepIndex = stepIdx + 1
    // Start next step if available
    if (stepIdx + 1 < job.steps.length) {
      job.steps[stepIdx + 1].status = "running"
      job.steps[stepIdx + 1].started_at = isoNow()
      job.current_step = job.steps[stepIdx + 1].name
    } else {
      // All steps done
      job.status = "completed"
      job.completed_at = isoNow()
      job.current_step = null
    }
  }
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

// Health
app.get("/health/live", (_req, res) => {
  res.json({ status: "ok" })
})

// Auth me
app.get("/api/v1/auth/me", (_req, res) => {
  res.json({
    email: "dev@localhost",
    name: "Dev User",
    roles: ["admin"],
    countries: ["*"],
  })
})

// Metadata
app.get("/api/v1/metadata/countries", (_req, res) => {
  res.json({
    countries: COUNTRIES.map(({ code, queries, queries_count }) => ({
      code,
      queries,
      queries_count,
    })),
  })
})

app.get("/api/v1/metadata/stages", (_req, res) => {
  res.json({
    stages: [
      { code: "inicio", name: "Inicio" },
      { code: "precios", name: "Precios" },
      { code: "elegibilidad", name: "Elegibilidad" },
      { code: "pesaje", name: "Pesaje" },
      { code: "calibracion", name: "Calibracion" },
      { code: "mtr", name: "MTR" },
    ],
  })
})

// Data availability (simulates SQL Server check)
const MOCK_AVAILABILITY = {
  "202602": {
    argentina: { elegibilidad: true, pesaje: true },
    bolivia: { elegibilidad: true, pesaje: true },
    brasil: { elegibilidad: true, pesaje: true },
    cam: { elegibilidad: false, pesaje: false },
    chile: { elegibilidad: true, pesaje: true },
    colombia: { elegibilidad: true, pesaje: true },
    ecuador: { elegibilidad: false, pesaje: false },
    mexico: { elegibilidad: true, pesaje: true },
    peru: { elegibilidad: true, pesaje: false },
  },
  "202601": {
    argentina: { elegibilidad: true, pesaje: true },
    bolivia: { elegibilidad: true, pesaje: false },
    chile: { elegibilidad: true, pesaje: true },
    colombia: { elegibilidad: true, pesaje: true },
    peru: { elegibilidad: true, pesaje: true },
  },
}

app.get("/api/v1/metadata/data-availability", (req, res) => {
  const { period } = req.query
  if (!period || !/^\d{6}$/.test(period)) {
    return res.status(422).json({ error: "validation_error", message: "period must be YYYYMM format" })
  }
  const periodData = MOCK_AVAILABILITY[period] || {}
  const countries = {}
  for (const c of COUNTRIES) {
    countries[c.code] = periodData[c.code] || { elegibilidad: false, pesaje: false }
  }
  res.json({ period, countries })
})

// Trigger
app.post("/api/v1/trigger", (req, res) => {
  const { country, stage, period, queries: requestedQueries, aggregations, row_limit, lookback_months, skip_sync } = req.body

  if (!country || !stage) {
    return res.status(400).json({ error: "bad_request", message: "country and stage are required" })
  }

  const countryMeta = COUNTRIES.find((c) => c.code === country)
  if (!countryMeta) {
    return res.status(404).json({ error: "not_found", message: `Unknown country: ${country}` })
  }

  const queryList = requestedQueries ?? countryMeta.queries
  const jobId = uuid()
  const now = isoNow()

  const results = queryList.map((q) => ({
    query_name: q,
    status: "pending",
    rows_extracted: 0,
    table_name: null,
    duration_seconds: 0,
    error: null,
    started_at: null,
  }))

  const job = {
    job_id: jobId,
    status: "pending",
    country,
    stage,
    period: period ?? null,
    tag: `${country}-${stage}-${Date.now()}`,
    queries_total: queryList.length,
    queries_completed: 0,
    queries_failed: 0,
    created_at: now,
    started_at: null,
    completed_at: null,
    triggered_by: "calibration-app",
    aggregations: aggregations ?? null,
    error: null,
    results,
    steps: makeSteps(),
    current_step: "sync_data",
    _stepIndex: 0, // internal: tracks which step to advance next
  }

  // If skip_sync, immediately complete sync_data step and all queries
  if (skip_sync) {
    job.steps[0].status = "completed"
    job.steps[0].started_at = isoNow()
    job.steps[0].completed_at = isoNow()
    job._stepIndex = 1
    for (const r of job.results) {
      r.status = "completed"
      r.rows_extracted = Math.floor(Math.random() * 40000) + 500
      r.table_name = `kpi_prd_01.bridge.${country}_${r.query_name}`
      r.duration_seconds = 0
      r.started_at = isoNow()
    }
    job.queries_completed = queryList.length
    job.current_step = "copy_to_calibration"
  }

  jobs.set(jobId, job)

  if (row_limit || lookback_months) {
    console.log(`  Overrides: row_limit=${row_limit ?? "default"}, lookback_months=${lookback_months ?? "default"}`)
  }

  // Schedule progression: queries advance during sync_data, then steps advance after
  // sync_data takes queryList.length ticks + 1 to complete, remaining 5 steps take 2 ticks each
  let tick = 0
  const maxTicks = queryList.length + 2 + (CALIBRATION_STEPS.length - 1) * 2 + 3
  const interval = setInterval(() => {
    advanceJob(jobId)
    tick++
    const current = jobs.get(jobId)
    if (!current || current.status === "completed" || current.status === "failed" || current.status === "cancelled" || tick >= maxTicks) {
      clearInterval(interval)
    }
  }, 2000)

  // Return TriggerResponse (without results, matching the type)
  res.status(201).json({
    job_id: jobId,
    status: "pending",
    country,
    stage,
    period: period ?? null,
    tag: job.tag,
    queries: queryList,
    queries_count: queryList.length,
    created_at: now,
    triggered_by: "calibration-app",
  })
})

// Events list
app.get("/api/v1/events", (req, res) => {
  const { country, status, stage, period, limit: limitStr, offset: offsetStr } = req.query
  const limit = parseInt(limitStr, 10) || 50
  const offset = parseInt(offsetStr, 10) || 0

  let items = Array.from(jobs.values()).map((job) => enrichSummary(job))

  if (country) items = items.filter((j) => j.country === country)
  if (status) items = items.filter((j) => j.status === status)
  if (stage) items = items.filter((j) => j.stage === stage)
  if (period) items = items.filter((j) => j.period === period)

  // Sort by created_at descending
  items.sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime())

  const total = items.length
  const paged = items.slice(offset, offset + limit)

  res.json({ items: paged, total, limit, offset })
})

// Event detail
app.get("/api/v1/events/:jobId", (req, res) => {
  const job = jobs.get(req.params.jobId)
  if (!job) {
    return res.status(404).json({ error: "not_found", message: `Job ${req.params.jobId} not found` })
  }
  const enriched = enrichSummary(job)
  res.json({ ...enriched, results: job.results })
})

// Cancel event
app.post("/api/v1/events/:jobId/cancel", (req, res) => {
  const job = jobs.get(req.params.jobId)
  if (!job) {
    return res.status(404).json({ error: "not_found", message: `Job ${req.params.jobId} not found` })
  }
  if (job.status === "completed" || job.status === "failed" || job.status === "cancelled") {
    return res.status(409).json({ error: "conflict", message: `Job is already ${job.status}` })
  }
  job.status = "cancelled"
  job.completed_at = isoNow()
  job.current_step = null
  job.error = "Cancelled by user"
  // Mark running steps as cancelled
  if (job.steps) {
    for (const step of job.steps) {
      if (step.status === "running") {
        step.status = "failed"
        step.completed_at = isoNow()
        step.error = "Cancelled by user"
      }
    }
  }
  // Mark pending queries as cancelled
  if (job.results) {
    for (const r of job.results) {
      if (r.status === "pending" || r.status === "running") {
        r.status = "failed"
        r.error = "Cancelled by user"
      }
    }
  }
  const enriched = enrichSummary(job)
  res.json({ ...enriched, results: job.results })
})

// Download CSV
app.get("/api/v1/events/:jobId/download", (req, res) => {
  const job = jobs.get(req.params.jobId)
  if (!job) {
    return res.status(404).json({ error: "not_found", message: `Job ${req.params.jobId} not found` })
  }
  if (job.status !== "completed" && job.status !== "failed") {
    return res.status(409).json({ error: "job_not_finished", message: `Job is still ${job.status}` })
  }

  const lines = []
  lines.push("# Calibration Job Report")
  lines.push(`job_id,${job.job_id}`)
  lines.push(`country,${job.country}`)
  lines.push(`stage,${job.stage}`)
  lines.push(`period,${job.period || ""}`)
  lines.push(`tag,${job.tag}`)
  lines.push(`status,${job.status}`)
  lines.push(`triggered_by,${job.triggered_by}`)
  lines.push(`started_at,${job.started_at || ""}`)
  lines.push(`completed_at,${job.completed_at || ""}`)
  lines.push(`error,${job.error || ""}`)
  lines.push("")
  lines.push("# Pipeline Steps")
  lines.push("step_name,status,started_at,completed_at,duration_seconds,error")
  if (job.steps) {
    for (const s of job.steps) {
      let duration = ""
      if (s.started_at && s.completed_at) {
        duration = ((new Date(s.completed_at) - new Date(s.started_at)) / 1000).toFixed(2)
      }
      lines.push(`${s.name},${s.status},${s.started_at || ""},${s.completed_at || ""},${duration},${s.error || ""}`)
    }
  }
  lines.push("")
  lines.push("# Query Results")
  lines.push("query_name,status,rows_extracted,table_name,duration_seconds,error")
  if (job.results) {
    for (const r of job.results) {
      lines.push(`${r.query_name},${r.status},${r.rows_extracted},${r.table_name || ""},${(r.duration_seconds || 0).toFixed(2)},${r.error || ""}`)
    }
  }

  const csv = lines.join("\n")
  const filename = `calibration_${job.country}_${job.period || "no-period"}_${job.job_id.slice(0, 8)}.csv`

  res.setHeader("Content-Type", "text/csv")
  res.setHeader("Content-Disposition", `attachment; filename="${filename}"`)
  res.send(csv)
})

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

app.listen(PORT, () => {
  console.log(`Mock sql-databricks-bridge running on http://localhost:${PORT}`)
  console.log(`  GET  http://localhost:${PORT}/health/live`)
  console.log(`  GET  http://localhost:${PORT}/api/v1/auth/me`)
  console.log(`  GET  http://localhost:${PORT}/api/v1/metadata/countries`)
  console.log(`  GET  http://localhost:${PORT}/api/v1/metadata/data-availability?period=202602`)
  console.log(`  POST http://localhost:${PORT}/api/v1/trigger`)
  console.log(`  GET  http://localhost:${PORT}/api/v1/events`)
  console.log(`  GET  http://localhost:${PORT}/api/v1/events/:jobId`)
  console.log(`  POST http://localhost:${PORT}/api/v1/events/:jobId/cancel`)
  console.log(`\nPre-seeded ${jobs.size} jobs for data availability testing`)
  console.log(`\nSeeded data summary:`)
  console.log(`  Current period (${CURRENT_PERIOD}):`)
  console.log(`    Full (elegibilidad+pesaje): ${FULL_COUNTRIES.join(", ")}`)
  console.log(`    Elegibilidad only: peru`)
  console.log(`    Failed elegibilidad: ecuador, cam`)
  console.log(`  Previous period (${PREVIOUS_PERIOD}):`)
  console.log(`    Full: argentina, chile, colombia, peru`)
  console.log(`    Elegibilidad only: bolivia`)
})
