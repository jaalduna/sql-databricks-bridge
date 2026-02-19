/**
 * Hybrid proxy server for E2E testing:
 * - Mocks metadata endpoints (countries, data-availability) and auth/me
 * - Proxies trigger/events to the real sql-databricks-bridge backend
 *
 * Usage:
 *   REAL_BACKEND=http://localhost:8000 node mock-server/hybrid-proxy.js
 */
import express from "express"
import cors from "cors"
import http from "http"

const app = express()
app.use(cors())
app.use(express.json())

const PORT = 5000
const REAL_BACKEND = process.env.REAL_BACKEND || "http://localhost:8000"

// ---------------------------------------------------------------------------
// Mocked metadata endpoints
// ---------------------------------------------------------------------------

const COUNTRIES = [
  { code: "argentina", queries: ["j_atoscompra_new", "hato_cabecalho", "productos", "clientes", "region_kantar", "canales"], queries_count: 6 },
  { code: "bolivia", queries: ["j_atoscompra_new", "hato_cabecalho", "productos", "clientes", "canales"], queries_count: 5 },
  { code: "brasil", queries: ["j_atoscompra_new", "hato_cabecalho", "produtos", "clientes", "region_kantar", "canales", "categorias"], queries_count: 7 },
  { code: "cam", queries: ["j_atoscompra_new", "hato_cabecalho", "productos", "clientes", "canales"], queries_count: 5 },
  { code: "chile", queries: ["j_atoscompra_new", "hato_cabecalho", "productos", "clientes", "region_kantar", "canales"], queries_count: 6 },
  { code: "colombia", queries: ["j_atoscompra_new", "hato_cabecalho", "productos", "clientes", "region_kantar", "canales"], queries_count: 6 },
  { code: "ecuador", queries: ["j_atoscompra_new", "hato_cabecalho", "productos", "clientes", "canales"], queries_count: 5 },
  { code: "mexico", queries: ["j_atoscompra_new", "hato_cabecalho", "productos", "clientes", "region_kantar", "canales", "categorias"], queries_count: 7 },
  { code: "peru", queries: ["j_atoscompra_new", "hato_cabecalho", "productos", "clientes", "region_kantar", "canales"], queries_count: 6 },
]

app.get("/api/v1/metadata/countries", (_req, res) => {
  res.json({ countries: COUNTRIES })
})

app.get("/api/v1/metadata/data-availability", (req, res) => {
  const { period } = req.query
  if (!period || !/^\d{6}$/.test(period)) {
    return res.status(422).json({ error: "validation_error", message: "period must be YYYYMM format" })
  }
  // Mock: Bolivia always has both available for current period
  const countries = {}
  for (const c of COUNTRIES) {
    if (c.code === "bolivia") {
      countries[c.code] = { elegibilidad: true, pesaje: true }
    } else {
      countries[c.code] = { elegibilidad: false, pesaje: false }
    }
  }
  res.json({ period, countries })
})

app.get("/health/live", (_req, res) => {
  res.json({ status: "ok" })
})

app.get("/api/v1/health", (_req, res) => {
  res.json({ status: "ok" })
})

// Auth me — always mocked locally
app.get("/api/v1/auth/me", (_req, res) => {
  res.json({
    email: "dev@localhost",
    name: "Dev User",
    roles: ["admin"],
    countries: ["*"],
  })
})

// ---------------------------------------------------------------------------
// Proxy to real backend for trigger/events
// ---------------------------------------------------------------------------

function proxyToBackend(req, res) {
  const url = new URL(req.originalUrl, REAL_BACKEND)
  const options = {
    hostname: url.hostname,
    port: url.port,
    path: url.pathname + url.search,
    method: req.method,
    headers: { ...req.headers, host: url.host },
  }

  const proxyReq = http.request(options, (proxyRes) => {
    res.status(proxyRes.statusCode)
    for (const [key, value] of Object.entries(proxyRes.headers)) {
      if (key.toLowerCase() !== "transfer-encoding") {
        res.setHeader(key, value)
      }
    }
    proxyRes.pipe(res)
  })

  proxyReq.on("error", (err) => {
    console.error(`Proxy error: ${err.message}`)
    res.status(502).json({ error: "proxy_error", message: `Backend unavailable: ${err.message}` })
  })

  if (req.body && Object.keys(req.body).length > 0) {
    proxyReq.write(JSON.stringify(req.body))
  }
  proxyReq.end()
}

// Proxy trigger and events to real backend
app.post("/api/v1/trigger", proxyToBackend)
app.get("/api/v1/events", proxyToBackend)
app.get("/api/v1/events/:jobId/download", proxyToBackend)
app.get("/api/v1/events/:jobId", proxyToBackend)
app.post("/api/v1/events/:jobId/cancel", proxyToBackend)

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

app.listen(PORT, () => {
  console.log(`Hybrid proxy server running on http://localhost:${PORT}`)
  console.log(`  Mocked:  /metadata/countries, /metadata/data-availability, /auth/me`)
  console.log(`  Proxied: /trigger, /events/* -> ${REAL_BACKEND}`)
  console.log(``)
  console.log(`  Bolivia data-availability: elegibilidad=true, pesaje=true`)
})
