# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Is

SQL-Databricks Bridge is a bidirectional data sync service between SQL Server and Databricks Unity Catalog. It has three main components:

1. **Python backend** (FastAPI) — REST API + event poller + CLI for data extraction and sync
2. **React frontend** (Vite + Tailwind + shadcn/ui) — Dashboard for triggering calibration pipelines, deployed as both a web app and a Tauri desktop app
3. **Python SDK** — Client library for Databricks notebooks/jobs to interact with the bridge

## Build & Run Commands

### Backend (Python / Poetry)

```bash
poetry install                          # Install all dependencies
PYTHONPATH=src pytest tests/unit/ -v    # Run unit tests (no external connections)
PYTHONPATH=src pytest tests/ -v         # Run all tests (integration tests need DATABRICKS_* env vars)
PYTHONPATH=src pytest tests/unit/test_operations.py -v                # Single test file
PYTHONPATH=src pytest tests/unit/test_operations.py::TestSyncOperatorInsert -v  # Single test class
poetry run ruff check src/              # Lint
poetry run mypy src/                    # Type check
bridge serve                            # Start API server (port 8000)
bridge extract --queries-path ./queries --country bolivia  # CLI extraction
```

### Frontend (npm)

```bash
cd frontend
npm install                # Install dependencies
npm run dev                # Dev server (Vite, port 5173)
npm run build              # Production build (tsc + vite)
npm run test               # Run tests (vitest)
npm run test:watch         # Watch mode tests
npm run lint               # ESLint
npm run tauri:dev          # Tauri desktop dev mode
npm run tauri:build        # Build Tauri desktop app
```

## Architecture

### Data Flow — Two Directions

**SQL Server -> Databricks (Extraction):**
`SQLServerClient` -> `Extractor` (Polars DataFrames) -> `DeltaTableWriter` (stage-then-CTAS to Delta tables)

**Databricks -> SQL Server (Event-Driven Sync):**
Databricks job writes to `bridge_events` table -> `EventPoller` detects events -> `SyncOperator` executes INSERT/UPDATE/DELETE on SQL Server

### Backend Layers (`src/sql_databricks_bridge/`)

- **`core/`** — Business logic: `extractor.py` (SQL extraction), `delta_writer.py` (Databricks writes via stage-then-CTAS), `diff_sync.py` (2-level fingerprint differential sync), `config.py` (pydantic-settings from `.env`), `pipeline_tracker.py` + `calibration_launcher.py` (calibration pipeline orchestration)
- **`api/routes/`** — FastAPI endpoints under `/api/v1`: `extract.py`, `sync.py`, `trigger.py`, `pipeline.py`, `health.py`, `auth.py`, `metadata.py`, `tags.py`, `databricks_jobs.py`, `diff_sync_schedule.py`
- **`sync/`** — Databricks-to-SQL sync: `operations.py` (SyncOperator), `poller.py` (EventPoller), `validators.py`, `retry.py`
- **`db/`** — Database clients: `sql_server.py` (pyodbc/SQLAlchemy), `databricks.py` (Databricks SDK), `jobs_table.py`, `local_store.py` (SQLite for local job state)
- **`auth/`** — Token auth + Azure AD: `token.py`, `azure_ad.py`, `permissions.py`, `authorized_users.py`
- **`models/`** — Pydantic models: `events.py` (SyncEvent/SyncOperation), `pipeline.py` (CalibrationPipeline steps), `calibration.py`
- **`cli/commands.py`** — Typer CLI (`bridge` command): extract, serve, diff-sync, test-connection, list-queries, list-countries
- **`data/`** — Bundled YAML configs (stages.yaml, permissions.yaml, authorized_users.yaml)
- **`main.py`** — FastAPI app factory with lifespan (starts EventPoller + DatabricksJobMonitor)

### Country-Aware Query System

Queries live in two mirrored directories that must be kept in sync:
- `queries/countries/{country}/` — used in development (via `QUERIES_PATH` env var)
- `config/countries/{country}/` — used by the compiled executable on the remote server

Resolution priority: country-specific overrides > common fallback. `CountryAwareQueryLoader` handles this. Fact queries use `{lookback_months}` placeholder for time filtering.

### Multi-Country SQL Server

Each country connects to a different SQL Server instance via `kantar_db_handler` (vendored wheel). `SQLServerClient(country="bolivia")` auto-resolves the correct server/database. Fallback to `.env` if not installed.

### Differential Sync (`core/diff_sync.py` + `core/fingerprint.py`)

2-level fingerprint comparison for incremental extraction:
1. Level 1 fingerprints (e.g. by `periodo`) identify changed groups
2. Level 2 fingerprints (e.g. by `periodo + idproduto`) identify changed rows within groups
3. Only changed slices are extracted and replaced (DELETE + INSERT)

**How it works:**
- Checksums are computed on **SQL Server** using `CHECKSUM_AGG(CHECKSUM(...))` grouped by period/product
- Previous checksums are stored in a **Databricks Delta metadata table**
- Python compares current vs stored checksums to find what changed
- Only changed slices are extracted from SQL Server and written to Databricks via DELETE + INSERT
- Historical data in Databricks is preserved (no CREATE OR REPLACE)

**Key files:**
- `core/diff_sync.py` — orchestrator (Level 1 → Level 2 → extract → write)
- `core/fingerprint.py` — SQL Server checksum computation + Databricks fingerprint storage
- `core/diff_sync_runner.py` — shared logic for CLI and API (HTTP-based trigger + poll)
- `api/routes/diff_sync_schedule.py` — API endpoints for periodic scheduling

#### Configuration (`.env`)

```bash
# Tables that auto-use diff sync (server-side default)
# Format: table:level1_col:level2_col[:mutable_months]
# mutable_months: only check last N months for changes (older assumed unchanged)
DIFF_SYNC_TABLES=j_atoscompra_new:periodo:idproduto,vw_artigoz:idsector:idproduto,rg_domicilios_pesos:periodo:idpeso,domicilio_posse_bens:periodo:idposse_bem,mordom:periodo:iddomicilio,hato_cabecalho:periodo:idproduto:2

# Checksum column subsets for wide tables (avoids slow CHECKSUM(*) on many columns)
# Format: table:col1+col2+col3 (comma-separated for multiple tables)
# If not defined for a table, uses CHECKSUM(*) by default
DIFF_SYNC_CHECKSUM_COLUMNS=j_atoscompra_new:quantidade+preco+unidades_packs+value_pm+coef_01+coef_02+coef_03+FACTOR_RW1+idato
```

**Important:** Diff-sync tables require a `periodo` column in their SQL query. Tables without a native `periodo` column must add a computed one:
- `rg_domicilios_pesos`: `ano*100+messem as periodo`
- `domicilio_posse_bens`: `YEAR(data)*100+MONTH(data) as periodo`
- `mordom`: `ano as periodo`
- `hato_cabecalho` (Mexico): `CONVERT(VARCHAR(6), data_compra, 112) AS periodo`

#### CLI Command

```bash
# All countries, 15-min loop (requires server running)
bridge diff-sync

# Single country, one-shot
bridge diff-sync --country bolivia --once

# Custom interval and API URL
bridge diff-sync --api-url http://myworkspace.kantar.com:8000/api/v1 --interval 30
```

Parameters: `--country`/`-c`, `--interval`/`-i` (minutes), `--once`, `--api-url`, `--lookback-months`, `--stage`, `--log-file`

#### API Endpoints

```
POST   /api/v1/diff-sync/start    — Start periodic diff sync (all or specific countries)
POST   /api/v1/diff-sync/stop     — Stop the periodic loop
GET    /api/v1/diff-sync/status   — Current state, last results, next run
GET    /api/v1/diff-sync/history  — Past round results
```

#### Databricks SQL Warehouse

All Delta operations (DELETE, INSERT, fingerprint queries) require a running SQL Warehouse (`DATABRICKS_WAREHOUSE_ID`). With serverless + auto-stop, the warehouse starts on demand (~7s cold start) and stops when idle. For periodic diff-sync, set auto-stop to 1 minute to minimize cost.

### Calibration Pipeline (`models/pipeline.py`)

6-step orchestrated pipeline: sync -> copy_bronze -> merge -> simulate_weights -> simulate_kpis -> calculate_targets. Each step maps to a Databricks Asset Bundle job. `CalibrationJobLauncher` triggers jobs, `DatabricksJobMonitor` polls run status.

### Frontend (`frontend/`)

React 19 + React Router + TanStack Query + shadcn/ui. Auth via Azure AD (MSAL) or bypass mode (`VITE_AUTH_BYPASS=true`). API client at `src/lib/api.ts` hits `/api/v1`. Pages: Dashboard (country cards), History (job list), Calibration (pipeline view), EventDetail.

### SDK (`sdk/`)

`BridgeEventsClient` — writes sync events to Databricks Delta table. `BridgeClient` (api_client.py) — REST API wrapper. Used from Databricks notebooks/jobs.

## Key Configuration

- **Remote server**: `http://myworkspace.kantar.com:8000/` (production/remote bridge API)
- **Backend env**: `.env` at project root (see `.env.example`). Key vars: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_WAREHOUSE_ID`, `SQLSERVER_*`
- **Frontend env**: `frontend/.env` with `VITE_BRIDGE_API_URL`, `VITE_AZURE_AD_CLIENT_ID`, `VITE_AZURE_AD_TENANT_ID`
- **Permissions**: `src/sql_databricks_bridge/data/permissions.yaml` (bundled) or `config/permissions.yaml`
- **Ruff**: line-length 100, target py311, rules E/F/I/UP/B/SIM
- **pytest**: asyncio_mode=auto, testpaths=["tests"], -v --tb=short

## Test Conventions

- Unit tests mock both SQL Server and Databricks clients (see `tests/conftest.py` fixtures: `mock_sql_client`, `mock_databricks_client`, `mock_delta_writer`)
- Integration tests use `@requires_databricks` marker and need real `DATABRICKS_*` env vars
- DataFrames use Polars throughout (never pandas)
- `PYTHONPATH=src` is required to run tests from project root

## Git

- Remote is named `github` (not `origin`)
- Main branch: `main`, development branch: `develop`
