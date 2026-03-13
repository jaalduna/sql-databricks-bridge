# SQL-Databricks Bridge

Bidirectional data synchronization service between SQL Server and Databricks, with a desktop UI for calibration and eligibility workflows.

---

## Overview

Organizations using both SQL Server (operational/transactional) and Databricks (analytics/ML) need a reliable way to move data between them. This bridge provides:

- **SQL Server -> Databricks**: Extract tables via country-specific SQL queries, convert to Parquet, create Delta tables
- **Databricks -> SQL Server**: Event-driven sync (INSERT/UPDATE/DELETE) via a bridge events table
- **Calibration**: Trigger and monitor Databricks calibration jobs per country
- **Eligibility**: Run and track eligibility workflows per country
- **Desktop UI**: React + Tauri app for managing all operations visually

---

## Architecture

```
┌─────────────────────┐                              ┌─────────────────────┐
│    SQL SERVER        │                              │     DATABRICKS      │
│                      │    SQL -> Databricks         │                     │
│  Operational Tables  │  ──────────────────────────> │  Delta Tables       │
│  (9 countries,       │    Extract + Parquet + CTAS  │  (Unity Catalog)    │
│   5 servers)         │                              │                     │
│                      │    Databricks -> SQL         │  Calibration Jobs   │
│  Target Tables       │  <────────────────────────── │  Eligibility Jobs   │
│                      │    Event-Driven Sync         │  bridge_events      │
└──────────┬───────────┘                              └──────────┬──────────┘
           │                                                      │
           │         ┌────────────────────────────────┐           │
           │         │   SQL-DATABRICKS BRIDGE        │           │
           │         │        (FastAPI)                │           │
           │         │                                 │           │
           └─────────┤  REST API + WebSocket           ├───────────┘
                     │  Event Poller                   │
                     │  Calibration Launcher           │
                     │  Databricks Job Monitor         │
                     └───────────────┬─────────────────┘
                                     │
                     ┌───────────────┴─────────────────┐
                     │   Desktop UI (Tauri + React)     │
                     │                                  │
                     │  Sync Dashboard                  │
                     │  Calibration Manager             │
                     │  Eligibility Manager             │
                     └──────────────────────────────────┘
```

---

## Supported Countries

Argentina, Bolivia, Brasil, CAM, Chile, Colombia, Ecuador, Mexico, Peru

Each country has its own SQL Server instance, query set, and configuration.

---

## Project Structure

```
sql-databricks-bridge/
├── src/sql_databricks_bridge/       # Backend (FastAPI)
│   ├── api/routes/                  # REST endpoints
│   │   ├── extract.py               #   POST /extract
│   │   ├── sync.py                  #   POST /sync
│   │   ├── jobs.py                  #   GET /jobs
│   │   ├── trigger.py               #   POST /trigger (calibration)
│   │   ├── eligibility.py           #   Eligibility endpoints
│   │   ├── pipeline.py              #   Pipeline status
│   │   ├── databricks_jobs.py       #   Databricks job management
│   │   ├── health.py                #   Health checks
│   │   ├── metadata.py              #   Country/query metadata
│   │   └── tags.py                  #   Version tags
│   ├── auth/                        # Azure AD + token auth
│   ├── cli/commands.py              # Typer CLI
│   ├── core/                        # Business logic
│   │   ├── extractor.py             #   SQL extraction
│   │   ├── delta_writer.py          #   Delta table creation
│   │   ├── calibration_launcher.py  #   Launch Databricks jobs
│   │   ├── calibration_tracker.py   #   Track job progress
│   │   ├── databricks_monitor.py    #   Monitor running jobs
│   │   ├── country_query_loader.py  #   Country-aware query resolution
│   │   ├── diff_sync.py             #   Differential sync
│   │   └── pipeline_tracker.py      #   Pipeline state tracking
│   ├── db/                          # Database clients
│   │   ├── sql_server.py            #   SQL Server (pyodbc)
│   │   ├── databricks.py            #   Databricks SDK
│   │   └── local_store.py           #   SQLite local persistence
│   ├── sync/                        # Databricks -> SQL sync
│   │   ├── poller.py                #   Event table poller
│   │   ├── operations.py            #   INSERT/UPDATE/DELETE
│   │   └── retry.py                 #   Exponential backoff
│   ├── models/                      # Pydantic models
│   └── main.py                      # FastAPI app + lifespan
│
├── frontend/                        # Desktop UI
│   ├── src/
│   │   ├── modules/
│   │   │   ├── sync/                #   Dashboard, history, event detail
│   │   │   ├── calibracion/         #   Calibration management per country
│   │   │   └── elegibilidad/        #   Eligibility workflows
│   │   ├── components/              #   Shared UI components
│   │   ├── hooks/                   #   Shared React hooks
│   │   └── lib/                     #   API client, auth config
│   └── src-tauri/                   # Tauri desktop wrapper
│
├── queries/                         # SQL query templates
│   ├── countries/{country}/*.sql    #   Per-country queries (9 countries)
│   └── servers/{server}/*.sql       #   Per-server queries (5 servers)
│
├── config/                          # YAML configuration
│   ├── countries/{country}.yaml     #   Per-country settings
│   ├── servers/{server}.yaml        #   SQL Server connection configs
│   ├── stages.yaml                  #   Extraction stage definitions
│   └── permissions.yaml             #   API token permissions
│
├── sdk/                             # Python SDK for Databricks jobs
├── scripts/                         # Utility scripts
├── tests/                           # Unit + integration tests
├── deploy/                          # Deployment configs
├── vendor/                          # Vendored wheels
└── docs/                            # Documentation
```

---

## Quick Start

### Backend

```bash
# Install
poetry install
cp .env.example .env   # Edit with your credentials

# Start API server
bridge serve

# Or use start script
./start.sh
```

### Frontend (Desktop App)

```bash
cd frontend
npm install

# Dev mode (browser)
npm run dev

# Desktop app
npm run tauri:dev
```

### CLI

```bash
# Extract all tables for a country
bridge extract --queries-path ./queries --country bolivia --lookback-months 24

# With custom destination
bridge extract --queries-path ./queries --country chile --destination kpi_dev_01.chile

# List available queries
bridge list-queries --country colombia

# List countries and their servers
bridge list-countries

# Test connections
bridge test-connection
```

---

## Data Flows

### Flow 1: SQL Server -> Databricks (Extraction)

1. Client requests extraction via CLI, API, or UI
2. Bridge resolves country-specific queries from `queries/countries/{country}/`
3. Executes SQL with `{lookback_months}` parameter substitution
4. Converts results to Parquet via Polars
5. Uploads to Databricks Volume staging path
6. Creates Delta table via `CREATE OR REPLACE TABLE ... AS SELECT * FROM read_files()`

### Flow 2: Databricks -> SQL Server (Event-Driven Sync)

1. Databricks job inserts event into `bridge_events` table
2. Bridge poller detects new events (every 10s)
3. Reads source data from Databricks
4. Executes INSERT/UPDATE/DELETE on SQL Server

### Flow 3: Calibration

1. User selects country + parameters in UI
2. Backend triggers Databricks job via Jobs API
3. Monitor polls job status, streams progress via WebSocket
4. Results and history tracked in local SQLite store

### Flow 4: Eligibility

1. User configures eligibility run per country
2. Backend orchestrates eligibility workflow
3. Status tracked and displayed in UI

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/extract` | Start extraction job |
| GET | `/jobs/{id}` | Get job status |
| GET | `/jobs` | List all jobs |
| DELETE | `/jobs/{id}` | Cancel job |
| POST | `/sync` | Trigger sync operation |
| POST | `/trigger/calibration` | Launch calibration job |
| GET | `/trigger/calibration/status` | Calibration job status |
| GET | `/eligibility/*` | Eligibility endpoints |
| GET | `/pipeline/status` | Pipeline stage status |
| GET | `/metadata/countries` | Available countries |
| GET | `/health/live` | Liveness probe |
| GET | `/health/ready` | Readiness probe |

---

## Country-Aware Query System

Queries are organized per-country under `queries/countries/{country}/`:

```
queries/countries/
├── argentina/     # ~35 queries
├── bolivia/       # ~40 queries
├── brasil/        # ~40 queries
├── cam/           # ~30 queries
├── chile/         # ~40 queries
├── colombia/      # ~40 queries
├── ecuador/       # ~35 queries
├── mexico/        # ~40 queries
└── peru/          # ~35 queries
```

**Time-based filtering** — fact queries use `{lookback_months}` placeholder:
```sql
SELECT * FROM j_atoscompra_new
WHERE periodo >= FORMAT(DATEADD(MONTH, -{lookback_months}, GETDATE()), 'yyyyMMdd')
```

Dimension queries extract all data (no time filter).

---

## Configuration

### Environment Variables

```bash
# SQL Server (fallback if kantar_db_handler not available)
SQLSERVER_HOST=your-server.database.windows.net
SQLSERVER_DATABASE=your_database
SQLSERVER_USERNAME=your_user
SQLSERVER_PASSWORD=your_password

# Databricks
DATABRICKS_HOST=https://workspace.azuredatabricks.net
DATABRICKS_TOKEN=your_token
DATABRICKS_CATALOG=your_catalog
```

See `.env.example` for all available settings.

### Permissions (`config/permissions.yaml`)

```yaml
users:
  - token: "your-api-token"
    name: "your-project"
    permissions:
      - table: "dbo.YourTable"
        access: "read_write"
        max_delete_rows: 10000
```

---

## SDK (for Databricks Jobs)

```python
from sql_databricks_bridge_sdk import BridgeEventsClient

client = BridgeEventsClient()
event_id = client.create_insert_event(
    source_table="catalog.schema.source",
    target_table="dbo.Target",
    primary_keys=["id"]
)
result = client.wait_for_completion(event_id)
```

See [SDK README](sdk/README.md) for details.

---

## Development

```bash
# Run tests
poetry run pytest

# Unit tests only
poetry run pytest tests/unit/ -v

# Linting
poetry run ruff check src/

# Type checking
poetry run mypy src/

# Frontend tests
cd frontend && npm test
```

---

## License

Proprietary - Kantar Worldpanel
