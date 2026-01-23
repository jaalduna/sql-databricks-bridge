# SQL-Databricks Bridge - Architecture

## Overview

SQL-Databricks Bridge provides bidirectional data synchronization between SQL Server and Databricks:

1. **Extraction Flow** (SQL Server → Databricks): API + CLI for extracting data from SQL Server and uploading to Databricks Volumes as Parquet files.

2. **Sync Flow** (Databricks → SQL Server): Polling-based event system for syncing data changes back to SQL Server.

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SQL-Databricks Bridge                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────────┐ │
│  │   CLI       │    │  REST API   │    │    Event Poller         │ │
│  │  (Typer)    │    │  (FastAPI)  │    │    (10s interval)       │ │
│  └──────┬──────┘    └──────┬──────┘    └───────────┬─────────────┘ │
│         │                  │                        │               │
│         └────────┬─────────┴────────────────────────┘               │
│                  │                                                   │
│         ┌────────▼────────┐                                         │
│         │    Core Layer   │                                         │
│         ├─────────────────┤                                         │
│         │ • Query Loader  │                                         │
│         │ • Param Resolver│                                         │
│         │ • Extractor     │                                         │
│         │ • Uploader      │                                         │
│         └────────┬────────┘                                         │
│                  │                                                   │
│    ┌─────────────┴─────────────┐                                    │
│    │                           │                                    │
│ ┌──▼───────────┐    ┌──────────▼───────────┐                       │
│ │ SQL Server   │    │    Databricks        │                       │
│ │ Client       │    │    Client            │                       │
│ └──────────────┘    └──────────────────────┘                       │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
          │                           │
          ▼                           ▼
    ┌───────────┐            ┌─────────────────┐
    │SQL Server │            │   Databricks    │
    │ Database  │            │ Unity Catalog   │
    └───────────┘            │ + Volumes       │
                             └─────────────────┘
```

## Module Structure

### `/src/sql_databricks_bridge/`

```
├── main.py                 # FastAPI application entry point
├── api/
│   ├── routes/
│   │   ├── extract.py      # POST /extract - Start extraction
│   │   ├── jobs.py         # GET /jobs/{id} - Job status
│   │   └── health.py       # Health check endpoints
│   ├── dependencies.py     # Auth dependencies
│   └── schemas.py          # Pydantic models
├── cli/
│   └── commands.py         # Typer CLI commands
├── core/
│   ├── config.py           # pydantic-settings configuration
│   ├── query_loader.py     # SQL file discovery
│   ├── param_resolver.py   # YAML parameter merging
│   ├── extractor.py        # SQL query execution
│   └── uploader.py         # Databricks file upload
├── sync/
│   ├── poller.py           # Event polling loop
│   ├── operations.py       # INSERT/UPDATE/DELETE logic
│   ├── validators.py       # PK and limit validation
│   └── retry.py            # Exponential backoff
├── auth/
│   ├── token.py            # Token utilities
│   ├── permissions.py      # Table-level access control
│   ├── loader.py           # YAML hot reload
│   └── audit.py            # Security audit logging
├── db/
│   ├── sql_server.py       # SQL Server connection
│   └── databricks.py       # Databricks SDK wrapper
└── models/
    └── events.py           # Sync event models
```

## Data Flow

### Extraction Flow (SQL → Databricks)

```
1. Request received (API or CLI)
         │
         ▼
2. Query Loader discovers .sql files
         │
         ▼
3. Param Resolver merges YAML configs
   (common_params.yaml + {country}.yaml)
         │
         ▼
4. Extractor executes formatted SQL
         │
         ▼
5. Results converted to Polars DataFrame
         │
         ▼
6. Uploader writes Parquet to Databricks Volume
         │
         ▼
7. Job status updated
```

### Sync Flow (Databricks → SQL)

```
1. Poll bridge_events table every 10s
         │
         ▼
2. Fetch pending events (ORDER BY priority DESC)
         │
         ▼
3. Validate event (PKs, limits)
         │
         ▼
4. Read source data from Databricks
         │
         ▼
5. Execute operation on SQL Server
   ├── INSERT: Bulk insert
   ├── UPDATE: Row-by-row with PKs
   └── DELETE: With limit check
         │
         ▼
6. Update event status (completed/failed)
```

## Authentication

```
Request with Bearer token
         │
         ▼
Extract token from Authorization header
         │
         ▼
Look up user in permissions.yaml
         │
         ▼
Validate table-level permissions
         │
         ▼
Audit log (success/failure)
```

### Permission Levels

| Level | Read | Write | Delete |
|-------|------|-------|--------|
| `read` | ✓ | ✗ | ✗ |
| `write` | ✗ | ✓ | ✓* |
| `read_write` | ✓ | ✓ | ✓* |

*DELETE limited by `max_delete_rows`

## Configuration

### Environment Variables

```bash
# Service
ENVIRONMENT=development|staging|production
DEBUG=true|false
LOG_LEVEL=DEBUG|INFO|WARNING|ERROR

# SQL Server
SQLSERVER_HOST=hostname
SQLSERVER_PORT=1433
SQLSERVER_DATABASE=dbname
SQLSERVER_USERNAME=user
SQLSERVER_PASSWORD=secret

# Databricks
DATABRICKS_HOST=https://workspace.azuredatabricks.net
DATABRICKS_TOKEN=token
# Or Service Principal:
DATABRICKS_CLIENT_ID=client_id
DATABRICKS_CLIENT_SECRET=secret
DATABRICKS_CATALOG=catalog
DATABRICKS_SCHEMA=schema
DATABRICKS_VOLUME=volume

# Polling
POLLING_INTERVAL_SECONDS=10
MAX_EVENTS_PER_POLL=100
```

### YAML Configuration

**permissions.yaml**

```yaml
users:
  - token: "project-token"
    name: "project-name"
    permissions:
      - table: "dbo.Table"
        access: "read_write"
        max_delete_rows: 10000
```

**common_params.yaml**

```yaml
flg_scanner: flg_scanner
factor: factor_rw1 as factor_rw
```

**{Country}.yaml**

```yaml
country_code: CO
database: KWP_Colombia
tables:
  purchases_table: J_AtosCompra_CO
```

## Events Table Schema

```sql
CREATE TABLE bridge.events.bridge_events (
  event_id STRING,
  operation STRING,              -- INSERT/UPDATE/DELETE
  source_table STRING,           -- catalog.schema.table
  target_table STRING,           -- schema.table
  primary_keys ARRAY<STRING>,    -- Required for UPDATE/DELETE
  priority INT DEFAULT 0,        -- Higher = processed first
  status STRING DEFAULT 'pending',
  rows_expected INT,
  rows_affected INT,
  discrepancy INT,               -- expected - affected
  warning STRING,
  error_message STRING,
  filter_conditions MAP<STRING, STRING>,
  metadata MAP<STRING, STRING>,
  created_at TIMESTAMP,
  processed_at TIMESTAMP
)
```

## Error Handling

### Retry Strategy

- **Max attempts**: 3
- **Backoff**: Exponential with jitter
- **Base delay**: 1 second
- **Max delay**: 60 seconds

### Discrepancy Reporting

When `rows_affected != rows_expected`:
- `discrepancy` field populated
- Warning logged
- Event still marked as completed

### Delete Protection

- `max_delete_rows` enforced per user/table
- DELETE blocked if count exceeds limit
- Event marked as `blocked` status

## Monitoring

### Health Endpoints

- `GET /health/live` - Service running
- `GET /health/ready` - Dependencies connected
- `GET /health/startup` - Service started

### Audit Logging

All security events logged as JSON:

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "event_type": "auth_success",
  "user_name": "project-name",
  "source_ip": "10.0.0.1",
  "resource": "dbo.Table",
  "action": "read"
}
```

## Deployment

### Docker

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml poetry.lock ./
RUN pip install poetry && poetry install --no-dev
COPY src/ ./src/
COPY config/ ./config/
CMD ["poetry", "run", "sql-databricks-bridge", "serve"]
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
spec:
  containers:
    - name: bridge
      image: sql-databricks-bridge:latest
      ports:
        - containerPort: 8000
      livenessProbe:
        httpGet:
          path: /health/live
          port: 8000
      readinessProbe:
        httpGet:
          path: /health/ready
          port: 8000
      env:
        - name: SQLSERVER_PASSWORD
          valueFrom:
            secretKeyRef:
              name: bridge-secrets
              key: sql-password
```
