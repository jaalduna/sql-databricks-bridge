# SQL-Databricks Bridge - Quick Start Guide

## Installation

```bash
# Clone the repository
cd /path/to/sql-databricks-bridge

# Install dependencies
poetry install

# Copy environment template
cp .env.example .env

# Edit .env with your credentials
```

## Configuration

### Environment Variables

Edit `.env` with your SQL Server and Databricks credentials:

```bash
# SQL Server
SQLSERVER_HOST=your-sql-server.database.windows.net
SQLSERVER_PORT=1433
SQLSERVER_DATABASE=your_database
SQLSERVER_USERNAME=your_username
SQLSERVER_PASSWORD=your_password

# Databricks
DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
DATABRICKS_TOKEN=your_personal_access_token
DATABRICKS_CATALOG=your_catalog
DATABRICKS_SCHEMA=your_schema
DATABRICKS_VOLUME=your_volume
```

### Permissions

Edit `config/permissions.yaml` to configure API tokens and table access:

```yaml
users:
  - token: "your-api-token"
    name: "your-project"
    permissions:
      - table: "dbo.YourTable"
        access: "read"
```

## Usage

### CLI Extraction

Extract data from SQL Server to Databricks:

```bash
# Extract all queries for Colombia
sql-databricks-bridge extract \
  --queries-path /path/to/queries \
  --config-path /path/to/config \
  --country Colombia \
  --destination /Volumes/catalog/schema/volume/raw

# Extract specific queries
sql-databricks-bridge extract \
  --queries-path ./queries \
  --config-path ./config \
  --country Brasil \
  --destination /Volumes/my_catalog/my_schema/my_volume \
  --query j_atoscompra_new \
  --query vw_artigoz

# Overwrite existing files
sql-databricks-bridge extract \
  --queries-path ./queries \
  --config-path ./config \
  --country Mexico \
  --destination /Volumes/catalog/schema/volume \
  --overwrite
```

### API Server

Start the API server:

```bash
# Development mode with auto-reload
sql-databricks-bridge serve --reload

# Production mode
sql-databricks-bridge serve --host 0.0.0.0 --port 8000
```

### API Endpoints

**Start extraction job:**

```bash
curl -X POST http://localhost:8000/extract \
  -H "Authorization: Bearer your-api-token" \
  -H "Content-Type: application/json" \
  -d '{
    "queries_path": "/path/to/queries",
    "config_path": "/path/to/config",
    "country": "Colombia",
    "destination": "/Volumes/catalog/schema/volume"
  }'
```

**Check job status:**

```bash
curl http://localhost:8000/jobs/{job_id} \
  -H "Authorization: Bearer your-api-token"
```

**Health check:**

```bash
curl http://localhost:8000/health/live
curl http://localhost:8000/health/ready
```

### Test Connection

Verify your credentials are working:

```bash
sql-databricks-bridge test-connection
```

### List Available Queries

See what queries are available in a directory:

```bash
sql-databricks-bridge list-queries --queries-path /path/to/queries
```

### Show Parameters

See resolved parameters for a country:

```bash
sql-databricks-bridge show-params \
  --config-path /path/to/config \
  --country Colombia
```

## Databricks → SQL Sync

The bridge also supports syncing data from Databricks back to SQL Server.

### 1. Create Events Table

Create the events table in Databricks:

```sql
CREATE TABLE IF NOT EXISTS bridge.events.bridge_events (
  event_id STRING,
  operation STRING,
  source_table STRING,
  target_table STRING,
  primary_keys ARRAY<STRING>,
  priority INT DEFAULT 0,
  status STRING DEFAULT 'pending',
  rows_expected INT,
  rows_affected INT,
  discrepancy INT,
  warning STRING,
  error_message STRING,
  filter_conditions MAP<STRING, STRING>,
  metadata MAP<STRING, STRING>,
  created_at TIMESTAMP DEFAULT current_timestamp(),
  processed_at TIMESTAMP
)
```

### 2. Insert Sync Events

Insert events to trigger sync operations:

```sql
-- INSERT operation
INSERT INTO bridge.events.bridge_events (
  event_id, operation, source_table, target_table, rows_expected
) VALUES (
  uuid(), 'INSERT', 'catalog.schema.my_data', 'dbo.MyTable', 1000
);

-- UPDATE operation (requires primary keys)
INSERT INTO bridge.events.bridge_events (
  event_id, operation, source_table, target_table, primary_keys
) VALUES (
  uuid(), 'UPDATE', 'catalog.schema.my_updates', 'dbo.MyTable', array('id')
);

-- DELETE operation
INSERT INTO bridge.events.bridge_events (
  event_id, operation, source_table, target_table, primary_keys, filter_conditions
) VALUES (
  uuid(), 'DELETE', 'catalog.schema.deletions', 'dbo.MyTable',
  array('id'), map('status', 'inactive')
);
```

### 3. Monitor Events

Check event status:

```sql
SELECT event_id, operation, status, rows_affected, error_message
FROM bridge.events.bridge_events
WHERE created_at > current_date() - INTERVAL 1 DAY
ORDER BY created_at DESC;
```

## Troubleshooting

### Connection Issues

```bash
# Test SQL Server connection
sql-databricks-bridge test-connection

# Check environment variables
env | grep -E "SQLSERVER_|DATABRICKS_"
```

### Permission Denied (403)

1. Check your token is in `config/permissions.yaml`
2. Verify the table is in your permissions list
3. Ensure the access level is correct (read/write/read_write)

### Query Parameters Missing

```bash
# Check what parameters are needed
sql-databricks-bridge list-queries --queries-path ./queries

# Check resolved parameters for your country
sql-databricks-bridge show-params --config-path ./config --country Colombia
```
