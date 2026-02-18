# SQL-Databricks Bridge - Server Deployment Guide

## Prerequisites

Install the following on the Windows server before proceeding:

1. **Microsoft ODBC Driver 17 (or 18) for SQL Server**
   - Download: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
   - Required for SQL Server connectivity

2. **Microsoft Visual C++ Redistributable (2015-2022)**
   - Download: https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist
   - Required by the compiled executable

## Folder Structure

```
deploy/
├── sql-databricks-bridge.exe   # Main executable
├── .env.template               # Configuration template
├── .env                        # Your configuration (create from template)
├── queries/                    # SQL query definitions
│   ├── countries/
│   │   ├── argentina/
│   │   ├── bolivia/
│   │   ├── brasil/
│   │   ├── cam/
│   │   ├── chile/
│   │   ├── colombia/
│   │   ├── ecuador/
│   │   ├── mexico/
│   │   └── peru/
│   └── servers/                # Server-wide shared tables
│       ├── KTCLSQL001/
│       ├── KTCLSQL002/
│       ├── KTCLSQL003/
│       ├── KTCLSQL004/
│       └── KTCLSQL005/
└── .bridge_data/               # Auto-created at runtime (SQLite DB)
    └── jobs.db
```

## Configuration

1. Copy the template to create your config file:

```cmd
copy .env.template .env
```

2. Edit `.env` and fill in the 4 required Databricks variables:

| Variable | Requerida | Descripcion | Default |
|---|---|---|---|
| `DATABRICKS_HOST` | Si | URL del workspace Databricks | — |
| `DATABRICKS_TOKEN` | Si | Personal access token de Databricks | — |
| `DATABRICKS_WAREHOUSE_ID` | Si | ID del SQL Warehouse | — |
| `DATABRICKS_CATALOG` | Si | Nombre del catalogo Unity Catalog | — |

Las demas variables son opcionales y tienen defaults de produccion. SQL Server host y database se resuelven automaticamente por pais via kantar_db_handler. Ver `.env.template` para la lista completa.

## Running the Service

### Manual Start (for testing)

Open a terminal **in this folder** and run:

```cmd
sql-databricks-bridge.exe serve --host 0.0.0.0 --port 8000
```

### Verify It's Running

```cmd
curl http://localhost:8000/api/v1/health/live
```

Expected response:

```json
{"status":"ok"}
```

You can also open `http://localhost:8000/docs` in a browser to see the API documentation.

### Test Connectivity

Before running the full service, verify that both SQL Server and Databricks connections work:

```cmd
sql-databricks-bridge.exe test-connection
```

## Features

### Rolling Months Filter

Server queries (`queries/servers/KTCLSQL001-005/`) use a `{lookback_months}` placeholder to limit data extraction to a rolling time window. The default is **24 months**, configurable via:

- `.env`: `LOOKBACK_MONTHS=24` (server-wide default)
- Frontend: "Rolling Months" input field per sync job
- API: `lookback_months` parameter in POST `/trigger`

### Parallel Query Execution

Sync jobs run multiple SQL Server queries concurrently (default: 4 parallel). Configure via `MAX_PARALLEL_QUERIES` in `.env`.

### SQLite Crash Recovery

Job progress is persisted to a local SQLite database (`.bridge_data/jobs.db`). On server restart, in-flight jobs are automatically marked as failed so the frontend reflects the correct state.

### Server-Wide Tables

In addition to per-country queries, the bridge supports server-wide shared tables (e.g. `loc_psdata_compras`, `loc_psdata_procesado` from `PS_LATAM`). These are synced by selecting a server name (e.g. `KTCLSQL001`) instead of a country.

## Installing as a Windows Service (NSSM)

[NSSM](https://nssm.cc/) (Non-Sucking Service Manager) lets you run the bridge as a Windows service that starts automatically on boot.

### 1. Download NSSM

Download from https://nssm.cc/download and extract `nssm.exe` to a known location (e.g. `C:\tools\nssm.exe`).

### 2. Install the Service

Open an **Administrator** command prompt:

```cmd
nssm install SQLDatabricksBridge
```

In the NSSM GUI that opens, configure:

| Field | Value |
|---|---|
| **Path** | `C:\path\to\deploy\sql-databricks-bridge.exe` |
| **Startup directory** | `C:\path\to\deploy` |
| **Arguments** | `serve --host 0.0.0.0 --port 8000` |

Replace `C:\path\to\deploy` with the actual path where you placed this folder.

On the **I/O** tab (optional):
- Set **Output (stdout)** to `C:\path\to\deploy\logs\service.log`
- Set **Error (stderr)** to `C:\path\to\deploy\logs\error.log`

Click **Install service**.

### 3. Start the Service

```cmd
nssm start SQLDatabricksBridge
```

### 4. Manage the Service

```cmd
nssm status SQLDatabricksBridge    # Check status
nssm stop SQLDatabricksBridge      # Stop service
nssm restart SQLDatabricksBridge   # Restart service
nssm remove SQLDatabricksBridge    # Uninstall service
```

## CLI Commands

| Command | Description |
|---|---|
| `serve` | Start the API server |
| `version` | Show the installed version |
| `test-connection` | Verify SQL Server and Databricks connectivity |

Example:

```cmd
sql-databricks-bridge.exe version
sql-databricks-bridge.exe test-connection
```

## Troubleshooting

### "ODBC Driver not found"
- Install Microsoft ODBC Driver 17 or 18 for SQL Server
- Verify the `SQLSERVER_DRIVER` value in `.env` matches the installed driver version

### Connection refused on port 8000
- Check that the service is running: `nssm status SQLDatabricksBridge`
- Check Windows Firewall allows inbound TCP on port 8000
- Review logs in `logs/error.log` (if configured)

### "charmap codec can't encode character" error
- This is a cosmetic issue with Rich console output on some Windows terminals
- Set `PYTHONIOENCODING=utf-8` as an environment variable, or run in Windows Terminal instead of cmd.exe

### Databricks token expired
- Generate a new personal access token in the Databricks workspace
- Update `DATABRICKS_TOKEN` in `.env`
- Restart the service: `nssm restart SQLDatabricksBridge`

### SQLite database issues
- The job database is stored in `.bridge_data/jobs.db` (auto-created)
- To reset, stop the service and delete the `.bridge_data` folder — it will be recreated on next start

### Updating to a new version
```cmd
nssm stop SQLDatabricksBridge
:: Replace the exe with the new version manually
copy /Y new-sql-databricks-bridge.exe sql-databricks-bridge.exe
nssm start SQLDatabricksBridge
```
