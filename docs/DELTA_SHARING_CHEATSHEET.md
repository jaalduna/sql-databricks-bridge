# Delta Sharing Quick Reference

## Setup Commands

### Enable Delta Sharing for Bolivia

```bash
# Dry run (preview what will happen)
python scripts/enable_delta_sharing.py \
  --country bolivia \
  --share-name bolivia_data_share \
  --dry-run

# Create share with all tables
python scripts/enable_delta_sharing.py \
  --country bolivia \
  --share-name bolivia_data_share

# Share only specific tables
python scripts/enable_delta_sharing.py \
  --country bolivia \
  --share-name bolivia_core_share \
  --include-tables j_atoscompra_new hato_cabecalho rg_panelis

# Exclude sensitive tables
python scripts/enable_delta_sharing.py \
  --country bolivia \
  --share-name bolivia_public_share \
  --exclude-tables sensitive_table1 sensitive_table2
```

### Verify Setup

```bash
# Check share configuration
python scripts/verify_delta_sharing.py --share bolivia_data_share

# Run full verification with query test
python scripts/verify_delta_sharing.py --share bolivia_data_share --test-query

# List all shares
python scripts/enable_delta_sharing.py --list-shares
```

## Databricks CLI Commands

### Shares

```bash
# List all shares
databricks shares list

# Get share details
databricks shares get --share bolivia_data_share

# Delete share
databricks shares delete --share bolivia_data_share

# List share permissions
databricks shares list-permissions --share bolivia_data_share
```

### Recipients

```bash
# List all recipients
databricks recipients list

# Create internal recipient (same org)
databricks recipients create \
  --name analytics_workspace \
  --comment "Analytics team workspace"

# Create external recipient
databricks recipients create \
  --name external_partner \
  --comment "External data consumer" \
  --authentication-type DATABRICKS

# Get recipient details (includes activation URL)
databricks recipients get --recipient analytics_workspace

# Delete recipient
databricks recipients delete --recipient analytics_workspace
```

### Grant/Revoke Access

```bash
# Grant access to share
databricks shares grant \
  --share bolivia_data_share \
  --recipient analytics_workspace

# Revoke access
databricks shares revoke \
  --share bolivia_data_share \
  --recipient analytics_workspace
```

## SQL Commands

### Query Shared Data (Recipient Side)

```sql
-- Create catalog from share
CREATE CATALOG bolivia_shared
USING SHARE `<provider>`.bolivia_data_share;

-- List tables
SHOW TABLES IN bolivia_shared.bolivia;

-- Query data
SELECT * FROM bolivia_shared.bolivia.j_atoscompra_new
WHERE Periodo = 202401
LIMIT 100;

-- Aggregate query
SELECT
  Periodo,
  COUNT(DISTINCT IdDomicilio) as households,
  SUM(Quantidade) as total_packs,
  SUM(Valor) as total_value
FROM bolivia_shared.bolivia.j_atoscompra_new
GROUP BY Periodo
ORDER BY Periodo DESC;
```

### Manage Shares (Provider Side)

```sql
-- Add table to existing share
ALTER SHARE bolivia_data_share
ADD TABLE `000-sql-databricks-bridge`.`bolivia`.`new_table`;

-- Remove table from share
ALTER SHARE bolivia_data_share
REMOVE TABLE `000-sql-databricks-bridge`.`bolivia`.`old_table`;

-- Grant access to recipient
GRANT SELECT ON SHARE bolivia_data_share TO RECIPIENT `analytics_workspace`;

-- Revoke access
REVOKE SELECT ON SHARE bolivia_data_share FROM RECIPIENT `analytics_workspace`;
```

### Enable Change Data Feed

```sql
-- Enable CDC on table for incremental sync
ALTER TABLE `000-sql-databricks-bridge`.`bolivia`.`j_atoscompra_new`
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Query changes (recipient side)
SELECT * FROM table_changes('bolivia_shared.bolivia.j_atoscompra_new', 0)
WHERE _change_type IN ('insert', 'update', 'delete');
```

### Time Travel Queries

```sql
-- Query historical data by timestamp
SELECT * FROM bolivia_shared.bolivia.j_atoscompra_new
TIMESTAMP AS OF '2026-01-01 00:00:00';

-- Query by version
SELECT * FROM bolivia_shared.bolivia.j_atoscompra_new
VERSION AS OF 42;

-- Show table history
DESCRIBE HISTORY bolivia_shared.bolivia.j_atoscompra_new;
```

## Python API

### Create and Manage Shares

```python
from databricks.sdk import WorkspaceClient

client = WorkspaceClient()

# Create share
share = client.shares.create(
    name="bolivia_data_share",
    comment="Bolivia market data"
)

# Add table to share
from databricks.sdk.service.sharing import (
    SharedDataObject,
    SharedDataObjectUpdate
)

client.shares.update(
    name="bolivia_data_share",
    updates=[
        SharedDataObjectUpdate(
            action="ADD",
            data_object=SharedDataObject(
                name="000-sql-databricks-bridge.bolivia.j_atoscompra_new",
                data_object_type="TABLE"
            )
        )
    ]
)

# Grant access
client.shares.update_permissions(
    name="bolivia_data_share",
    changes=[{
        "principal": "analytics_workspace",
        "add": ["SELECT"]
    }]
)

# List share details
share = client.shares.get("bolivia_data_share")
print(f"Tables: {len(share.objects)}")
for obj in share.objects:
    print(f"  - {obj.name}")
```

### Consume Shared Data (Recipient)

```python
import delta_sharing

# Load share profile
profile = "/path/to/config.share"

# Create client
sharing_client = delta_sharing.SharingClient(profile)

# List tables
tables = sharing_client.list_all_tables()
for table in tables:
    print(f"{table.share}.{table.schema}.{table.name}")

# Read as Pandas
import pandas as pd
df = delta_sharing.load_as_pandas(
    f"{profile}#bolivia_data_share.bolivia.j_atoscompra_new"
)

# Read as Spark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df = delta_sharing.load_as_spark(
    f"{profile}#bolivia_data_share.bolivia.j_atoscompra_new"
)
```

## Monitoring

### Audit Logs

```sql
-- View share access logs
SELECT
  event_time,
  user_identity.email as user,
  request_params.share_name,
  request_params.schema_name,
  request_params.name as table_name,
  response.status_code
FROM system.access.audit
WHERE action_name = 'deltaSharingQueryTable'
  AND request_params.share_name = 'bolivia_data_share'
ORDER BY event_time DESC
LIMIT 100;

-- Count queries per recipient
SELECT
  user_identity.email as recipient,
  request_params.name as table,
  COUNT(*) as query_count
FROM system.access.audit
WHERE action_name = 'deltaSharingQueryTable'
  AND request_params.share_name = 'bolivia_data_share'
  AND event_date >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY recipient, table
ORDER BY query_count DESC;
```

### Check Share Status

```python
from databricks.sdk import WorkspaceClient

client = WorkspaceClient()

# Get share info
share = client.shares.get("bolivia_data_share")
print(f"Share: {share.name}")
print(f"Tables: {len(share.objects)}")
print(f"Created: {share.created_at}")

# Get recipients
permissions = client.shares.list_permissions("bolivia_data_share")
if permissions.privilege_assignments:
    print(f"Recipients: {len(permissions.privilege_assignments)}")
    for perm in permissions.privilege_assignments:
        print(f"  - {perm.principal}")
```

## Troubleshooting

### Check Permissions

```sql
-- Show your permissions
SHOW GRANTS ON METASTORE;

-- Check if you can create shares
SELECT * FROM system.information_schema.effective_grants
WHERE grantor = current_user()
  AND privilege_type = 'CREATE_SHARE';
```

### Grant Share Permissions (Admin)

```sql
-- Grant CREATE SHARE to user
GRANT CREATE SHARE ON METASTORE TO `user@company.com`;

-- Grant USE SHARE to recipient
GRANT USE SHARE ON SHARE bolivia_data_share TO RECIPIENT `analytics_workspace`;
```

### Test Share Access

```bash
# As provider, verify share exists
databricks shares get --share bolivia_data_share

# As recipient, list available shares
databricks shares list

# Test query from recipient side
databricks sql \
  --warehouse-id <warehouse_id> \
  --statement "SELECT COUNT(*) FROM bolivia_shared.bolivia.j_atoscompra_new"
```

## Common Workflows

### Setup New Share (Complete Flow)

```bash
# 1. Create share with tables
python scripts/enable_delta_sharing.py \
  --country bolivia \
  --share-name bolivia_data_share

# 2. Verify setup
python scripts/verify_delta_sharing.py \
  --share bolivia_data_share \
  --test-query

# 3. Create recipient
databricks recipients create \
  --name analytics_team

# 4. Grant access
databricks shares grant \
  --share bolivia_data_share \
  --recipient analytics_team

# 5. Get activation link
databricks recipients get --recipient analytics_team
# Send activation_url to recipient
```

### Update Existing Share

```bash
# Add new tables
python scripts/enable_delta_sharing.py \
  --country bolivia \
  --share-name bolivia_data_share \
  --include-tables new_table1 new_table2

# Remove table (SQL)
# ALTER SHARE bolivia_data_share REMOVE TABLE catalog.schema.old_table;
```

### Share Multiple Countries

```bash
# Create shares for each country
for country in bolivia chile colombia ecuador; do
  python scripts/enable_delta_sharing.py \
    --country $country \
    --share-name ${country}_data_share
done

# Grant recipient access to all
for share in bolivia_data_share chile_data_share colombia_data_share ecuador_data_share; do
  databricks shares grant --share $share --recipient regional_analytics
done
```

## Environment Variables

```bash
# Required for scripts
export DATABRICKS_HOST="https://adb-xxxx.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi..."
export DATABRICKS_WAREHOUSE_ID="08fa9256f365f47a"

# Optional
export DATABRICKS_CATALOG="000-sql-databricks-bridge"
```

## References

- Full guide: [docs/DELTA_SHARING_GUIDE.md](./DELTA_SHARING_GUIDE.md)
- Databricks docs: https://docs.databricks.com/en/delta-sharing/
- Delta Sharing spec: https://github.com/delta-io/delta-sharing
