# Delta Sharing Guide for sql-databricks-bridge

This guide explains how to enable Delta Sharing for tables in the `000-sql-databricks-bridge` catalog.

## What is Delta Sharing?

**Delta Sharing** is an open protocol for secure data sharing that allows you to share live data from your Delta Lake tables with:
- Other Databricks workspaces
- Other cloud platforms (AWS, Azure, GCP)
- External organizations
- Non-Databricks users (via open-source clients)

**Key benefits:**
- **No data duplication**: Recipients read directly from your tables
- **Real-time access**: Recipients always see the latest data
- **Secure**: Fine-grained access control with audit logs
- **Open standard**: Works across clouds and platforms

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  000-sql-databricks-bridge.bolivia (Source Catalog)             │
│                                                                  │
│  Tables:                                                         │
│    - j_atoscompra_new                                           │
│    - hato_cabecalho                                             │
│    - rg_panelis                                                 │
│    - vw_artigoz                                                 │
│    - ... (30+ tables)                                           │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         │ Create Delta Share
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  Delta Share: "bolivia_data_share"                              │
│                                                                  │
│  Shareable with:                                                │
│    - Other Databricks workspaces                                │
│    - External organizations                                     │
│    - Data consumers (read-only)                                 │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         │ Grant access to Recipients
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  Recipient: "analytics_team"                                    │
│  Recipient: "external_partner"                                  │
│  Recipient: "reporting_workspace"                               │
│                                                                  │
│  Each recipient gets:                                           │
│    - Secure share credential                                    │
│    - Read-only access                                           │
│    - Access to live data (no copying)                           │
└─────────────────────────────────────────────────────────────────┘
```

## Prerequisites

1. **Databricks Unity Catalog** enabled
2. **Metastore admin** or **CREATE SHARE** privilege
3. **Tables must be in Unity Catalog** (✓ Already the case for `000-sql-databricks-bridge`)
4. **Python environment** with `databricks-sdk` installed

```bash
pip install databricks-sdk python-dotenv
```

## Quick Start

### 1. Enable Delta Sharing for All Bolivia Tables

```bash
cd /home/jaalduna/Documents/projects/sql-databricks-bridge

# Dry run first (see what would be done)
python scripts/enable_delta_sharing.py \
  --country bolivia \
  --share-name bolivia_data_share \
  --dry-run

# Actually create the share
python scripts/enable_delta_sharing.py \
  --country bolivia \
  --share-name bolivia_data_share
```

**Output:**
```
======================================================================
Enabling Delta Sharing for: 000-sql-databricks-bridge.bolivia
Share name: bolivia_data_share
Dry run: False
======================================================================

Listing tables in 000-sql-databricks-bridge.bolivia...
  Found 45 tables

Tables to share (45):
  - a_accesocanal
  - a_canal
  - hato_cabecalho
  - j_atoscompra_new
  - rg_panelis
  - vw_artigoz
  ... (and 39 more)

Creating share: bolivia_data_share
  ✓ Created share: bolivia_data_share

Adding tables to share...
    ✓ Added: a_accesocanal
    ✓ Added: a_canal
    ✓ Added: hato_cabecalho
    ... (adding all tables)

======================================================================
SUMMARY
======================================================================
Total tables: 45
Tables to share: 45
Successfully added: 45
Failed: 0

✓ Delta Share 'bolivia_data_share' is ready!

Next steps:
1. Grant access to recipients:
   databricks shares grant --share bolivia_data_share --recipient <recipient_name>
2. Generate share URL:
   databricks shares get --share bolivia_data_share
3. Or use Databricks UI: Data > Delta Sharing > bolivia_data_share
```

### 2. Share Only Specific Tables

If you only want to share certain tables:

```bash
python scripts/enable_delta_sharing.py \
  --country bolivia \
  --share-name bolivia_core_tables \
  --include-tables j_atoscompra_new hato_cabecalho rg_panelis vw_artigoz
```

### 3. Exclude Sensitive Tables

To share all tables except some:

```bash
python scripts/enable_delta_sharing.py \
  --country bolivia \
  --share-name bolivia_public_data \
  --exclude-tables sensitive_table1 sensitive_table2
```

### 4. List Existing Shares

```bash
python scripts/enable_delta_sharing.py --list-shares
```

**Output:**
```
======================================================================
EXISTING DELTA SHARES
======================================================================

📊 bolivia_data_share
   Created: 2026-02-05T10:30:00Z
   Comment: Delta Share for 000-sql-databricks-bridge tables
   Tables (45):
     - 000-sql-databricks-bridge.bolivia.j_atoscompra_new
     - 000-sql-databricks-bridge.bolivia.hato_cabecalho
     - 000-sql-databricks-bridge.bolivia.rg_panelis
     - 000-sql-databricks-bridge.bolivia.vw_artigoz
     ... and 41 more
```

## Managing Recipients

### Create a Recipient (Internal Workspace)

For sharing with another workspace in the same organization:

```bash
databricks recipients create \
  --name analytics_workspace \
  --comment "Analytics team workspace"
```

### Create a Recipient (External Organization)

For sharing with external partners:

```bash
databricks recipients create \
  --name external_partner \
  --comment "External analytics partner" \
  --authentication-type DATABRICKS
```

This generates a **sharing identifier** and **activation link** that you send to the external party.

### Grant Access to Recipient

```bash
databricks shares grant \
  --share bolivia_data_share \
  --recipient analytics_workspace
```

### Revoke Access

```bash
databricks shares revoke \
  --share bolivia_data_share \
  --recipient analytics_workspace
```

## Consuming Shared Data

### In Databricks (Recipient Side)

Once a recipient has been granted access:

1. **Accept the share** (first time only):
   - Go to Databricks UI → Data → Delta Sharing → Shared with me
   - Click the share and accept it

2. **Create a catalog** from the share:
   ```sql
   CREATE CATALOG bolivia_shared
   USING SHARE `<provider>`.bolivia_data_share;
   ```

3. **Query the data**:
   ```sql
   SELECT * FROM bolivia_shared.bolivia.j_atoscompra_new
   WHERE Periodo = 202401;
   ```

### Using Python (Recipient Side)

```python
import delta_sharing

# Get share profile from recipient activation
profile_path = "/path/to/share_config.json"

# Create SharingClient
client = delta_sharing.SharingClient(profile_path)

# List all tables
tables = client.list_all_tables()

# Read table as pandas DataFrame
df = delta_sharing.load_as_pandas(
    f"{profile_path}#bolivia_data_share.bolivia.j_atoscompra_new"
)

# Or use Spark
spark_df = delta_sharing.load_as_spark(
    f"{profile_path}#bolivia_data_share.bolivia.j_atoscompra_new"
)
```

## Security and Permissions

### Required Permissions (Share Creator)

To create shares, you need:
- `CREATE SHARE` on the metastore, OR
- Metastore admin role

### Share Permissions Model

1. **Share creator** controls:
   - Which tables are in the share
   - Which recipients can access the share
   - Can revoke access at any time

2. **Recipients** get:
   - **Read-only access** to shared tables
   - No ability to modify data
   - No ability to see underlying storage

3. **Table owners** retain:
   - Full control over tables
   - Ability to update/delete data
   - Changes are immediately visible to recipients

### Best Practices

1. **Use descriptive share names**: `bolivia_data_share`, not `share1`
2. **Document what's shared**: Add comments to shares
3. **Regular audits**: Review recipients periodically
4. **Principle of least privilege**: Only share necessary tables
5. **Monitor access**: Use audit logs to track usage

## Monitoring and Auditing

### View Share Activity

```sql
-- See who accessed the share
SELECT * FROM system.access.audit
WHERE action_name = 'deltaSharingQueryTable'
  AND request_params.share_name = 'bolivia_data_share'
ORDER BY event_time DESC
LIMIT 100;
```

### Check Share Configuration

```bash
# Get share details
databricks shares get --share bolivia_data_share

# List all recipients
databricks recipients list

# Get recipient details
databricks recipients get --recipient analytics_workspace
```

### Python API

```python
from databricks.sdk import WorkspaceClient

client = WorkspaceClient()

# Get share details
share = client.shares.get("bolivia_data_share")
print(f"Share: {share.name}")
print(f"Created: {share.created_at}")
print(f"Tables: {len(share.objects)}")

# List recipients with access
recipients = client.recipients.list()
for recipient in recipients:
    print(f"Recipient: {recipient.name}")
```

## Common Use Cases

### Use Case 1: Cross-Workspace Analytics

**Scenario**: Share Bolivia data with a separate analytics workspace for reporting.

```bash
# 1. Create share
python scripts/enable_delta_sharing.py \
  --country bolivia \
  --share-name bolivia_analytics_share

# 2. Create recipient
databricks recipients create \
  --name analytics_workspace \
  --sharing-identifier <workspace_id>

# 3. Grant access
databricks shares grant \
  --share bolivia_analytics_share \
  --recipient analytics_workspace
```

**Analytics workspace** can now query:
```sql
SELECT
  Periodo,
  COUNT(DISTINCT IdDomicilio) as households,
  SUM(Quantidade) as total_packs
FROM bolivia_shared.bolivia.j_atoscompra_new
GROUP BY Periodo;
```

### Use Case 2: Partner Data Access

**Scenario**: Share specific tables with an external partner for market analysis.

```bash
# 1. Create limited share (only specific tables)
python scripts/enable_delta_sharing.py \
  --country bolivia \
  --share-name bolivia_partner_share \
  --include-tables j_atoscompra_new vw_artigoz a_canal

# 2. Create external recipient
databricks recipients create \
  --name market_research_partner \
  --comment "External market research firm" \
  --authentication-type DATABRICKS

# 3. Share the activation link with partner
databricks recipients get --recipient market_research_partner
# Copy the activation_url and send to partner
```

### Use Case 3: Multi-Country Sharing

**Scenario**: Share data from multiple countries in separate shares.

```bash
# Share Bolivia
python scripts/enable_delta_sharing.py \
  --country bolivia \
  --share-name bolivia_data_share

# Share Chile
python scripts/enable_delta_sharing.py \
  --country chile \
  --share-name chile_data_share

# Share Colombia
python scripts/enable_delta_sharing.py \
  --country colombia \
  --share-name colombia_data_share

# Grant recipient access to all countries
databricks shares grant --share bolivia_data_share --recipient regional_analytics
databricks shares grant --share chile_data_share --recipient regional_analytics
databricks shares grant --share colombia_data_share --recipient regional_analytics
```

## Troubleshooting

### Error: "Permission Denied"

**Cause**: You don't have `CREATE SHARE` privilege.

**Solution**: Ask your Databricks admin to grant:
```sql
GRANT CREATE SHARE ON METASTORE TO `<your_user>`;
```

### Error: "Table not found"

**Cause**: Table is not in Unity Catalog or doesn't exist.

**Solution**: Verify table exists:
```sql
SHOW TABLES IN `000-sql-databricks-bridge`.`bolivia`;
```

### Share Created but Recipients Can't See Tables

**Cause**: Tables weren't added to share, or recipient doesn't have access.

**Solution**:
1. Check share contents: `databricks shares get --share bolivia_data_share`
2. Verify recipient access: `databricks shares list-permissions --share bolivia_data_share`
3. Re-run the script to add missing tables

### Recipients See "Access Denied"

**Cause**: Recipient hasn't been granted access to the share.

**Solution**:
```bash
databricks shares grant --share bolivia_data_share --recipient <recipient_name>
```

## Advanced Configuration

### Sharing Specific Partitions

To share only certain data partitions:

```sql
-- Create a view with filtered data
CREATE VIEW 000-sql-databricks-bridge.bolivia.filtered_purchases AS
SELECT * FROM 000-sql-databricks-bridge.bolivia.j_atoscompra_new
WHERE Periodo >= 202401;

-- Share the view instead of the full table
ALTER SHARE bolivia_data_share
ADD TABLE 000-sql-databricks-bridge.bolivia.filtered_purchases;
```

### Change Data Feed (CDC)

Enable Change Data Feed for incremental sync:

```sql
-- Enable CDF on table
ALTER TABLE 000-sql-databricks-bridge.bolivia.j_atoscompra_new
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Recipients can now query changes
SELECT * FROM table_changes('bolivia_shared.bolivia.j_atoscompra_new', 0)
WHERE _change_type IN ('insert', 'update');
```

### Time Travel for Recipients

Recipients can query historical versions:

```sql
-- Query table as of specific timestamp
SELECT * FROM bolivia_shared.bolivia.j_atoscompra_new
TIMESTAMP AS OF '2026-01-01';

-- Query table as of version
SELECT * FROM bolivia_shared.bolivia.j_atoscompra_new
VERSION AS OF 42;
```

## Clean Up

### Remove Tables from Share

```bash
databricks shares update \
  --share bolivia_data_share \
  --remove 000-sql-databricks-bridge.bolivia.sensitive_table
```

### Delete Share

```bash
# Revoke all recipient access first
databricks shares revoke --share bolivia_data_share --recipient analytics_workspace

# Delete share
databricks shares delete --share bolivia_data_share
```

## References

- [Delta Sharing Documentation](https://docs.databricks.com/en/delta-sharing/index.html)
- [Delta Sharing Protocol](https://github.com/delta-io/delta-sharing)
- [Unity Catalog Sharing Guide](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)

## Next Steps

1. **Enable Delta Sharing** for Bolivia tables (this guide)
2. **Create recipients** for your data consumers
3. **Grant access** to specific shares
4. **Monitor usage** via audit logs
5. **Expand to other countries** (Chile, Colombia, Ecuador, etc.)
