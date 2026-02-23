# Differential Sync Implementation Plan

## Status: IN PROGRESS
Last updated: 2026-02-20

## Objective
Implement a 2-level differential sync feature that detects changes in SQL Server
data using stored CHECKSUM_AGG(CHECKSUM(*)) fingerprints and only downloads/replaces
the changed (level1_col, level2_col) slices in Databricks Delta tables.

## Key Design Decisions
- **Per-query filter**: `differential_sync` field in TriggerRequest maps query names
  to their diff-sync column config. Queries not listed use full OVERWRITE (backward-compatible).
- **Stored fingerprints**: SQL Server fingerprints stored in Databricks metadata table.
  Comparison is SQL_now vs SQL_at_last_sync (not cross-platform).
- **XOR checksum**: `CHECKSUM_AGG(CHECKSUM(*))` on SQL Server. Detects inserts, edits, deletes.
- **Block replacement**: Changed (level1, level2) slices are DELETE+INSERT in Databricks.
- **Delta time travel**: Each sync creates a new Delta version (automatic snapshot).
- **Streaming**: Chunks uploaded as separate parquet parts to staging Volume (flat memory).
- **Bolivia test**: Use 6 lookback periods for real data validation.

## Architecture

### New Files
1. `core/fingerprint.py` - SQL Server fingerprint computation
2. `core/diff_sync.py` - Differential sync orchestrator (compare, extract, merge)
3. `tests/test_diff_sync.py` - Synthetic + real data validation script

### Modified Files
1. `core/delta_writer.py` - Add `write_diff_slices()` for DELETE+INSERT pattern
2. `api/routes/trigger.py` - Add `DiffSyncConfig` to TriggerRequest, integrate in extraction loop
3. `core/config.py` - Add `fingerprint_table` setting

### Fingerprint Storage (Databricks Delta Table)
```sql
CREATE TABLE IF NOT EXISTS `{catalog}`.`metadata`.`sync_fingerprints` (
  country STRING,
  table_name STRING,        -- e.g. 'j_atoscompra_new'
  level STRING,             -- 'period' or 'product'
  level1_value STRING,      -- e.g. '202401' (periodo value)
  level2_value STRING,      -- e.g. '5' (idproduto value), NULL for level='period'
  row_count BIGINT,
  checksum_xor BIGINT,
  synced_at TIMESTAMP,
  job_id STRING
) USING DELTA
```

### API Model
```python
class DiffSyncQueryConfig(BaseModel):
    level1_column: str = "periodo"    # GROUP BY for Level 1
    level2_column: str = "idproduto"  # GROUP BY for Level 2

class TriggerRequest(BaseModel):
    ...existing fields...
    differential_sync: dict[str, DiffSyncQueryConfig] | None = None
    # Example: {"j_atoscompra_new": {"level1_column": "periodo", "level2_column": "idproduto"}}
    # Queries not in this dict use full OVERWRITE
```

### Data Flow (Differential Query)
```
1. Compute Level 1 fingerprints on SQL Server
   SELECT {level1_col}, COUNT(*), CHECKSUM_AGG(CHECKSUM(*))
   FROM {table} WHERE {time_filter} GROUP BY {level1_col}

2. Load stored Level 1 fingerprints from Databricks metadata table

3. Compare -> identify changed level1 values (count OR checksum differs)
   - First sync (no stored fingerprints) = all values are "new" = full download

4. For each changed level1 value:
   SELECT {level2_col}, COUNT(*), CHECKSUM_AGG(CHECKSUM(*))
   FROM {table} WHERE {level1_col} = ? GROUP BY {level2_col}

5. Compare Level 2 -> identify changed (level1, level2) pairs

6. Extract only changed rows from SQL Server:
   SELECT * FROM {table} WHERE {level1_col} = ? AND {level2_col} = ?
   Stream chunks to staging Volume

7. In Databricks:
   DELETE FROM target WHERE (level1_col, level2_col) IN (changed_pairs)
   INSERT INTO target SELECT * FROM read_files('staging/*.parquet')

8. Update stored fingerprints (overwrite all fingerprints for this table/country)
```

## Implementation Phases

### Phase 1: Core Modules
- [x] `core/fingerprint.py` - Fingerprint computation
- [x] `core/delta_writer.py` - Add write_diff_slices + streaming upload
- [x] `core/diff_sync.py` - Orchestration

### Phase 2: API Integration
- [x] `api/routes/trigger.py` - DiffSyncQueryConfig model + integration in process_query
- [x] `core/config.py` - fingerprint_table setting

### Phase 3: Synthetic Data Test
- [ ] Create test with synthetic SQL Server table
- [ ] Verify N (baseline), N+1 (changes detected), N+2 (restored)

### Phase 4: Bolivia Real Data Test (6 lookback periods)
- [ ] Run baseline sync (N) for j_atoscompra_new
- [ ] Introduce variation in specific period/product (N+1)
- [ ] Verify changes detected and synced
- [ ] Restore original values (N+2)
- [ ] Verify data matches original SQL table

## Testing Strategy
1. **Synthetic**: Create temp table in SQL Server, populate with known data,
   run 3 iterations with controlled mutations. Validate counts match.
2. **Bolivia real data**: Use j_atoscompra_new with lookback_months=6.
   Iteration N: baseline. N+1: UPDATE a row in specific (periodo, idproduto).
   N+2: Restore. Verify Databricks matches SQL Server after each iteration.
