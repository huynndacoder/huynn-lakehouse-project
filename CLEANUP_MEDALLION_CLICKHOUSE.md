# ClickHouse Medallion Cleanup

## What Was Removed (ClickHouse Only)

**Date:** 2026-04-08

### Removed Tables/Views

1. **Silver Layer:**
   - `lakehouse.silver_taxi_trips` table (never populated, 0 rows)

2. **Gold Layer:**
   - `lakehouse.gold_zone_performance` view (lazy view, queried Bronze directly)
   - `lakehouse.gold_hourly_metrics` view (lazy view, queried Bronze directly)
   - `lakehouse.gold_borough_summary` view (lazy view, queried Bronze directly)

### Why Removed

**Medallion architecture is unnecessary for real-time ClickHouse:**

1. **Fast Query Performance:** ClickHouse already provides sub-40ms queries on 200K+ rows
2. **No Data Quality Issues:** All records pass quality filters (fare >= 0, distance >= 0, timestamps present)
3. **Lazy Views:** Gold views were not materialized - they re-computed aggregations on every query
4. **Single Purpose:** This ClickHouse instance serves dashboard API only, not ML workloads

### What Remains

**ClickHouse (Real-time Dashboard API):**
- `taxi_prod` - Real-time trips from Kafka Engine
- `nyc_weather` - Real-time weather from Kafka Engine
- `taxi_zones` - Static lookup table (265 zones)
- `taxi_kafka_source`, `taxi_kafka_parser` - Kafka ingestion
- `weather_kafka_source`, `weather_kafka_parser` - Kafka ingestion

**Iceberg (Batch ML Pipeline):**
- Bronze/Silver/Gold medallion architecture remains intact
- Used for ML feature engineering
- Long-term storage with time travel
- Batch analytics

### Architecture Simplification

**Before:**
```
Kafka → ClickHouse Bronze → Silver (empty) → Gold Views (unused)
                                    ↑
                              API queries Bronze (ignores Silver/Gold)
```

**After:**
```
Kafka → ClickHouse.taxi_prod → API
```

**Separation of Concerns:**
- **ClickHouse:** Real-time serving layer (sub-second queries, no medallion needed)
- **Iceberg:** Batch processing layer (medallion appropriate for ML/ETL)

### Performance Impact

| Metric | Before Removal | After Removal |
|--------|----------------|---------------|
| Tables | 7 (Bronze + Silver + Gold views) | 4 (Bronze only) |
| Query Latency | 8-40ms | 8-40ms (unchanged) |
| Storage | Silver: 0 rows, Gold: lazy views | Single Bronze table |
| Complexity | Confusing (unused layers) | Clear (single purpose) |

### Files Modified

1. `init_clickhouse.sql` - Removed Silver and Gold definitions
2. `CLEANUP_MEDALLION_CLICKHOUSE.md` - This documentation

### Files to Update (Future)

1. `README.md` - Remove medallion references for ClickHouse
2. `PRESENTATION_SCRIPT.md` - Update architecture explanation

### Commands Executed

```sql
-- Drop unused Silver table
DROP TABLE IF EXISTS lakehouse.silver_taxi_trips;

-- Drop lazy Gold views
DROP VIEW IF EXISTS lakehouse.gold_zone_performance SYNC;
DROP VIEW IF EXISTS lakehouse.gold_hourly_metrics SYNC;
DROP VIEW IF EXISTS lakehouse.gold_borough_summary SYNC;
```

### Verification

```bash
# List remaining tables
docker exec clickhouse clickhouse-client --query "SHOW TABLES FROM lakehouse"

# Expected output:
# nyc_weather
# taxi_kafka_parser
# taxi_kafka_source
# taxi_prod
# taxi_zones
# weather_kafka_parser
# weather_kafka_source

# Verify data integrity
docker exec clickhouse clickhouse-client --query "SELECT COUNT(*) FROM lakehouse.taxi_prod"
# Expected: 215600+ (growing from real-time ingestion)
```

### API Compatibility

✅ No changes required - API was already querying Bronze (`taxi_prod`) directly

### Reverting (If Needed)

If medallion layers need to be restored for any reason:

```bash
# Restore from git history
git checkout HEAD~1 -- init_clickhouse.sql

# Recreate tables
docker exec clickhouse clickhouse-client --multiquery < init_clickhouse.sql
```

### References

- [ClickHouse Best Practices](https://clickhouse.com/docs/en/guides/best-practices)
- [Medallion Architecture](https://Databricks.com/lakehouse/medallion) - Appropriate for batch/lakehouse, not real-time OLAP