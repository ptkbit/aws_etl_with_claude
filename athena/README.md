# Athena SQL Queries

Amazon Athena DDL statements and analytical SQL queries for the processed adverse event data.

## Planned Files

| File | Purpose |
|------|---------|
| `01-create-tables.sql` | DDL to create external tables over the Glue Data Catalog |
| `02-analysis-queries.sql` | Common analytical queries (severity breakdown, event counts, etc.) |

## Table Design

- **Metastore:** AWS Glue Data Catalog (default in us-east-1)
- **Format:** Parquet with Snappy compression
- **Partitioning:** By `study_id` for efficient per-study queries
- **Location:** `s3://<bucket>/processed/adverse_events/`

## Query Results

Athena query results are stored in: `s3://<bucket>/athena-results/`

## Example Queries

```sql
-- Count adverse events by severity per study
SELECT study_id, severity, COUNT(*) AS event_count
FROM adverse_events_processed
GROUP BY study_id, severity
ORDER BY study_id, event_count DESC;

-- Find LIFE_THREATENING events
SELECT * FROM adverse_events_processed
WHERE severity = 'LIFE_THREATENING'
ORDER BY onset_date;
```
