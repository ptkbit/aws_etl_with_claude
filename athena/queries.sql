-- =============================================================================
-- ClinicalTrialAEPipeline — Athena Reference Queries
-- Database : ctae_database
-- Workgroup: ctae-workgroup
--
-- Tables (created by Glue Crawlers after first ETL run):
--   processed_adverse_events  — valid records, partitioned by study_id
--   quarantine_adverse_events — invalid records with validation_error column
--
-- Run all queries in the ctae-workgroup so results land in the Athena
-- results bucket and the 1 GB scan cap is enforced.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. PREVIEW — Most recent valid records
-- -----------------------------------------------------------------------------
SELECT
    subject_id,
    study_id,
    adverse_event,
    severity,
    onset_date,
    report_date,
    ingestion_timestamp
FROM processed_adverse_events
ORDER BY ingestion_timestamp DESC
LIMIT 100;


-- -----------------------------------------------------------------------------
-- 2. AGGREGATION — Event counts by study and severity
-- -----------------------------------------------------------------------------
SELECT
    study_id,
    severity,
    COUNT(*) AS event_count
FROM processed_adverse_events
GROUP BY study_id, severity
ORDER BY study_id, event_count DESC;


-- -----------------------------------------------------------------------------
-- 3. HIGH SEVERITY — SEVERE and LIFE_THREATENING events with reporting lag
-- -----------------------------------------------------------------------------
SELECT
    subject_id,
    study_id,
    adverse_event,
    severity,
    onset_date,
    report_date,
    DATE_DIFF('day', DATE(onset_date), DATE(report_date)) AS days_to_report
FROM processed_adverse_events
WHERE severity IN ('SEVERE', 'LIFE_THREATENING')
ORDER BY onset_date DESC;


-- -----------------------------------------------------------------------------
-- 4. PARTITION CHECK — Record counts per study partition
-- Useful to verify the partitionBy(study_id) layout is correct.
-- -----------------------------------------------------------------------------
SELECT
    study_id,
    COUNT(*)          AS total_records,
    MIN(onset_date)   AS earliest_onset,
    MAX(onset_date)   AS latest_onset,
    MIN(ingestion_timestamp) AS first_ingested,
    MAX(ingestion_timestamp) AS last_ingested
FROM processed_adverse_events
GROUP BY study_id
ORDER BY study_id;


-- -----------------------------------------------------------------------------
-- 5. QUARANTINE AUDIT — Inspect all invalid records
-- -----------------------------------------------------------------------------
SELECT
    subject_id,
    study_id,
    adverse_event,
    severity,
    onset_date,
    report_date,
    validation_error,
    ingestion_timestamp
FROM quarantine_adverse_events
ORDER BY ingestion_timestamp DESC
LIMIT 200;


-- -----------------------------------------------------------------------------
-- 6. QUARANTINE SUMMARY — Error frequency by validation rule
-- -----------------------------------------------------------------------------
SELECT
    validation_error,
    COUNT(*) AS record_count
FROM quarantine_adverse_events
GROUP BY validation_error
ORDER BY record_count DESC;


-- -----------------------------------------------------------------------------
-- 7. DATA QUALITY RATIO — Valid vs. invalid records per ingestion run
-- Join on ingestion_timestamp truncated to minute for approximate grouping.
-- -----------------------------------------------------------------------------
WITH processed AS (
    SELECT
        DATE_TRUNC('minute', ingestion_timestamp) AS run_window,
        COUNT(*) AS valid_count
    FROM processed_adverse_events
    GROUP BY 1
),
quarantined AS (
    SELECT
        DATE_TRUNC('minute', ingestion_timestamp) AS run_window,
        COUNT(*) AS invalid_count
    FROM quarantine_adverse_events
    GROUP BY 1
)
SELECT
    COALESCE(p.run_window, q.run_window)        AS run_window,
    COALESCE(p.valid_count, 0)                  AS valid_records,
    COALESCE(q.invalid_count, 0)                AS invalid_records,
    COALESCE(p.valid_count, 0) +
        COALESCE(q.invalid_count, 0)            AS total_records,
    ROUND(
        100.0 * COALESCE(p.valid_count, 0) /
        NULLIF(COALESCE(p.valid_count, 0) + COALESCE(q.invalid_count, 0), 0),
        2
    )                                           AS valid_pct
FROM processed p
FULL OUTER JOIN quarantined q ON p.run_window = q.run_window
ORDER BY run_window DESC;
