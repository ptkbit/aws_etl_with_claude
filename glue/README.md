# Glue PySpark ETL Scripts

AWS Glue PySpark scripts for validating and transforming adverse event data.

## Planned Files

| File | Purpose |
|------|---------|
| `adverse_events_etl.py` | Main ETL job: read CSV → validate → write Parquet |
| `validation.py` | Reusable validation functions and UDFs |

## ETL Job Flow

1. Read CSV from `s3://<bucket>/raw/adverse_events/<file>.csv`
2. Apply pharma domain validation rules to every record
3. Split into valid and invalid DataFrames
4. Write valid records as Parquet to `s3://<bucket>/processed/adverse_events/` (partitioned by `study_id`)
5. Write invalid records as Parquet to `s3://<bucket>/quarantine/adverse_events/` (with `validation_error` column)
6. Log record counts (total, valid, invalid) to CloudWatch

## Validation Rules Applied

- `subject_id` matches `^SUB-\d{5}$`
- `severity` is one of: `MILD`, `MODERATE`, `SEVERE`, `LIFE_THREATENING`
- `onset_date` and `report_date` are valid `YYYY-MM-DD` dates
- All mandatory fields are non-null and non-empty
- `report_date` >= `onset_date`

## Job Parameters (passed from Lambda)

| Parameter | Description |
|-----------|-------------|
| `--s3_input_bucket` | Source S3 bucket name |
| `--s3_input_key` | S3 object key of the uploaded CSV |
| `--processed_path` | S3 URI for valid output (set via CloudFormation) |
| `--quarantine_path` | S3 URI for invalid output (set via CloudFormation) |
