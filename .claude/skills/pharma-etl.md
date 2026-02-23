# Skill: pharma-etl — Clinical Trial Adverse Event Pipeline

## Purpose

This skill guides Claude in writing ETL code, validation logic, PySpark scripts, Lambda functions,
and CloudFormation templates for the AWS clinical trial adverse event data pipeline.
Apply these rules and patterns whenever writing or reviewing code in this project.

---

## Validation Rules (Always Enforce)

### 1. Mandatory Fields
Every record must have all of the following fields present, non-null, and non-empty:
- `subject_id`
- `study_id`
- `adverse_event`
- `severity`
- `onset_date`
- `report_date`

### 2. subject_id Format
- Regex pattern: `^SUB-\d{5}$`
- Valid: `SUB-00001`, `SUB-12345`
- Invalid: `SUB-1`, `SUB-ABC`, `00001`, `SUB-1234`, `sub-00001`

### 3. Severity Values (Exact Match, Case-Sensitive)
Allowed values only:
- `MILD`
- `MODERATE`
- `SEVERE`
- `LIFE_THREATENING`

### 4. Date Fields (onset_date, report_date)
- Format: `YYYY-MM-DD` (ISO 8601)
- Must be valid calendar dates (e.g., 2024-02-30 is invalid)
- `report_date` must be >= `onset_date`

### 5. PII Rules
- No patient names, DOBs, addresses, phone numbers, or email addresses
- `subject_id` (pseudonymized) is the only subject identifier allowed in the processed zone

---

## ETL Architecture Patterns

### Invalid Record Handling
- **Never** silently drop invalid records
- Write invalid records to quarantine: `s3://<bucket>/quarantine/adverse_events/`
- Add a `validation_error` string column describing every failure reason (comma-separated if multiple)
- Add a `ingestion_timestamp` column (UTC) to all output records
- Log counts of valid vs. invalid records to CloudWatch Logs

### PySpark Validation Template

```python
import re
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

VALID_SEVERITIES = {"MILD", "MODERATE", "SEVERE", "LIFE_THREATENING"}
SUBJECT_ID_RE = re.compile(r"^SUB-\d{5}$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
MANDATORY_FIELDS = ["subject_id", "study_id", "adverse_event", "severity", "onset_date", "report_date"]


def is_valid_date(value: str) -> bool:
    if not value or not DATE_RE.match(value):
        return False
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def validate_record(subject_id, study_id, adverse_event, severity, onset_date, report_date) -> str | None:
    """Return comma-separated error string, or None if record is valid."""
    errors = []

    # Mandatory field presence
    for name, val in [
        ("subject_id", subject_id),
        ("study_id", study_id),
        ("adverse_event", adverse_event),
        ("severity", severity),
        ("onset_date", onset_date),
        ("report_date", report_date),
    ]:
        if val is None or str(val).strip() == "":
            errors.append(f"Missing required field: {name}")

    # Format validations (only if field is present)
    if subject_id and not SUBJECT_ID_RE.match(str(subject_id)):
        errors.append(f"Invalid subject_id format: '{subject_id}'")

    if severity and severity not in VALID_SEVERITIES:
        errors.append(f"Invalid severity: '{severity}'")

    if onset_date and not is_valid_date(str(onset_date)):
        errors.append(f"Invalid onset_date format: '{onset_date}'")

    if report_date and not is_valid_date(str(report_date)):
        errors.append(f"Invalid report_date format: '{report_date}'")

    # Logical date check
    if onset_date and report_date and is_valid_date(str(onset_date)) and is_valid_date(str(report_date)):
        if report_date < onset_date:
            errors.append("report_date is before onset_date")

    return ", ".join(errors) if errors else None


# Register as PySpark UDF
validate_udf = F.udf(validate_record, StringType())
```

### PySpark Split Pattern (Valid vs. Quarantine)

```python
# Apply validation
df = df.withColumn(
    "validation_error",
    validate_udf(
        F.col("subject_id"), F.col("study_id"), F.col("adverse_event"),
        F.col("severity"), F.col("onset_date"), F.col("report_date")
    )
).withColumn("ingestion_timestamp", F.current_timestamp())

valid_df = df.filter(F.col("validation_error").isNull()).drop("validation_error")
invalid_df = df.filter(F.col("validation_error").isNotNull())

# Write valid records (partitioned Parquet)
valid_df.write.mode("append").partitionBy("study_id").parquet(processed_path)

# Write invalid records to quarantine
invalid_df.write.mode("append").parquet(quarantine_path)
```

---

## CloudFormation Conventions

- **Format:** YAML (not JSON)
- **Tag all resources** with:
  ```yaml
  Tags:
    - Key: Project
      Value: ClinicalTrialAEPipeline
    - Key: Environment
      Value: dev
  ```
- Use `!Sub`, `!Ref`, `!GetAtt`, `!ImportValue` for dynamic values — never hardcode ARNs
- Export key ARNs and names in `Outputs` section for cross-stack references
- Use `AWS::CloudFormation::Stack` for nested stacks when templates exceed ~400 lines
- S3 bucket names should use `!Sub "${AWS::AccountId}-${AWS::Region}-<purpose>"` to ensure global uniqueness

---

## Lambda Conventions

```python
# Runtime: python3.13
# Handler: handler.lambda_handler
# Trigger: S3 ObjectCreated event (on raw bucket)

import boto3
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue = boto3.client("glue", region_name="us-east-1")
JOB_NAME = os.environ["GLUE_JOB_NAME"]


def lambda_handler(event, context):
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        logger.info(f"Triggering Glue job for s3://{bucket}/{key}")

        response = glue.start_job_run(
            JobName=JOB_NAME,
            Arguments={
                "--s3_input_bucket": bucket,
                "--s3_input_key": key,
            },
        )
        logger.info(f"Glue job run ID: {response['JobRunId']}")
```

---

## Athena Conventions

- Always use Glue Data Catalog as metastore (default in us-east-1)
- Use `CREATE EXTERNAL TABLE IF NOT EXISTS` for Parquet tables
- Partition by `study_id` for efficient querying
- Store Athena query results in a dedicated S3 prefix: `s3://<bucket>/athena-results/`
- Use `MSCK REPAIR TABLE` or Glue Crawler to update partition metadata

### Example DDL Pattern

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS adverse_events_processed (
    subject_id        STRING,
    adverse_event     STRING,
    severity          STRING,
    onset_date        DATE,
    report_date       DATE,
    ingestion_timestamp TIMESTAMP
)
PARTITIONED BY (study_id STRING)
STORED AS PARQUET
LOCATION 's3://<bucket>/processed/adverse_events/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');
```

---

## Common Mistakes to Avoid

| Mistake | Correct Approach |
|---------|-----------------|
| Silently dropping invalid rows | Always write to quarantine with `validation_error` |
| Hardcoding bucket names or ARNs | Use CloudFormation parameters and `!Sub` |
| Case-insensitive severity check | Severity is **case-sensitive** — `mild` is invalid |
| Using `YYYY-dd-MM` date format | Always use `YYYY-MM-DD` (ISO 8601) |
| Writing PII to processed zone | Only `subject_id` allowed; strip everything else |
| Using `terraform` or CDK | This project uses **CloudFormation only** |
| Using Python < 3.13 | Lambda and Glue jobs must target **Python 3.13** |
