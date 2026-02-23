"""
adverse_events_etl.py -- AWS Glue PySpark ETL Job
ClinicalTrialAEPipeline

Reads a raw adverse event CSV from S3, validates each record against
pharma domain rules, and routes output to:
  - processed zone : valid records (Parquet, partitioned by study_id)
  - quarantine zone: invalid records (Parquet + validation_error column)

Job parameters (passed by Lambda trigger):
  --s3_input_bucket   : source S3 bucket name
  --s3_input_key      : source S3 object key (path to CSV file)
  --processed_path    : s3:// URI for the processed zone prefix
  --quarantine_path   : s3:// URI for the quarantine zone prefix

Glue version: 4.0 (Python 3.10, Spark 3.3)
"""

import sys
import re
import logging
from datetime import datetime

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Pharma domain constants
# ---------------------------------------------------------------------------
VALID_SEVERITIES = {"MILD", "MODERATE", "SEVERE", "LIFE_THREATENING"}
SUBJECT_ID_RE = re.compile(r"^SUB-\d{5}$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
MANDATORY_FIELDS = [
    "subject_id",
    "study_id",
    "adverse_event",
    "severity",
    "onset_date",
    "report_date",
]


# ---------------------------------------------------------------------------
# Validation logic
# ---------------------------------------------------------------------------
def _is_valid_date(value: str) -> bool:
    """Return True if value is a valid YYYY-MM-DD calendar date."""
    if not value or not DATE_RE.match(value):
        return False
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def validate_record(
    subject_id, study_id, adverse_event, severity, onset_date, report_date
) -> str | None:
    """
    Validate a single record against pharma domain rules.

    Returns a comma-separated string of error messages if the record is
    invalid, or None if the record is valid.
    """
    errors = []

    # 1. Mandatory field presence
    for field_name, value in [
        ("subject_id", subject_id),
        ("study_id", study_id),
        ("adverse_event", adverse_event),
        ("severity", severity),
        ("onset_date", onset_date),
        ("report_date", report_date),
    ]:
        if value is None or str(value).strip() == "":
            errors.append(f"Missing required field: {field_name}")

    # 2. subject_id format (only when present)
    if subject_id and not SUBJECT_ID_RE.match(str(subject_id)):
        errors.append(f"Invalid subject_id format: '{subject_id}'")

    # 3. Severity (case-sensitive exact match)
    if severity and severity not in VALID_SEVERITIES:
        errors.append(f"Invalid severity value: '{severity}'")

    # 4. Date format validation
    if onset_date and not _is_valid_date(str(onset_date)):
        errors.append(f"Invalid onset_date format: '{onset_date}'")

    if report_date and not _is_valid_date(str(report_date)):
        errors.append(f"Invalid report_date format: '{report_date}'")

    # 5. Logical date ordering (only when both dates are individually valid)
    if (
        onset_date
        and report_date
        and _is_valid_date(str(onset_date))
        and _is_valid_date(str(report_date))
        and str(report_date) < str(onset_date)
    ):
        errors.append(
            f"report_date '{report_date}' is before onset_date '{onset_date}'"
        )

    return ", ".join(errors) if errors else None


# Register as a PySpark UDF
validate_udf = F.udf(validate_record, StringType())


# ---------------------------------------------------------------------------
# Main ETL logic
# ---------------------------------------------------------------------------
def main():
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "s3_input_bucket",
            "s3_input_key",
            "processed_path",
            "quarantine_path",
        ],
    )

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    input_path = f"s3://{args['s3_input_bucket']}/{args['s3_input_key']}"
    processed_path = args["processed_path"]
    quarantine_path = args["quarantine_path"]

    logger.info("Job started")
    logger.info(f"Input  : {input_path}")
    logger.info(f"Processed output  : {processed_path}")
    logger.info(f"Quarantine output : {quarantine_path}")

    # -----------------------------------------------------------------------
    # Read raw CSV
    # -----------------------------------------------------------------------
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")  # keep all columns as strings for validation
        .option("mode", "PERMISSIVE")
        .csv(input_path)
    )

    total_count = df.count()
    logger.info(f"Records read: {total_count}")

    if total_count == 0:
        logger.warning("Input file is empty -- nothing to process.")
        job.commit()
        return

    # -----------------------------------------------------------------------
    # Apply validation and stamp ingestion timestamp
    # -----------------------------------------------------------------------
    df = df.withColumn(
        "validation_error",
        validate_udf(
            F.col("subject_id"),
            F.col("study_id"),
            F.col("adverse_event"),
            F.col("severity"),
            F.col("onset_date"),
            F.col("report_date"),
        ),
    ).withColumn("ingestion_timestamp", F.current_timestamp())

    valid_df = df.filter(F.col("validation_error").isNull()).drop("validation_error")
    invalid_df = df.filter(F.col("validation_error").isNotNull())

    valid_count = valid_df.count()
    invalid_count = invalid_df.count()

    logger.info(f"Valid records   : {valid_count}")
    logger.info(f"Invalid records : {invalid_count}")

    # -----------------------------------------------------------------------
    # Write valid records -> processed zone (Parquet, partitioned by study_id)
    # PII note: only subject_id is carried forward; no patient names or DOBs.
    # -----------------------------------------------------------------------
    if valid_count > 0:
        valid_df.write.mode("append").partitionBy("study_id").parquet(processed_path)
        logger.info(f"Written {valid_count} valid records to {processed_path}")

    # -----------------------------------------------------------------------
    # Write invalid records -> quarantine zone (Parquet + validation_error)
    # Records are never silently dropped; every failure is captured.
    # -----------------------------------------------------------------------
    if invalid_count > 0:
        invalid_df.write.mode("append").parquet(quarantine_path)
        logger.info(f"Written {invalid_count} invalid records to {quarantine_path}")

    job.commit()
    logger.info("Job completed successfully")


if __name__ == "__main__":
    main()
