"""
handler.py -- Lambda pipeline trigger for ClinicalTrialAEPipeline
Runtime: Python 3.13

Handles two EventBridge event types:
  1. aws.s3 / Object Created
       -> Starts the Glue ETL job with the S3 bucket and key as arguments.
  2. aws.glue / Glue Job State Change (state: SUCCEEDED)
       -> Starts the Glue Crawler to refresh the Data Catalog partition metadata.

Environment variables (set by CloudFormation):
  GLUE_JOB_NAME : name of the Glue ETL job to start
  CRAWLER_NAME  : name of the Glue Crawler to start after ETL succeeds
"""

import logging
import os

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue = boto3.client("glue", region_name="us-east-1")

GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]
CRAWLER_NAME = os.environ["CRAWLER_NAME"]


def lambda_handler(event, context):
    source = event.get("source", "")
    logger.info(f"Event received -- source: {source}")

    if source == "aws.s3":
        _handle_s3_event(event)
    elif source == "aws.glue":
        _handle_glue_event(event)
    else:
        logger.warning(f"Unrecognized event source: {source!r} -- ignoring")


def _handle_s3_event(event: dict) -> None:
    """Start the Glue ETL job for the uploaded S3 object."""
    detail = event["detail"]
    bucket = detail["bucket"]["name"]
    key = detail["object"]["key"]

    logger.info(f"New file detected: s3://{bucket}/{key}")

    response = glue.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            "--s3_input_bucket": bucket,
            "--s3_input_key": key,
        },
    )
    run_id = response["JobRunId"]
    logger.info(f"Glue job '{GLUE_JOB_NAME}' started -- run ID: {run_id}")


def _handle_glue_event(event: dict) -> None:
    """Start the Glue Crawler when the ETL job succeeds."""
    detail = event["detail"]
    job_name = detail.get("jobName", "")
    state = detail.get("state", "")

    logger.info(f"Glue job '{job_name}' reached state: {state}")

    if state == "SUCCEEDED":
        try:
            glue.start_crawler(Name=CRAWLER_NAME)
            logger.info(f"Crawler '{CRAWLER_NAME}' started successfully")
        except glue.exceptions.CrawlerRunningException:
            # Crawler may already be running from a concurrent job run -- safe to skip
            logger.warning(
                f"Crawler '{CRAWLER_NAME}' is already running -- skipping"
            )
