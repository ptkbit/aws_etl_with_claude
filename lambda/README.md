# Lambda Function

AWS Lambda function that triggers the Glue ETL job when a new file is uploaded
to the S3 raw zone.

## Planned Files

| File | Purpose |
|------|---------|
| `handler.py` | Lambda handler — parses S3 event and starts Glue job |
| `requirements.txt` | Python dependencies (boto3 is provided by Lambda runtime) |

## Trigger

- **Event source:** S3 `ObjectCreated` events on the raw bucket prefix `raw/adverse_events/`
- **Runtime:** Python 3.13

## Behavior

1. Receives S3 event with bucket name and object key
2. Starts the Glue ETL job, passing `--s3_input_bucket` and `--s3_input_key` as job arguments
3. Logs the Glue job run ID to CloudWatch

## Environment Variables

| Variable | Description |
|----------|-------------|
| `GLUE_JOB_NAME` | Name of the Glue job to trigger (set via CloudFormation) |
