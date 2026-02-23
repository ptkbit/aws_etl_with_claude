# AWS Clinical Trial Adverse Event ETL Pipeline

An end-to-end serverless data pipeline for ingesting, validating, and querying
clinical trial adverse event data on AWS — built entirely with free-tier services
and Infrastructure as Code (AWS CloudFormation).

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────┐
│                                                                          │
│   1. Ingest          2. Trigger         3. Validate & Transform          │
│                                                                          │
│  ┌──────────┐      ┌──────────┐       ┌─────────────────────────────┐   │
│  │  S3 Raw  │─────▶│  Lambda  │──────▶│      AWS Glue (PySpark)     │   │
│  │  Zone    │      │(trigger) │       │                             │   │
│  └──────────┘      └──────────┘       └────────────┬────────────────┘   │
│                                                    │                    │
│                               ┌────────────────────┴────────────────┐   │
│                               ▼                                     ▼   │
│                       ┌──────────────┐                   ┌───────────────┐│
│                       │  S3 Processed│                   │ S3 Quarantine ││
│                       │  Zone        │                   │ (invalid rows)││
│                       │  (Parquet)   │                   └───────────────┘│
│                       └──────┬───────┘                                   │
│                              │                                           │
│                   4. Catalog │              5. Query                     │
│                              ▼                                           │
│                    ┌──────────────────┐    ┌──────────────┐             │
│                    │  Glue Data       │───▶│    Athena    │             │
│                    │  Catalog +       │    │  (SQL)       │             │
│                    │  Crawler         │    └──────────────┘             │
│                    └──────────────────┘                                 │
│                                                                          │
│   Monitoring: CloudWatch Logs + Metrics at every stage                  │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Components

| # | Component | AWS Service | Purpose |
|---|-----------|-------------|---------|
| 1 | Raw Ingestion | Amazon S3 (raw zone) | Receive raw adverse event CSV files |
| 2 | Pipeline Trigger | AWS Lambda (Python 3.13) | Start Glue ETL job on S3 `ObjectCreated` event |
| 3 | ETL & Validation | AWS Glue + PySpark | Validate records, transform, partition, write Parquet |
| 4 | Valid Storage | Amazon S3 (processed zone) | Clean Parquet data, partitioned by `study_id` |
| 4 | Quarantine | Amazon S3 (quarantine zone) | Invalid records with `validation_error` column |
| 5 | Data Catalog | AWS Glue Data Catalog + Crawler | Schema registration and partition discovery |
| 6 | Query Layer | Amazon Athena | Ad-hoc SQL queries over processed data |
| 7 | Monitoring | Amazon CloudWatch | Logs, metrics, and alerts throughout the pipeline |
| — | IaC | AWS CloudFormation | All infrastructure defined as code (YAML) |

---

## Pharma Domain Rules

### Input Schema

| Field | Type | Validation Rule |
|-------|------|-----------------|
| `subject_id` | String | Pattern: `^SUB-\d{5}$` (e.g., `SUB-00123`) |
| `study_id` | String | Non-empty |
| `adverse_event` | String | Non-empty |
| `severity` | String | One of: `MILD` · `MODERATE` · `SEVERE` · `LIFE_THREATENING` |
| `onset_date` | Date | Format: `YYYY-MM-DD` |
| `report_date` | Date | Format: `YYYY-MM-DD`; must be ≥ `onset_date` |

### PII Policy

Only `subject_id` (a pseudonymized identifier) is permitted in the processed zone.
No patient names, dates of birth, or contact details may pass through the pipeline.

### Invalid Records

Records failing any validation rule are written to the **quarantine zone** with an
additional `validation_error` column describing every failed rule. They are never
silently dropped.

---

## Project Structure

```
/
├── CloudFormation/       → CloudFormation YAML templates (S3, IAM, Lambda, Glue, Athena)
├── lambda/               → Lambda function code (S3 → Glue trigger)
├── glue/                 → PySpark ETL scripts
├── athena/               → Athena DDL and analysis SQL queries
├── data/
│   └── sample/           → Sample CSV files for testing (mix of valid and invalid rows)
├── docs/                 → Architecture decisions, runbooks, data dictionary
├── CLAUDE.md             → Persistent project memory for Claude Code
└── README.md             → This file
```

---

## S3 Path Conventions

```
s3://<bucket>/raw/adverse_events/<filename>.csv   ← drop files here to trigger pipeline
s3://<bucket>/processed/adverse_events/           ← Parquet, partitioned by study_id
s3://<bucket>/quarantine/adverse_events/          ← invalid records with validation_error
s3://<bucket>/athena-results/                     ← Athena query output
```

---

## Getting Started

> **Status:** Project scaffold only — no AWS resources provisioned yet.

### Prerequisites

- Python 3.13
- AWS CLI v2, configured with credentials for your account
- AWS account (free-tier eligible)

### Local Development

```bash
# Clone and enter the project
git clone <repo-url>
cd aws_etl_with_claude_code

# (Optional) create a virtual environment
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# Test with sample data
cat data/sample/adverse_events.csv
```

### Deployment Order (coming soon)

1. Deploy `CloudFormation/01-storage.yaml` — S3 buckets + bucket policies
2. Deploy `CloudFormation/02-iam.yaml` — IAM roles for Lambda and Glue
3. Deploy `CloudFormation/03-glue.yaml` — Glue job + crawler
4. Deploy `CloudFormation/04-lambda.yaml` — Lambda function + S3 event trigger
5. Upload a test file to the raw S3 bucket to trigger the pipeline end-to-end
6. Query results in Athena

---

## AWS Region

All resources are deployed to **`us-east-1`** (US East, N. Virginia).

---

## License

Internal project — clinical trial data pipeline for pharma domain learning purposes.
All sample data is entirely fictional.
