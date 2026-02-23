# CLAUDE.md — AWS Clinical Trial Adverse Event ETL Pipeline

This file is persistent project memory for Claude Code. Always read and follow these rules.

---

## Project Context

| Setting | Value |
|---------|-------|
| AWS Region | us-east-1 |
| Infrastructure as Code | AWS CloudFormation **only** (no Terraform) |
| Language | Python 3.13 |
| Version Control | Git |
| ETL Engine | AWS Glue with PySpark |
| Pipeline Trigger | AWS Lambda on S3 upload event |
| Storage | Amazon S3 (raw zone + processed zone + quarantine zone) |
| Catalog | AWS Glue Data Catalog + Glue Crawler |
| Query Layer | Amazon Athena |
| Monitoring | Amazon CloudWatch |

**Constraint:** Use **free-tier AWS services only**.

---

## Project Structure

```
/
├── CloudFormation/       → CloudFormation YAML templates
├── lambda/               → Lambda function code (Python 3.13)
├── glue/                 → Glue PySpark ETL scripts
├── athena/               → Athena SQL queries
├── data/
│   └── sample/           → Sample/test CSV files
├── docs/                 → Architecture and runbook documentation
├── CLAUDE.md             → This file (project memory)
└── README.md             → Project overview
```

---

## Pharma Domain Rules

### Input Data
- Source: Clinical trial adverse event CSV files
- Delivered to: `s3://<bucket>/raw/adverse_events/`

### Mandatory Fields
Every record **must** contain all of the following non-null, non-empty fields:

| Field | Description |
|-------|-------------|
| `subject_id` | Pseudonymized trial subject identifier |
| `study_id` | Clinical study identifier |
| `adverse_event` | Description of the adverse event |
| `severity` | Severity classification |
| `onset_date` | Date the event began |
| `report_date` | Date the event was reported |

### Field Validation Rules

| Field | Rule |
|-------|------|
| `subject_id` | Must match regex `^SUB-\d{5}$` (e.g., SUB-00123) |
| `severity` | Must be exactly one of: `MILD`, `MODERATE`, `SEVERE`, `LIFE_THREATENING` |
| `onset_date` | Must be in `YYYY-MM-DD` format and a valid calendar date |
| `report_date` | Must be in `YYYY-MM-DD` format; must be >= `onset_date` |

### PII Policy
- **No PII** beyond `subject_id` is permitted in the processed zone
- `subject_id` is a pseudonymized identifier — not a real name, DOB, or contact detail
- Never write patient names, dates of birth, addresses, or contact information anywhere in the pipeline

---

## S3 Path Conventions

```
s3://<bucket>/raw/adverse_events/<filename>.csv       ← ingest here
s3://<bucket>/processed/adverse_events/               ← valid records (Parquet, partitioned by study_id)
s3://<bucket>/quarantine/adverse_events/              ← invalid records (Parquet, with validation_error column)
```

---

## Coding Standards

- **Python:** 3.13 — use modern syntax and standard library
- **PySpark:** Used inside all Glue ETL jobs
- **CloudFormation:** YAML format preferred; tag all resources with:
  - `Project: ClinicalTrialAEPipeline`
  - `Environment: dev`
- **Secrets:** Never hardcode credentials — use IAM roles and environment variables
- **Error handling:** Invalid records go to quarantine with a `validation_error` column; never silently drop records

---

## Key Design Decisions

1. Lambda triggers Glue job by passing `s3_bucket` and `s3_key` as Glue job parameters
2. Glue job validates each record against pharma domain rules before writing
3. Valid records → Parquet in processed zone, partitioned by `study_id`
4. Invalid records → Parquet in quarantine zone with `validation_error` field
5. Glue Crawler updates the Data Catalog after each ETL run
6. Athena queries run against the Glue Data Catalog (no data movement)

---

## Skills

See `.claude/skills/pharma-etl.md` for detailed ETL validation patterns, PySpark templates, and CloudFormation conventions Claude should always apply in this project.

---

## Important Guardrails

- Do **not** provision real AWS resources without explicit user instruction
- Always validate data against pharma domain rules **before** writing to processed zone
- Always quarantine invalid records — never drop them silently
- Always use CloudFormation (not console clicks or SDK calls) for infrastructure
