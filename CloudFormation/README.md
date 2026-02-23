# CloudFormation Templates

AWS CloudFormation YAML templates for provisioning all pipeline infrastructure.

## Planned Templates

| File | Purpose |
|------|---------|
| `01-storage.yaml` | S3 buckets: raw, processed, quarantine, athena-results |
| `02-iam.yaml` | IAM roles and policies for Lambda, Glue, and Crawler |
| `03-glue.yaml` | Glue job definition and Glue Crawler |
| `04-lambda.yaml` | Lambda function and S3 event notification trigger |
| `05-athena.yaml` | Athena workgroup and output location configuration |

## Conventions

- Format: YAML
- All resources tagged with `Project: ClinicalTrialAEPipeline` and `Environment: dev`
- Use `!Sub`, `!Ref`, `!GetAtt` for dynamic values — no hardcoded ARNs
- Key outputs exported for cross-stack references

## Deployment Order

Deploy in numerical order — each stack may depend on outputs from the previous.
