# Hydropulse Medallion Pipeline: IoT Hydroponics Platform

An end-to-end data platform designed to process high-frequency smart hydroponic sensor metrics via a resilient multi-tier Lakehouse pattern.

---

## 🔒 Security Architecture Model

This pipeline decouples external network ingestion from backend analysis using two distinct security domains:
1. **Local Machine Ingestion:** Uses a dedicated **IAM User** (`hydropulse-s3-ingest`) with static Access Keys configured via the CLI to push raw telemetry to S3.
2. **Databricks Cloud Processing:** Uses an **IAM Role** (`databricks-s3-ingest-81c4c-db_s3_iam`) managed via AWS CloudFormation. This allows Auto Loader to securely access S3, SNS, and SQS assets natively without hardcoding access keys in your pipeline settings.

---

## 🚀 Execution & Setup Guide

### 1. Initialize Ingestion Engine
Ensure you have your credential context set up on your local machine:
```bash
pip install -r requirements.txt
aws configure
```

Run the local producer loop to stream raw records to your S3 landing path:

```bash
python phase1_ingest.py
```
### 2. Run the Medallion Infrastructure
Import hydropulse_dlt_notebook.py and Data_Audit_Notebook.py to your Databricks workspace.

Create your Delta Live Tables asset using the structure details inside databricks/config/pipeline_settings.json.

### 3. Orchestrate the Workflows DAG
Navigate to Databricks Workflows and assemble your jobs sequentially:

Task 1 (hydroflow): Triggers your Delta Live Tables processing.

Task 2 (Audit): Executes verification assertions (Depends on hydroflow).

Task 3 (Refresh_Dashboard): Updates your analytics dashboards (Depends on Audit).

Configure an event-driven File Arrival Trigger aimed directly at s3://hydroponics-data-project-2026/hydropulse-medallion-lake/landing/iot_raw/ to trigger processing runs automatically.
