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

