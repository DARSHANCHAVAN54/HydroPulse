# Hydropulse Medallion Pipeline: IoT Hydroponics Data Platform

An end-to-end data engineering platform that ingests streaming smart hydroponics IoT sensor data, processes it via an automated Databricks Delta Live Tables (DLT) Medallion architecture, runs quality assurance audits, and powers a live executive operations dashboard.

---

## 🏗️ Architecture & System Topology

The pipeline follows a modern event-driven Lakehouse pattern:
1. **Producer Core:** Local Python application streaming newline-delimited JSON batches from an IoT CSV reference framework.
2. **Landing Zone:** Secure AWS S3 Object Storage bucket with structured directory paths.
3. **ETL Pipeline:** Databricks Delta Live Tables utilizing **Auto Loader (`cloudFiles`)** to stream incrementally from Bronze ➔ Silver ➔ Gold layers.
4. **Data Governance & SLA Audit:** Automated programmatic gate auditing volumetric error distributions and processing freshness.
5. **Insights Layer:** Databricks Lakeview Dashboard providing dynamic real-time operations overview.

---

## 🛠️ Tech Stack & Tools
- **Language:** Python 3.10+, SQL
- **Cloud Infrastructure:** AWS (S3, IAM, SNS)
- **Data Ingestion:** Boto3, Pandas
- **Processing Engine:** Databricks Delta Live Tables (DLT), PySpark Structured Streaming
- **Orchestration:** Databricks Workflows DAG (File Arrival Triggers)

---

## 🚀 Setup and Deployment Instructions

### Step 1: AWS Infrastructure Configuration
1. Log into your AWS Console and create an S3 bucket named `hydroponics-data-project-2026`. Inside, create the target directory: `hydropulse-medallion-lake/landing/iot_raw/`.
2. Open the **IAM Console**, create a user programmatic-only named `hydropulse-s3-ingest`, and attach a direct JSON inline policy matching the configuration block found in the project documentation.
3. Generate and record your **Access Key ID** and **Secret Access Key**.

### Step 2: Local Producer Initialization
Clone the repository and install required packages locally:
```bash
pip install -r requirements.txt
aws configure

python phase1_ingest.py

Step 3: Databricks Pipeline Configurations
Import the scripts hydropulse_dlt_notebook.py and Data_Audit_Notebook.py into your Databricks Workspace user directory.

Navigate to Delta Live Tables ➔ Create Pipeline. Set the pipeline parameters as follows:

Pipeline Name: hydropulse_medallion_pipeline

Source Code: Target path of hydropulse_dlt_notebook

Target Schema: hydropulse_db

Compute/Photon: Enabled

Under Advanced ➔ Add Configuration, provide your secure access properties:

fs.s3a.access.key

fs.s3a.secret.key

Step 4: Workflow DAG Orchestration
Go to Workflows ➔ Jobs and build a coordinated 3-stage chain:

Task 1 (hydroflow): Run the hydropulse_medallion_pipeline DLT asset.

Task 2 (Audit): Execute the Data_Audit_Notebook (Depends on hydroflow).

Task 3 (Refresh_Dashboard): Refresh target visualization panels (Depends on Audit).

Set the pipeline trigger type to File Arrival pointed at your S3 telemetry data path landing directory.
