# HydroPulse: Hydroponic Sensor Data Pipeline on Databricks Lakehouse

An end-to-end data pipeline built on the Databricks Lakehouse architecture. This mini project ingests environmental telemetry data from automated hydroponic greenhouse sensors, processes it using Delta Live Tables (DLT) streaming data pipelines, enforces data quality thresholds, isolates multi-severity alert states, and updates an analytical control panel dashboard.

---

## 1. Problem Statement & Solution

### Problem Statement
Automated hydroponic operations continuously generate high-velocity IoT telemetry arrays (pH, TDS, temperature, humidity, water levels) that require validation and standardized data-quality enforcement. Without a unified processing pipeline, data quality, asset tracking, and consistent metric calculations are difficult to maintain across various environment changes.

### Project Description
HydroPulse implements a clean, cloud-native data pipeline. Raw sensor data landing in AWS S3 is ingested via **Databricks Auto Loader (`cloud_files`)** and processed through a streaming Medallion Architecture (Bronze-Silver-Gold) using **Delta Live Tables (DLT)**. Built-in quality rules flag out-of-bounds readings, and analytical calculations track actuator workloads against crop stress metrics. **Databricks Workflows** orchestrate the execution lifecycle, triggering an automated refresh of the control dashboard via a **Serverless SQL Warehouse** upon pipeline completion.

---

## 2. Project Folder Structure

The repository is organized following a clean, modular layout that keeps source code, system deployment parameters, dashboard configurations, and administrative tools distinct:

```text
hydropulse-automation-platform/
├── .gitignore
├── README.md
├── requirements.txt
├── config/
│ └── dlt_pipeline_conf.json
├── hydropulse_medallion_pipeline.py
```
```text
## Step 1: Clone the Repository

```bash
git clone https://github.com/<your-username>/hydropulse-automation-platform.git
cd hydropulse-automation-platform
```

---

## Step 2: Upload Project to Databricks

1. Open Databricks Workspace.
2. Create a new folder named:

```text
hydropulse-automation-platform
```

3. Upload:

* notebooks/
* config/
* scripts/

folders into the workspace.

---

## Step 3: Configure Data Source

Upload sensor data files to your configured S3 location.

Example:

```text
s3://hydropulse-data/raw/
```

Update the source path inside the DLT notebook if required.

---

## Step 4: Create Schema

Execute:

```sql
CREATE SCHEMA IF NOT EXISTS hydropulse_db2;
```

---

## Step 5: Create DLT Pipeline

1. Navigate to **Workflows → Delta Live Tables**.
2. Click **Create Pipeline**.
3. Configure:

| Setting          | Value                                   |
| ---------------- | --------------------------------------- |
| Pipeline Name    | HydroPulse                              |
| Source Code      | notebooks/hydropulse_medallion_pipeline |
| Target Schema    | hydropulse_db2                          |
| Storage Location | DBFS or S3 location                     |
| Pipeline Mode    | Continuous or Triggered                 |

4. Save the pipeline.

---

## Step 6: Start Pipeline

Click:

```text
Start
```

The pipeline automatically performs:

* Auto Loader ingestion
* Bronze table creation
* Silver table validation
* Quarantine record separation
* Gold table generation

---

## Step 7: Verify Tables

After successful execution, verify the following tables:

### Bronze

```sql
SELECT * FROM bronze_iot LIMIT 10;
```

### Silver

```sql
SELECT * FROM silver_iot LIMIT 10;
```

### Quarantine

```sql
SELECT * FROM quarantine_iot LIMIT 10;
```

### Gold Alerts

```sql
SELECT * FROM gold_alerts LIMIT 10;
```

### Gold Sensor Hourly

```sql
SELECT * FROM gold_sensor_hourly LIMIT 10;
```

### Gold Farm KPI

```sql
SELECT * FROM gold_farm_kpi LIMIT 10;
```

---

## Step 8: Build Dashboard

1. Open Databricks SQL.
2. Create a new Dashboard.
3. Connect the following datasets:

```text
workspace.hydropulse_db2.gold_alerts
workspace.hydropulse_db2.gold_sensor_hourly
workspace.hydropulse_db2.gold_farm_kpi
```

4. Create visualizations:

   * Alert Summary
   * Hourly Sensor Trends
   * Farm KPI Cards
   * Environmental Health Metrics

5. Save and publish the dashboard.

---

## Maintenance

To clean historical data:

```bash
chmod +x scripts/s3_data_purge.sh
./scripts/s3_data_purge.sh
```

Use only in non-production environments unless approved.

---

## Expected Output

After deployment, the platform provides:

* Automated file ingestion
* Data quality enforcement
* Quarantine management
* Alert generation
* Hourly sensor analytics
* Farm KPI reporting
* Interactive Databricks dashboard

```
```text
End-to-End Data Flow

Sensor Data Files
        │
        ▼
Auto Loader
        │
        ▼
bronze_iot
        │
        ▼
silver_iot
        │
 ┌──────┴──────┐
 ▼             ▼
Valid      Invalid
Data        Data
 │             │
 ▼             ▼
Gold       quarantine_iot
Tables
 │
 ▼
Databricks Dashboard
```
