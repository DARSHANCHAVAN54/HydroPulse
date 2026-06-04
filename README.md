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
├── notebooks/
│ └── hydropulse_medallion_pipeline.py
├── dashboards/
│ └── hydro_dashboard_assets/
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
