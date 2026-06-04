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
├── .gitignore                  # Prevents committing local developer environments or credentials
├── README.md                   # Core system documentation and run playbook
├── requirements.txt            # Local development packages
├── config/
│   └── dlt_pipeline_conf.json  # Declarative JSON metadata blueprint for DLT deployment
├── notebooks/
│   ├── 01_bronze_ingestion.py  # Ingestion notebook using Spark Auto Loader (cloud_files)
│   ├── 02_silver_cleaning.py   # Cleansing logic and native DLT expectation rules
│   └── 03_gold_analytical.py   # Gold Layer analytical KPI and alerting engine
├── dashboards/
│   └── hydroponics_control.json# Exported definitions file for the control interface
└── scripts/
    └── s3_data_purge.sh        # Controlled script for administrative data cleaning
