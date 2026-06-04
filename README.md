# HydroPulse: Hydroponic Sensor Data Pipeline on Databricks Lakehouse

An end-to-end, IoT telemetry platform built on the Databricks Lakehouse architecture. This system ingests high-velocity environmental data from automated hydroponic greenhouses, processes it using Delta Live Tables (DLT) streaming pipelines, enforces declarative data quality thresholds, routes multi-severity operational alarms, and dynamically updates an executive control panel dashboard.

---

## 1. Problem Statement & Solution

### The Problem
Automated hydroponic operations continuously generate high-velocity IoT telemetry arrays (pH, TDS, temperature, humidity, water levels) that require deterministic, low-latency ingestion, automated schema evolution, and rigid data-quality enforcement. Without a unified processing ecosystem, production environments suffer from decoupled orchestration, fragile file event listeners, complex checkpoint management, and high compute overhead, preventing immediate visibility into life-critical agricultural anomalies.

### The Solution
HydroPulse is a cloud-native unified data platform. Raw sensor data landing in AWS S3 is processed via **Delta Live Tables (DLT)** under a streaming Medallion Architecture. Built-in quality rules trap out-of-bounds readings, and advanced analytical calculations track actuator duty cycles against crop biological stress metrics. Databricks Workflows orchestrate the execution lifecycle, triggering an automated refresh of real-time executive dashboards via a Serverless SQL Warehouse upon pipeline completion.

---

## 2. Project Folder Structure

The repository is organized following a clean, modular structure that keeps source code, system deployment parameters, layout configurations, and administrative tools distinct:

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
│   └── 03_gold_analytical.py   # Production Gold Layer analytical KPI and alerting engine
├── dashboards/
│   └── hydroponics_control.json# Exported definitions file for the real-time control interface
└── scripts/
    └── s3_data_purge.sh        # Controlled script for administrative data cleaning
