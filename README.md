```markdown
# HydroPulse: Hydroponics Telemetry & Automation Data Platform

An end-to-end IoT data platform built on the Databricks Medallion Architecture.
This system ingests real-time environmental telemetry (pH, TDS, temperature, humidity, water levels) from
automated greenhouse microcontrollers, processes it using Delta Live Tables (DLT) streaming pipelines, enforces
strict data-quality thresholds, isolates automated system alert states, and runs an executive analytics control dashboard.

---

## 1. Project Folder Structure

The repository is organized following a strict separation of concerns, decoupling orchestration configurations, infrastructure scripts, core processing logic, and analytical visual layouts:

```text
hydropulse-automation-platform/
├── README.md                   # Comprehensive project documentation and execution playbook
├── requirements.txt            # Local Python dependencies for testing and development
├── config/
│   └── dlt_pipeline_conf.json  # Configuration settings for Delta Live Tables pipeline deployment
├── notebooks/
│   ├── 01_bronze_ingestion.py  # Ingestion streams via Auto Loader (cloud_files) from AWS S3
│   ├── 02_silver_cleaning.py   # Data cleansing, schema validation, and threshold tagging
│   └── 03_gold_analytical.py   # Production Gold Layer KPI metrics and multi-severity alert logic
├── dashboards/
│   └── hydroponics_control.json# Exported layout definitions for the executive control panel
└── scripts/
    └── s3_data_purge.sh        # Controlled script for administrative data purges

```

---

## 2. Technologies & Tools Used

* **Cloud Infrastructure:** AWS S3 (Scalable Landing Zone for IoT raw files).
* **Ingestion Engine:** Databricks Auto Loader (`cloud_files` API with optimized Directory Listing mode).
* **Distributed Processing Framework:** Apache Spark 3.x (PySpark Structured Streaming & Spark SQL).
* **Pipeline Orchestration Framework:** Delta Live Tables (DLT declarative batch and streaming pipelines).
* **Workflow Engine:** Databricks Workflows (Multi-Task Directed Acyclic Graph automation).
* **Data Serving Layer:** Unity Catalog & Serverless SQL Warehouses.
* **Visualization Layer:** Databricks Lakeview Dashboards (Real-Time Control Interface).

---

## 3. Platform Architecture & Data Pipeline Workflow

The platform leverages a fully managed Lakehouse architecture to move data seamlessly from edge sensor arrays to physical operations dashboards:

```
 ┌──────────────┐      ┌─────────────────┐      ┌─────────────────┐      ┌──────────────┐
 │    IoT Edge  │ ───> │  AWS S3 Bucket  │ ───> │  Bronze Layer   │ ───> │ Silver Layer │
 │ Sensors/Gwy  │      │ (Raw JSON/CSV)  │      │ (Append Ledger) │      │ (Enriched/DQ)│
 └──────────────┘      └─────────────────┘      └─────────────────┘      └──────┬───────┘
                                                                                │
 ┌──────────────┐      ┌─────────────────┐      ┌─────────────────┐             │
 │ Databricks   │ <─── │  SQL Warehouse  │ <─── │   Gold Layer    │ <───────────┘
 │ Dashboard    │      │ (Serverless SQL)│      │  (KPIs & Alerts)│
 └──────────────┘      └─────────────────┘      └─────────────────┘

```

### Well-Documented Data Flow

1. **Ingestion Zone (Bronze):** Automated directory listing reads incoming payloads from `s3://hydroponics-data-project-2026/hydropulse-medallion-lake/landing/iot_raw/`. Data is preserved as an immutable historical ledger in the streaming table `bronze_iot_raw`.
2. **Quality Gates & Enrichment (Silver):** The `silver_iot` streaming table enforces explicit DLT data quality expectations. It converts string dates into operational timestamps, maps telemetry readings against biological boundaries, and flings automated mechanical flags (`add_water`, `pH_reducer`) if values drift out of optimal ranges.
3. **Aggregation & Routing (Gold):** Refined data splits into three purpose-built business tables:
* `gold_sensor_hourly`: Compares operational mechanical actuator workloads against actual plant stress metrics.
* `gold_farm_kpi`: Surface-level metrics tracking macro farm health.
* `gold_alerts`: A real-time routing stream capturing any row flashing non-optimal parameters (`WARNING` or `CRITICAL`) and constructing precise text-alert strings.



---

## 4. Production Source Code (Gold Layer Processing Engine)

The following production script implements the final **Gold Layer Engine** within the Delta Live Tables framework, featuring robust string-concatenation macros to handle multi-severity alert criteria:

```python
import dlt
from pyspark.sql.functions import col, date_trunc, avg, round, when, lit, concat, current_timestamp
from pyspark.sql.functions import sum as spark_sum, count

# ==============================================================================
# GOLD LAYER: ADVANCED ANALYTICAL METRICS & ALARM ENGINE
# ==============================================================================

# -- Gold Table 1: Hourly Crop & System Metrics --------------------------------
@dlt.table(
    name="gold_sensor_hourly",
    comment="Materialized hourly crop metrics, system stressors, and actuator duty-cycles."
)
def gold_sensor_hourly():
    return (
        dlt.read("silver_iot")
        .groupBy(date_trunc("hour", col("event_timestamp")).alias("window_start"))
        .agg(
            count("*").alias("total_readings"),
            round(avg("health_score"), 2).alias("avg_health_score"),
            round((spark_sum(when(col("health_score") >= 80, lit(1.0)).otherwise(lit(0.0))) / count("*") * 100), 2).alias("optimal_growth_pct"),
            spark_sum(when(col("health_score") <= 40, lit(1)).otherwise(lit(0))).alias("stress_incidents"),
            
            # Mechanical Actuator Usage Trackers
            spark_sum("add_water").alias("water_usage_cycles"),
            spark_sum("nutrients_adder").alias("nutrient_usage_cycles"),
            spark_sum("pH_reducer").alias("ph_correction_cycles"),
            spark_sum(when((col("ex_fan") == 1) | (col("humidifier") == 1), lit(1)).otherwise(lit(0))).alias("climate_energy_cycles"),
            
            # Actuator Duty Cycles (Percentage of active duration window)
            round(avg("add_water") * 100, 2).alias("water_pump_duty"),
            round(avg("nutrients_adder") * 100, 2).alias("nutrient_pump_duty"),
            round(avg("pH_reducer") * 100, 2).alias("ph_correction_duty"),
            round(avg("ex_fan") * 100, 2).alias("exhaust_fan_duty"),
            round(avg("humidifier") * 100, 2).alias("humidifier_duty"),
            
            # Biological Stress Incidence
            round(avg("pH_stress") * 100, 2).alias("pH_stress_rate"),
            round(avg("TDS_stress") * 100, 2).alias("TDS_stress_rate"),
            round(avg("temp_stress") * 100, 2).alias("temp_stress_rate"),
            
            # Normalized Core Telemetry
            round(avg("pH"), 2).alias("avg_pH"),
            round(avg("TDS"), 2).alias("avg_TDS"),
            round(avg("ambient_temp"), 2).alias("avg_air_temp"),
            round(avg("ambient_humidity"), 2).alias("avg_humidity"),
            round(avg("water_temp"), 2).alias("avg_water_temp"),
            round(avg("water_level"), 2).alias("avg_water_level"),
            
            # Threshold Compliance Rates
            round(avg(when(col("pH_status") == "OPTIMAL", lit(1.0)).otherwise(lit(0.0))) * 100, 2).alias("pH_optimal_rate"),
            round(avg(when(col("TDS_status") == "OPTIMAL", lit(1.0)).otherwise(lit(0.0))) * 100, 2).alias("TDS_optimal_rate")
        )
        .withColumn("gold_update_time", current_timestamp().cast("string"))
    )

# -- Gold Table 2: Farm-Level High-Level KPIs ----------------------------------
@dlt.table(
    name="gold_farm_kpi",
    comment="High-level aggregated farm metrics for executive dashboard analysis."
)
def gold_farm_kpi():
    return (
        dlt.read("silver_iot")
        .groupBy(date_trunc("hour", col("event_timestamp")).alias("window_start"))
        .agg(
            round(avg("health_score"), 2).alias("farm_avg_health"),
            spark_sum(when(col("health_score") < 60, lit(1)).otherwise(lit(0))).alias("total_yield_risk_incidents"),
            (spark_sum("add_water") + spark_sum("nutrients_adder") + spark_sum("pH_reducer")).alias("total_resource_cycles"),
            spark_sum(when((col("ex_fan") == 1) | (col("humidifier") == 1), lit(1)).otherwise(lit(0))).alias("total_energy_cycles")
        )
        .withColumn("gold_update_time", current_timestamp().cast("string"))
    )

# -- Gold Table 3: Multi-Severity Real-Time Alerts ------------------------------
@dlt.table(
    name="gold_alerts",
    comment="Filtered real-time threshold anomalies capturing both WARNING & CRITICAL profiles."
)
def gold_alerts():
    return (
        dlt.read("silver_iot")
        .filter(
            col("pH_status").isin("CRITICAL", "WARNING") |
            col("TDS_status").isin("CRITICAL", "WARNING") |
            col("DHT_temp_status").isin("CRITICAL", "WARNING") |
            col("water_temp_status").isin("CRITICAL", "WARNING")
        )
        .select(
            col("event_timestamp").cast("string").alias("alert_timestamp"),
            col("pH"), 
            col("TDS"), 
            col("ambient_temp").alias("air_temp"), 
            col("water_temp"),
            col("pH_status"), 
            col("TDS_status"),
            col("DHT_temp_status").alias("air_temp_status"), 
            col("water_temp_status"),
            col("health_score"), 
            col("row_health"),
            
            # Dynamic String Construction Macro for Alert Delivery
            when(
                col("pH_status").isin("CRITICAL", "WARNING"), 
                concat(col("pH_status"), lit(" Risk Detected: "), col("pH").cast("string"))
            ).otherwise(lit(None)).alias("pH_alert_msg"),
            
            when(
                col("TDS_status").isin("CRITICAL", "WARNING"), 
                concat(col("TDS_status"), lit(" Risk Detected: "), col("TDS").cast("string"))
            ).otherwise(lit(None)).alias("TDS_alert_msg"),
            
            when(
                col("DHT_temp_status").isin("CRITICAL", "WARNING"), 
                concat(col("DHT_temp_status"), lit(" Temp Risk Detected: "), col("ambient_temp").cast("string"))
            ).otherwise(lit(None)).alias("temp_alert_msg")
        )
    )

```

---

## 5. Setup & Run Instructions

### Initial Deployment Playbook

1. **Repository Sync:** Import this repository directly into your Databricks workspace via **Workspace** -> **Repos** -> **Add Repo**.
2. **Configure Delta Live Tables Pipeline:**
* Navigate to **Delta Live Tables** in the Databricks sidebar and click **Create Pipeline**.
* Select **Advanced** product edition to support structural Data Quality expectations.
* Add the path strings pointing to your cloned `/notebooks/` directory.
* Set the **Target Schema** configuration parameter to `hydropulse_db`.


3. **Assemble Multi-Task Workflow DAG:**
* Navigate to **Workflows** -> **Create Job**.
* **Task 1 (`Run_Hydroponics_Pipeline`):** Set task type to `Pipeline`, select your DLT pipeline, and assign a cron schedule (e.g., every 15 minutes) or a manual execution pattern. This bypasses structural cloud permission restrictions (`s3:GetBucketNotification`).
* **Task 2 (`Refresh_Hydroponics_Dashboard`):** Set task type to `Dashboard`, click on your imported Hydroponics layout, select an active **Serverless SQL Warehouse** for isolated query serving, and mark its dependency on **Task 1**.



---

## 6. Cold Start & Data Resets Playbook

If schemas migrate or an upstream sensor error corrupts data, standard manual `TRUNCATE TABLE` operations will throw an `EXPECT_TABLE_NOT_VIEW` failure because DLT registers pipeline assets as Materialized Views and Streaming Tables in Unity Catalog.

To execute an infrastructure-wide reset and process all historical logs completely clean from scratch, execute this exact two-step sequence:

### Step 1: Wipe S3 Ingestion Folders via AWS CLI

```bash
aws s3 rm s3://hydroponics-data-project-2026/hydropulse-medallion-lake/landing/iot_raw/ --recursive

```

### Step 2: Clear DLT Lifecycle State and Trigger Re-compilation

1. Navigate to your **Delta Live Tables UI** in Databricks.
2. Click the options dropdown icon (`...`) next to the pipeline controls panel.
3. Click **Reset** (this completely drops the managed views and purges old checkpoint tracking metadata folders).
4. Once finished, click the dropdown arrow next to the **Start** button and select **Full Refresh All** to initialize a clean re-ingestion cycle.

---

## 7. Security & Credentials Management

This project maintains absolute compliance with cloud security standards and enterprise confidentiality requirements:

* **Zero Credentials Committed:** No AWS Access Keys, secret tokens, passwords, or explicit parameters exist inside the source text scripts.
* **IAM Role Assumptions:** Infrastructure connectivity relies entirely on secure token-profile mappings using AWS Security Token Service (`arn:aws:sts::818783924384:assumed-role/databricks-s3-ingest-81c4c-db_s3_iam`).
* **Robust Access Boundaries:** The `.gitignore` file strictly blocks environment profile configurations (`.env`), AWS credentials cache directories (`.aws/`), and encryption keys (`*.pem`).

---

## 8. Git Commit History Policy

This repository implements **Conventional Commits** to clearly document the development lifecycle. Merges to production require descriptive tags that catalog technical progress:

* `feat(ingest): establish auto loader streaming utilizing directory listing mode`
* `docs(readme): structure multi-task setup playbook and file topology maps`
* `fix(gold): widen alert filter array to route warning rows into alert table`
* `chore(security): apply robust gitignore blocks to exclude local run environments`

---

## 9. Verification & Validation Framework

To guarantee the pipeline and downstream dashboard are operational, verify the following platform parameters after execution:

* **DLT DAG Execution Graph:** Every node (`bronze_iot_raw` $\rightarrow$ `silver_iot` $\rightarrow$ `gold_alerts`) must display a **Green** state indicator upon pipeline completion.
* **Quality Metrics Verification:** Ensure that querying the `gold_alerts` table accurately surfaces rows containing both `WARNING` and `CRITICAL` flags, capturing the historical rows where system parameters drifted outside optimal ranges.
* **Downstream Orchestration Status:** Check the **Job Runs** logs under the Workflows panel to verify that Task 1 and Task 2 successfully complete in sequence without missing data updates.

```

```
