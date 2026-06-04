Here is a comprehensive, production-ready `README.md` file designed specifically for your GitHub repository. It integrates your entire end-to-end architecture, the corrected business logic, and clear setup guidelines.

---

# HydroPulse: Real-Time Hydroponics Automation Data Platform

An end-to-end, production-grade IoT data platform built on the Databricks Medallion Architecture. This system ingests real-time environmental telemetry (pH, TDS, temperature, humidity) from automated hydroponic greenhouses via AWS S3, processes it using Delta Live Tables (DLT), applies automated data-quality gates, routes multi-severity alarms, and refreshes an executive control dashboard.

---

## 1. Project Structure

```text
├── .github/
│   └── workflows/                # CI/CD deployment automation pipelines
├── config/
│   └── dlt_pipeline_conf.json    # Delta Live Tables environmental settings
├── notebooks/
│   ├── 01_bronze_ingestion.py    # Auto Loader ingestion from S3 landing zone
│   ├── 02_silver_cleaning.py     # Schema validation and telemetry boundary checks
│   └── 03_gold_analytical.py     # Advanced KPI aggregation & alarm engine (Latest Code)
├── dashboards/
│   └── hydroponics_control.json  # Databricks Dashboard UI layout export
├── scripts/
│   └── s3_data_purge.sh          # AWS CLI emergency data wipe script
├── README.md                     # Project documentation
└── requirements.txt              # Local developer dependencies

```

---

## 2. Platform Architecture & Workflow

The platform leverages a fully managed Lakehouse pattern to convert unstable physical sensor data into actionable greenhouse business logic.

```
[IoT Sensors] -> [AWS S3 Landing] -> [Bronze: Raw Ledger] -> [Silver: Cleaned/Enriched] -> [Gold: KPIs & Alarms] -> [SQL Warehouse] -> [Interactive Dashboard]

```

### Infrastructure & Tools Used

* **Storage Layer:** **AWS S3** (`s3://hydroponics-data-project-2026/`) as the scalable landing zone for unstructured JSON/CSV telemetry logs.
* **Ingestion Engine:** **Databricks Auto Loader (`cloud_files`)** configured with directory-listing mode to track incoming files efficiently without costly cloud infrastructure overhead.
* **Transformation Pipeline:** **Delta Live Tables (DLT)** for a fully auditable, streaming data pipeline managed natively through continuous or scheduled execution blocks.
* **Orchestration:** **Databricks Workflows (DAG)** governing a multi-task chain that manages execution order, computes resources, and triggers downstream actions on complete pipeline success.
* **Serving Engine:** **Serverless SQL Warehouse** for compute virtualization to isolate operational query load from heavy processing tasks.

---

## 3. Production Pipeline Code

This script contains the production-ready **Gold Layer Engine** implemented inside the DLT framework. It incorporates a critical business logic upgrade that prevents empty alert states by dynamically processing both `WARNING` and `CRITICAL` environmental status streams.

```python
import dlt
from pyspark.sql.functions import col, date_trunc, avg, round, when, lit, concat, current_timestamp
from pyspark.sql.functions import sum as spark_sum, count

# ==============================================================================
# GOLD LAYER (Advanced Analytical KPI Engine & Alarm Router)
# ==============================================================================

# -- Gold Table 1: Hourly Sensor KPIs ------------------------------------------
@dlt.table(
    name="gold_sensor_hourly",
    comment="Materialized hourly crop KPIs, system stresses, and pump duty-cycle metrics."
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
            
            # Resource Cycles Accumulation
            spark_sum("add_water").alias("water_usage_cycles"),
            spark_sum("nutrients_adder").alias("nutrient_usage_cycles"),
            spark_sum("pH_reducer").alias("ph_correction_cycles"),
            spark_sum(when((col("ex_fan") == 1) | (col("humidifier") == 1), lit(1)).otherwise(lit(0))).alias("climate_energy_cycles"),
            
            # Actuator Duty Cycles (Percentage of time active)
            round(avg("add_water") * 100, 2).alias("water_pump_duty"),
            round(avg("nutrients_adder") * 100, 2).alias("nutrient_pump_duty"),
            round(avg("pH_reducer") * 100, 2).alias("ph_correction_duty"),
            round(avg("ex_fan") * 100, 2).alias("exhaust_fan_duty"),
            round(avg("humidifier") * 100, 2).alias("humidifier_duty"),
            
            # Biological Stress Rates
            round(avg("pH_stress") * 100, 2).alias("pH_stress_rate"),
            round(avg("TDS_stress") * 100, 2).alias("TDS_stress_rate"),
            round(avg("temp_stress") * 100, 2).alias("temp_stress_rate"),
            
            # Telemetry Core Averages
            round(avg("pH"), 2).alias("avg_pH"),
            round(avg("TDS"), 2).alias("avg_TDS"),
            round(avg("ambient_temp"), 2).alias("avg_air_temp"),
            round(avg("ambient_humidity"), 2).alias("avg_humidity"),
            round(avg("water_temp"), 2).alias("avg_water_temp"),
            round(avg("water_level"), 2).alias("avg_water_level"),
            
            # Environmental Optimization Rates
            round(avg(when(col("pH_status") == "OPTIMAL", lit(1.0)).otherwise(lit(0.0))) * 100, 2).alias("pH_optimal_rate"),
            round(avg(when(col("TDS_status") == "OPTIMAL", lit(1.0)).otherwise(lit(0.0))) * 100, 2).alias("TDS_optimal_rate")
        )
        .withColumn("gold_update_time", current_timestamp().cast("string"))
    )

# -- Gold Table 2: Farm-Level Metrics ------------------------------------------
@dlt.table(
    name="gold_farm_kpi",
    comment="High-level operational farm dashboard KPIs for executive risk monitoring."
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

# -- Gold Table 3: Real-time Critical Alerts ------------------------------------
@dlt.table(
    name="gold_alerts",
    comment="Filtered stream routing threshold anomalies (WARNING & CRITICAL) requiring intervention."
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

## 4. Setup & Run Instructions

Follow these instructions to spin up the data pipeline or execute a complete cold wipe and reset.

### Prerequisites

* AWS CLI installed and configured with appropriate permissions.
* Databricks Workspace access with permissions to create DLT Pipelines and Workflows.

### Initial Deployment Steps

1. **Stage Files in AWS S3:** Drop your raw sensor payloads into the target directory bucket folder.
2. **Create Delta Live Tables Pipeline:**
* Navigate to **Delta Live Tables** $\rightarrow$ **Create Pipeline**.
* Set Product Edition to **Advanced** (required for Data Quality expectations support).
* Link the source paths to the `notebooks/` directory containing your code logic.
* Specify the Target Schema as `hydropulse_db`.


3. **Configure Databricks Workflow (DAG Linkage):**
* Create a new Multi-Task Workflow Job.
* **Task 1:** Create a `Pipeline` task linking out to your Hydroponics DLT pipeline block. Configure it on a Cron schedule (e.g., every 15 minutes) using directory listing mode to safely sidestep AWS S3 event notification blocks.
* **Task 2:** Create a downstream `Dashboard` task dependent on Task 1. Map it directly to your Hydroponics Control Dashboard via an active Serverless SQL Warehouse.



---

### Execution Routine: Running a 100% Fresh Start

If data corruptions appear or schemas migrate, use these steps to wipe your architecture and run a clean data historical reset.

#### Step 1: Wipe the AWS S3 Ingestion Folder

Execute an isolated recursive removal using the AWS CLI tool to drop old source objects:

```bash
aws s3 rm s3://hydroponics-data-project-2026/hydropulse-medallion-lake/landing/iot_raw/ --recursive

```

#### Step 2: Clear Pipelines and Reset Metadata

Because DLT builds underlying physical dependencies as specialized streaming tables and views, running a standard manual `TRUNCATE TABLE` via SQL will result in a `EXPECT_TABLE_NOT_VIEW` compilation check failure.

To correctly reset the tables and clear hidden Auto Loader streaming state checkpoints:

1. Go to your **Delta Live Tables UI** inside Databricks.
2. Click the options expansion button (`...`) next to your pipeline execution interface.
3. Click **Reset** (this completely purges the lifecycle state and underlying data structures safely).
4. Once completed, click the drop arrow adjacent to the **Start** button and select **Full Refresh All** to cleanly re-ingest fresh landing data from zero.

---
