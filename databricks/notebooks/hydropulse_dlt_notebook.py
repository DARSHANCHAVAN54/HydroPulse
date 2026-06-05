import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

S3_LANDING_PATH = "s3://hydroponics-data-project-2026/hydropulse-medallion-lake/landing/iot_raw/"

# =======================================================
# 1. BRONZE LAYER (Keeps Auto Loader streaming active)
# =======================================================
@dlt.table(
    name="bronze_iot",
    comment="Raw streaming IoT telemetry stored in S3"
)
def bronze_iot():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(S3_LANDING_PATH)
        .withColumn("bronze_processing_time", current_timestamp())
    )

# ==============================================================================
# 2. SILVER & QUARANTINE LAYER (Upgraded Advanced Agriculture Engine)
# ==============================================================================

# Shared Master Validation Rule: Ensures complete integrity across critical telemetry
sensor_ok = (
    col("id").isNotNull() &
    col("timestamp").isNotNull() &
    col("pH").cast(DoubleType()).between(0.0, 14.0) &
    col("TDS").cast(DoubleType()).between(0.0, 5000.0) &
    col("DHT_temp").cast(DoubleType()).between(10.0, 50.0) &
    col("DHT_humidity").cast(DoubleType()).between(0.0, 100.0) &
    trim(lower(col("pH_reducer"))).isin("on", "off") &
    trim(lower(col("add_water"))).isin("on", "off") &
    trim(lower(col("nutrients_adder"))).isin("on", "off") &
    trim(lower(col("humidifier"))).isin("on", "off") &
    trim(lower(col("ex_fan"))).isin("on", "off")
)

@dlt.view(
    name="silver_iot",
    comment="Transient advanced engineering layer: Standardizes, grades status, profiles stress, and scores crop health."
)
def silver_iot():
    # Read incremental batch from Bronze
    raw_df = dlt.read("bronze_iot")
    
    # 1. Filter out bad rows (routed cleanly to Quarantine instead), parse core data and rename columns
    base_df = raw_df.filter(sensor_ok).select(
        col("id").cast(IntegerType()),
        to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss").alias("event_timestamp"),
        to_timestamp(col("ingest_timestamp"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("producer_timestamp"),
        col("pH").cast(DoubleType()),
        col("TDS").cast(DoubleType()),
        col("water_level").cast(DoubleType()),
        col("DHT_temp").cast(DoubleType()).alias("ambient_temp"),
        col("DHT_humidity").cast(DoubleType()).alias("ambient_humidity"),
        col("water_temp").cast(DoubleType()),
        
        # Actuator Standardized Binarization (1 = Active, 0 = Standby)
        when(trim(lower(col("pH_reducer"))) == "on", 1).otherwise(0).alias("pH_reducer"),
        when(trim(lower(col("add_water"))) == "on", 1).otherwise(0).alias("add_water"),
        when(trim(lower(col("nutrients_adder"))) == "on", 1).otherwise(0).alias("nutrients_adder"),
        when(trim(lower(col("humidifier"))) == "on", 1).otherwise(0).alias("humidifier"),
        when(trim(lower(col("ex_fan"))) == "on", 1).otherwise(0).alias("ex_fan"),
        col("bronze_processing_time")
    )
    
    # 2. Dynamic Threshold Status Grading (Optimal -> Warning -> Critical)
    graded_df = base_df \
        .withColumn("pH_status", 
            when(col("pH").between(5.5, 6.5), "OPTIMAL")
            .when(col("pH").between(5.0, 7.0), "WARNING")
            .otherwise("CRITICAL")) \
        .withColumn("TDS_status", 
            when(col("TDS").between(800.0, 1500.0), "OPTIMAL")
            .when(col("TDS").between(500.0, 2000.0), "WARNING")
            .otherwise("CRITICAL")) \
        .withColumn("DHT_temp_status", 
            when(col("ambient_temp").between(20.0, 26.0), "OPTIMAL")
            .when(col("ambient_temp").between(18.0, 30.0), "WARNING")
            .otherwise("CRITICAL")) \
        .withColumn("DHT_humidity_status", 
            when(col("ambient_humidity").between(60.0, 80.0), "OPTIMAL")
            .when(col("ambient_humidity").between(50.0, 90.0), "WARNING")
            .otherwise("CRITICAL")) \
        .withColumn("water_temp_status", 
            when(col("water_temp").between(18.0, 24.0), "OPTIMAL")
            .when(col("water_temp").between(15.0, 28.0), "WARNING")
            .otherwise("CRITICAL"))
            
    # 3. Component Health Accumulator Matrix (20 points per optimal variable)
    scored_df = graded_df.withColumn(
        "health_score",
        when(col("pH_status") == "OPTIMAL", lit(20)).otherwise(lit(0)) +
        when(col("TDS_status") == "OPTIMAL", lit(20)).otherwise(lit(0)) +
        when(col("DHT_temp_status") == "OPTIMAL", lit(20)).otherwise(lit(0)) +
        when(col("DHT_humidity_status") == "OPTIMAL", lit(20)).otherwise(lit(0)) +
        when(col("water_temp_status") == "OPTIMAL", lit(20)).otherwise(lit(0))
    )
    
    # 4. Stress Factor Analysis & Deductive Row Health Calculation
    final_silver_df = scored_df \
        .withColumn("pH_stress", when(~col("pH").between(5.5, 6.5), lit(1)).otherwise(lit(0))) \
        .withColumn("TDS_stress", when(~col("TDS").between(800.0, 1500.0), lit(1)).otherwise(lit(0))) \
        .withColumn("temp_stress", when(~col("water_temp").between(18.0, 24.0), lit(1)).otherwise(lit(0))) \
        .withColumn("row_health", 
            when((lit(100) - (col("pH_stress") * 30) - (col("TDS_stress") * 30) - (col("temp_stress") * 30)) < 0, lit(0))
            .otherwise((lit(100) - (col("pH_stress") * 30) - (col("TDS_stress") * 30) - (col("temp_stress") * 30)).cast(IntegerType()))) \
        .withColumn("silver_ts", current_timestamp().cast(StringType())) \
        .withColumn("layer_flag", lit("VALIDATED"))
        
    return final_silver_df


@dlt.table(
    name="quarantine_iot",
    comment="Physical storage table capturing and auditing corrupted or misaligned IoT frames for engineering review."
)
def quarantine_iot():
    # Capture the exact opposite of valid criteria and save permanently to S3
    return (
        dlt.read("bronze_iot")
        .filter(~sensor_ok)
        .withColumn("quarantine_ts", current_timestamp().cast(StringType()))
        .withColumn("layer_flag", lit("REJECTED"))
    )


from pyspark.sql.functions import col, date_trunc, avg, round, when, lit, concat, current_timestamp
from pyspark.sql.functions import sum as spark_sum, count

# ==============================================================================
# 3. GOLD LAYER (Advanced Analytical KPI Engine & Alarm Router)
# ==============================================================================

# ── Gold Table 1: Hourly Sensor KPIs ──────────────────────────────────────────
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


# ── Gold Table 2: Farm-Level Metrics ──────────────────────────────────────────
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


# ── Gold Table 3: Real-time Critical Alerts ────────────────────────────────────
@dlt.table(
    name="gold_alerts",
    comment="Filtered stream routing threshold anomalies (WARNING & CRITICAL) requiring intervention."
)
def gold_alerts():
    return (
        dlt.read("silver_iot")
        # FIX 1: Allow rows to pass through if status is EITHER CRITICAL or WARNING
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
            
            # FIX 2: Dynamic Alert Message Compiler handles both severities cleanly
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
