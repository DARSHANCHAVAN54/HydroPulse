from pyspark.sql.functions import col, max as spark_max, current_timestamp

# Configuration settings for the audit
DATABASE_NAME = "hydropulse_db"
MAX_QUARANTINE_THRESHOLD_PCT = 5.0  # Hard failure if bad rows exceed 5%
MAX_SLA_DELAY_HOURS = 2            # Alert as a WARNING if data is older than 2 hours

print(f"🚀 Starting simplified automated data audit for schema: '{DATABASE_NAME}'")

# ==============================================================================
# AUDIT GATE 1: Volumetric Distribution Check (CRITICAL GATE)
# ==============================================================================
try:
    silver_count = spark.table(f"{DATABASE_NAME}.silver_iot").count()
    quarantine_count = spark.table(f"{DATABASE_NAME}.quarantine_iot").count()
    total_ingested = silver_count + quarantine_count
    
    # If no records exist at all across both tables, stop everything
    if total_ingested == 0:
         raise ValueError("🚨 Audit Critical Failure: Silver and Quarantine tables are completely empty!")
         
    quarantine_percentage = (quarantine_count / total_ingested) * 100
    
    print(f"\n📊 Volumetric Audit Summary:")
    print(f"   - Validated Silver Rows: {silver_count}")
    print(f"   - Quarantined Bad Rows : {quarantine_count}")
    print(f"   - Current Quarantine Rate : {quarantine_percentage:.2f}%")

    # Evaluate Hard Limit
    if quarantine_percentage > MAX_QUARANTINE_THRESHOLD_PCT:
        raise ValueError(
            f"❌ Audit Boundary Failure: Quarantine rate of {quarantine_percentage:.2f}% "
            f"exceeds the maximum limit of {MAX_QUARANTINE_THRESHOLD_PCT}%!"
        )
    print("✅ Audit Gate 1 Passed: Volumetric distribution is within safe boundaries.")

except Exception as e:
    print(f"🚨 Crash in Volumetric Check: {str(e)}")
    raise e

# ==============================================================================
# AUDIT GATE 2: SLA Data Freshness Check (LEARNING-FRIENDLY WARNING GATE)
# ==============================================================================
try:
    # Grab the last time the Gold layer updated
    gold_time_df = spark.table(f"{DATABASE_NAME}.gold_sensor_hourly") \
                        .select(spark_max(col("gold_update_time").cast("timestamp")).alias("latest_update"))
    latest_gold_update = gold_time_df.collect()[0]["latest_update"]
    
    if latest_gold_update is None:
        print("⚠️ Warning: Gold layer update timestamp returned NULL. (Table might be empty)")
    else:
        # Calculate time difference between right now and the last entry
        now_df = spark.range(1).select(current_timestamp().alias("now"))
        current_time = now_df.collect()[0]["now"]
        
        time_delta_seconds = (current_time - latest_gold_update).total_seconds()
        time_delta_hours = time_delta_seconds / 3600.0
        
        print(f"\n⏰ SLA Freshness Audit Summary:")
        print(f"   - Current Reference Time: {current_time}")
        print(f"   - Latest Table Update   : {latest_gold_update}")
        print(f"   - Current Pipeline Delay: {time_delta_hours:.2f} hours")
        
        # Safe Learning Evaluator: Print alert instead of throwing an exception
        if time_delta_hours > MAX_SLA_DELAY_HOURS:
            print(f"⚠️  [ALERT] Downstream Gold tables are technically stale by {time_delta_hours:.2f} hours.")
            print(f"   Because you are in a testing/learning phase, this task will NOT break.")
        else:
            print("✅ Audit Gate 2 Passed: Data pipeline latency satisfies SLA criteria.")

except Exception as e:
    print(f"⚠️ Could not complete SLA freshness audit check: {str(e)}")

# ==============================================================================
# WORKFLOW COMPLETE SUCCESS
# ==============================================================================
print("\n🎉 Operational audit complete. Safe to proceed to dashboard downstream tasks!")
