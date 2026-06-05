-- DATASET 1: HOURLY LOGS PROFILE
SELECT window_start, avg_pH, avg_TDS, avg_water_temp, avg_air_temp, avg_humidity, avg_water_level, pH_stress_rate, TDS_stress_rate, water_pump_duty, nutrient_pump_duty, ph_correction_duty, exhaust_fan_duty, water_usage_cycles, nutrient_usage_cycles, ph_correction_cycles, climate_energy_cycles, humidifier_duty, avg_health_score, optimal_growth_pct, stress_incidents, total_readings
FROM hydropulse_db.gold_sensor_hourly
ORDER BY window_start DESC;

-- DATASET 2: ACTIVE FAULT MONITOR
SELECT alert_timestamp, pH, TDS, pH_status, TDS_status, health_score, COALESCE(pH_alert_msg, 'No pH Issue') AS pH_issue, COALESCE(TDS_alert_msg, 'No TDS Issue') AS TDS_issue
FROM hydropulse_db.gold_alerts
WHERE pH_status != 'OPTIMAL' OR TDS_status != 'OPTIMAL'
ORDER BY alert_timestamp DESC;

-- DATASET 3: MACRO FARM SUMMARY
SELECT window_start, farm_avg_health, total_yield_risk_incidents, total_resource_cycles
FROM hydropulse_db.gold_farm_kpi
ORDER BY window_start DESC;
