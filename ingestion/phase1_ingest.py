import os
import json
import time
import pandas as pd
import boto3
from datetime import datetime

# Initialize the S3 client (picks up credentials from 'aws configure')
s3_client = boto3.client('s3')

# Configuration
BUCKET_NAME = "hydroponics-data-project-2026"  # Your S3 bucket name
CSV_FILE = "IoTDataRaw.csv"
BATCH_SIZE = 10  # Grouping 10 records per 10 seconds

# 1. Read the raw data exactly as text strings row-by-row
df = pd.read_csv(CSV_FILE, dtype=str)
df.columns = [c.strip() for c in df.columns]

print(f"🚀 Starting Phase 1 Ingestion. Total rows to process: {len(df)}")

# 2. Iterate through data sequentially in chunks of 10 rows
for batch_start in range(0, len(df), BATCH_SIZE):
    batch = df.iloc[batch_start : batch_start + BATCH_SIZE]
    
    # Store rows as valid newline-delimited JSON (JSON Lines format)
    json_lines_str = ""
    
    for _, row in batch.iterrows():
        # Build raw record payload
        record = {}
        for column_name, value in row.items():
            record[column_name] = None if pd.isna(value) else str(value).strip()
            
        # 💡 UPDATED: Dynamically generate 'ingest_timestamp' using the CSV's own time
        # This prevents DLT Silver Layer from getting NULL values while keeping original time.
        csv_ts = record.get("timestamp")
        if csv_ts:
            # If the CSV timestamp lacks microseconds, pad it so DLT's HH:mm:ss.SSSSSS doesn't fail
            if "." not in csv_ts:
                record["ingest_timestamp"] = f"{csv_ts}.000000"
            else:
                record["ingest_timestamp"] = csv_ts
        else:
            record["ingest_timestamp"] = None
        
        # Append record to current batch string with a newline breaker
        json_lines_str += json.dumps(record) + "\n"
        
    # 3. Define a unique file name using the current time (prevents S3 file overwriting)
    file_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    s3_key = f"hydropulse-medallion-lake/landing/iot_raw/telemetry_{file_id}.json"
    
    # 4. Stream payload directly into S3
    try:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json_lines_str.encode('utf-8')
        )
        print(f"✅ Successfully uploaded {len(batch)} sequential records to s3://{BUCKET_NAME}/{s3_key}")
    except Exception as e:
        print(f"❌ Failed to upload batch to S3: {e}")
        
    # Sleep 10 seconds before generating the next batch file
    print("⏳ Sleeping for 10 seconds...")
    time.sleep(10)
