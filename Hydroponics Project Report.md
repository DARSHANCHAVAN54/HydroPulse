---

## Comprehensive Project Report

### 1. Project Context & Agricultural Value
Maintaining precise water quality parameters is a fundamental challenge in industrial hydroponics. Minor variations in pH balance can lock root systems, stopping nutrient absorption and causing entire crop lines to fail within hours. Similarly, tracking Total Dissolved Solids (TDS) is vital to ensure optimal fertilization levels without damaging vegetation. 

This project addresses these challenges by building an event-driven data platform that automatically processes streaming telemetry, tracks machinery usage rates, filters sensor errors, and sends instant critical environmental alerts.

### 2. Operational Ingestion Layer (Local Client Pipeline)
Data collection begins at your local environment layer, using an automated streaming component that processes an initial 50,571-row Kaggle dataset. To emulate physical hardware configurations, the system splits records into 10-row micro-batches sent every 10 seconds. 

To prevent timestamp casting errors down the line, the client engine validates incoming strings before they reach cloud storage. If a string lacks microsecond information, the script appends a `.000000` suffix to ensure it maps correctly to standard Spark timestamp formats.

### 3. Lakehouse Medallion Architecture (DLT Platform)
Processing is distributed across three clear architectural steps using Delta Live Tables:

* **Bronze Stage (Raw Intake):** Collects incoming files exactly as they are written using Databricks **Auto Loader (`cloudFiles`)**. Instead of manually parsing directories, Auto Loader subscribes directly to an AWS SQS queue generated via CloudFormation. This event-driven pattern triggers file imports instantly when data lands in S3.
* **Silver Stage (Quality Engineering & Calculations):** Telemetry streams go through deep schema filtering (`sensor_ok`). Records with corrupt IDs or invalid ranges are cleanly routed to `quarantine_iot`. Valid data is enriched with calculated operational metrics:
  * *Cumulative Health Score:* Assigns up to 20 points for each variable operating inside optimal agricultural ranges (such as a pH between 5.5 and 6.5).
  * *Stress Impact Tracking:* If a parameter hits warning limits, the system applies heavy deductive penalties (-30 points per violation) to generate a dynamic safety health indicator.
* **Gold Stage (Business Intelligence Views):** Aggregates raw metrics into specialized analysis structures:
  * `gold_sensor_hourly`: Tracks system performance metrics, including pump duty cycles (the percentage of active run-time per hour).
  * `gold_alerts`: Filters out optimal logs to build dedicated risk alerts using clear text string concatenations.

### 4. Data Governance & Workflow Orchestration
Data quality is enforced through an automated multi-stage Databricks Workflows DAG. After the Delta Live Tables processing finishes, an automated evaluation notebook verifies two main metrics before updating production views:

1. **Volumetric Data Quality Limit:** Computes structural error ratios across your current intake pipeline:
$$\text{Quarantine Rate} = \left( \frac{\text{Quarantine Count}}{\text{Silver Count} + \text{Quarantine Count}} \right) \times 100$$
If corrupt files make up more than $5\%$ of your total ingestion volume, the audit triggers a hard failure to protect downstream operational views from skewed statistics.
2. **SLA Delivery Verification:** Evaluates data freshness by cross-referencing system clock values against the newest entries inside your gold layer tables. If latency drops below a 2-hour window, warning notifications trigger automatically.

The entire architecture is event-driven. The moment your local script writes data to S3, your file arrival sensors trigger the Databricks workflow. This automatically processes the data, runs quality checks, and updates your operations dashboard without requiring manual oversight.
