# Retail Orders Data Quality Pipeline

A production-grade data quality pipeline built on Google Cloud Platform, implementing a full medallion architecture (Bronze → Silver → Gold) with five quality gates, Apache Spark transformation, automated quality history tracking, and two Airflow monitoring DAGs.

---

## Architecture

```
Source (orders.csv)
    │
    ▼
┌─────────────────────┐
│   Bronze Layer      │  Raw, unmodified data
│   GCS               │  gs://retailco-raw-najma/raw/orders/
└─────────────────────┘
    │
    ▼ Gate 1: Great Expectations
      Schema validation · Type checking · Null detection
    │
    ▼ Gate 2: Anomaly Detection
      Z-score · IQR · Quarantine
    │
    ▼ Gate 3: Quality Scorer
      6 dimensions · Weighted score · PASS / WARN / FAIL
    │
    ▼
┌─────────────────────┐
│  Spark Transform    │  Clean · Normalize · Deduplicate
└─────────────────────┘
    │
    ▼
┌─────────────────────┐
│   Silver Layer      │  Transformed, analytics-ready
│   GCS (Parquet)     │  gs://retailco-raw-najma/processed/orders/
└─────────────────────┘
    │
    ▼ Gate 4: Great Expectations (post-transform)
      Validates Spark output — catches transform bugs
    │
    ▼
┌─────────────────────┐
│   Gold Layer        │  Final, query-ready data
│   BigQuery          │  retailco_raw.orders_gold
└─────────────────────┘
    │
    ▼ Gate 5: Quality History
      Appends score to retailco_raw.quality_history
```

---

## Monitoring DAGs (Airflow)

Two independent Airflow DAGs run alongside the main pipeline:

```
┌──────────────────────────────────────────────────┐
│  DAG 1 — Freshness Monitor (every 12 hours)      │
│                                                  │
│  check_latest_ts → evaluate_sla → dispatch_alert │
│                                                  │
│  Reads: GCS Bronze blob.updated timestamp        │
│  OK     → log only                               │
│  WARN   → Slack alert (age ≥ 24h)               │
│  CRITICAL → Slack + PagerDuty log (age ≥ 48h)   │
└──────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────┐
│  DAG 2 — Trend Analyzer (daily 09:00)            │
│                                                  │
│  fetch_history → analyze_trend → report          │
│                                                  │
│  Reads: BigQuery retailco_raw.quality_history    │
│  Stable    → log only                            │
│  Degrading → Slack alert                         │
└──────────────────────────────────────────────────┘
```

Both DAGs are independent from the main pipeline — they monitor and alert, never trigger or modify data.

---

## Quality Gates

| Gate | Tool | Checks | On Fail |
|------|------|--------|---------|
| Gate 1 | Great Expectations | Schema, types, nulls | Schema/types → STOP · Nulls → WARN |
| Gate 2 | Custom anomaly detector | Z-score, IQR per column | Quarantine rate > 20% → STOP |
| Gate 3 | Custom quality scorer | 6 dimensions, weighted score | FAIL → STOP · WARN → continue |
| Gate 4 | Great Expectations | Post-transform validation | Schema/types → STOP · Nulls → WARN |
| Gate 5 | BigQuery | Quality history tracking | Appends result, never blocks |

### Quality Dimensions (Gate 3)

| Dimension | Weight | Hard Block |
|-----------|--------|------------|
| Completeness | 30% | < 80% → STOP |
| Validity | 20% | — |
| Accuracy | 20% | < 70% → STOP |
| Consistency | 15% | — |
| Uniqueness | 10% | < 95% → STOP |
| Timeliness | 5% | — |

---

## Project Structure

```
mini_project/
├── data/
│   ├── data_generator.py        # Generates synthetic orders dataset
│   └── inject_history.py        # Injects dummy quality history to BigQuery
├── src/
│   ├── anomaly_detector.py      # Z-score + IQR anomaly detection
│   ├── quality_scorer.py        # 6-dimension quality scoring engine
│   ├── freshness_monitor.py     # FreshnessSLA, FreshnessChecker, AlertDispatcher
│   └── trend_analyzer.py        # Rolling average + Z-score trend analysis
├── gates/
│   ├── gate1_validator.py       # GE validation (Bronze)
│   ├── gate2_anomaly.py         # Anomaly detection gate
│   └── gate3_scorer.py          # Quality scoring gate
├── jobs/
│   ├── gcs_uploader.py          # Upload local file to GCS Bronze
│   ├── spark_transform.py       # Spark: clean, normalize, deduplicate
│   └── bq_loader.py             # Gate 4 + load to Gold + Gate 5
├── dags/
│   ├── dag_freshness_monitor.py # Airflow DAG: freshness check every 12h
│   └── dag_trend_analyzer.py    # Airflow DAG: quality trend analysis daily
├── run_pipeline.py              # Orchestrator — pipeline entry point
├── requirements.txt             # Python dependencies
├── requirements_notes.txt       # External dependencies (JAR, credentials)
└── .gitignore
```

**Architecture principles:**
- `src/`, `gates/`, and `jobs/` are pure libraries — zero dataset-specific config
- All pipeline config (column names, thresholds, GCS paths, BQ tables) lives in `run_pipeline.py`
- All DAG config (bucket names, SLA hours, webhook URLs) lives at the top of each DAG file
- No `__main__` blocks in library files

---

## Tech Stack

| Layer | Tool |
|-------|------|
| Cloud Storage | Google Cloud Storage (GCS) |
| Data Warehouse | Google BigQuery |
| Batch Transform | Apache Spark (PySpark 3.5.6) |
| Schema Validation | Great Expectations 1.20 |
| Anomaly Detection | Custom Z-score + IQR detector |
| Quality Scoring | Custom 6-dimension scorer |
| Orchestration | Apache Airflow 2.10.4 |
| Alerting | Slack Incoming Webhooks |
| Language | Python 3.11 |

---

## Prerequisites

- Python 3.11
- Apache Airflow 2.10.4
- Google Cloud SDK (`gcloud`) authenticated
- GCP service account key with Storage and BigQuery permissions
- GCS connector JAR for Spark (`gcs-connector-hadoop3`)
- Slack workspace with Incoming Webhook configured

---

## Setup

**1. Clone the repository**
```bash
git clone https://github.com/NajmaFahmi/data-quality-pipeline.git
cd data-quality-pipeline/bulan_4/week_2/mini_project
```

**2. Create virtual environment**
```bash
python3.11 -m venv venv_dq
source venv_dq/bin/activate
pip install -r requirements.txt
```

**3. Set environment variables**
```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account-key.json
```

**4. Download GCS connector JAR**
```bash
mkdir -p jars
curl -L "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar" \
  -o jars/gcs-connector-hadoop3-latest.jar
```

**5. Update paths in `run_pipeline.py`**
```python
GCS_CONNECTOR_JAR = os.path.expanduser("~/path/to/gcs-connector.jar")
GCS_KEY_PATH      = os.path.expanduser("~/path/to/service-account-key.json")
BUCKET_NAME       = "your-gcs-bucket"
BQ_PROJECT        = "your-gcp-project"
```

**6. Update DAG configs**

In `dags/dag_freshness_monitor.py`:
```python
BUCKET_NAME       = "your-gcs-bucket"
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/..."
```

In `dags/dag_trend_analyzer.py`:
```python
BQ_PROJECT        = "your-gcp-project"
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/..."
```

**7. Copy DAG files to Airflow**
```bash
cp dags/dag_freshness_monitor.py $AIRFLOW_HOME/dags/
cp dags/dag_trend_analyzer.py $AIRFLOW_HOME/dags/
```

---

## Running the Pipeline

**Generate sample data**
```bash
python data/data_generator.py
```

**Run the full pipeline**
```bash
python run_pipeline.py
```

**Test Airflow DAGs manually**
```bash
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
airflow dags test dag_freshness_monitor 2026-08-16 --subdir $AIRFLOW_HOME/dags
airflow dags test dag_trend_analyzer 2026-08-16 --subdir $AIRFLOW_HOME/dags
```

**Activate DAGs for scheduling**
```bash
airflow dags unpause dag_freshness_monitor
airflow dags unpause dag_trend_analyzer
airflow scheduler
```

---

## Dataset

The sample dataset (`data_generator.py`) generates 207 orders records:

| Type | Count | Description |
|------|-------|-------------|
| Normal records | 200 | Realistic orders within expected ranges |
| Anomalies | 3 | Extreme values (quantity 999, price 99999) |
| Null records | 3 | Missing values in critical columns |
| Duplicates | 1 | Duplicate order_id |

---

## BigQuery Output Tables

| Table | Description |
|-------|-------------|
| `retailco_raw.orders_gold` | Final clean orders data, analytics-ready |
| `retailco_raw.quality_history` | Quality score history per pipeline run |

---

## Key Design Decisions

**Why keep nulls in Gold?**
Null handling (imputation, removal, flagging) is a business decision owned by Data Analysts and Data Scientists. The pipeline flags nulls via Gate 1 and Gate 4 warnings, passes them through, and leaves the decision to downstream teams.

**Why Spark for transformation?**
While pandas could handle 207 rows, Spark establishes production-grade patterns. At scale (millions of rows), Spark distributes processing across nodes — pandas would run out of memory. Building with Spark from the start means the pipeline scales without architectural changes.

**Why separate Gate 4 from Gate 1?**
Gate 1 validates source data quality. Gate 4 validates that Spark did not introduce new problems during transformation. These are two distinct failure modes requiring separate checks.

**Why two separate monitoring DAGs?**
Freshness and trend are orthogonal concerns. A pipeline can have fresh data with degrading quality, or stale data with historically excellent quality. Separating them allows independent scheduling, independent alerting thresholds, and independent failure handling.

**Why quarantine rate threshold in Gate 2?**
A quarantine rate above 20% signals a systemic upstream bug, not individual anomalies. The pipeline stops rather than processing majority-corrupted data into Gold, where it would silently damage downstream analytics.
