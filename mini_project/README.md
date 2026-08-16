# Retail Orders Data Quality Pipeline

A production-grade data quality pipeline built on Google Cloud Platform, implementing a full medallion architecture (Bronze → Silver → Gold) with five quality gates, Apache Spark transformation, and automated quality history tracking.

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
│   └── data_generator.py        # Generates synthetic orders dataset
├── src/
│   ├── anomaly_detector.py      # Z-score + IQR anomaly detection
│   ├── quality_scorer.py        # 6-dimension quality scoring engine
│   └── pipeline_transaction.py  # Pipeline-specific scorer config
├── gates/
│   ├── gate1_validator.py       # GE validation (Bronze)
│   ├── gate2_anomaly.py         # Anomaly detection gate
│   └── gate3_scorer.py          # Quality scoring gate
├── jobs/
│   ├── gcs_uploader.py          # Upload local file to GCS Bronze
│   ├── spark_transform.py       # Spark: clean, normalize, deduplicate
│   └── bq_loader.py             # Gate 4 + load to Gold + Gate 5
├── run_pipeline.py              # Orchestrator — pipeline entry point
├── requirements.txt             # Python dependencies
├── requirements_notes.txt       # External dependencies (JAR, credentials)
└── .gitignore
```

**Architecture principles:**
- `src/` and `gates/` and `jobs/` are pure libraries — zero dataset-specific config
- All pipeline config (column names, thresholds, GCS paths, BQ tables) lives in `run_pipeline.py`
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
| Language | Python 3.11 |

---

## Prerequisites

- Python 3.11
- Google Cloud SDK (`gcloud`) authenticated
- GCP service account key with Storage and BigQuery permissions
- GCS connector JAR for Spark (`gcs-connector-hadoop3`)

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

**Expected output**
```
ORDERS PIPELINE — START
[1/8] Uploading CSV to Bronze (GCS)...
[2/8] Gate 1 — Great Expectations (Bronze)...
[3/8] Gate 2 — Anomaly Detection...
[4/8] Gate 3 — Quality Scorer...
[5/8] Spark Transform — Clean, Normalize, Deduplicate...
[6/8] Gate 4 — Great Expectations (Silver)...
[7/8] Loading Silver → Gold (BigQuery)...
[8/8] Gate 5 — Saving Quality History to BigQuery...
ORDERS PIPELINE — COMPLETE
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

The pipeline is designed to detect and handle all four types — anomalies are quarantined at Gate 2, duplicates are removed by Spark, nulls are flagged with WARN and passed through for downstream teams.

---

## BigQuery Output Tables

| Table | Description |
|-------|-------------|
| `retailco_raw.orders_gold` | Final clean orders data, analytics-ready |
| `retailco_raw.quality_history` | Quality score history per pipeline run |

---

## Key Design Decisions

**Why keep nulls in Gold?**
Null handling (imputation, removal, flagging) is a business decision owned by Data Analysts and Data Scientists — not the pipeline. The DE pipeline flags nulls via Gate 1 and Gate 4 warnings, passes them through, and leaves the decision to downstream teams.

**Why Spark for transformation?**
While pandas could handle 207 rows, Spark is used to establish production-grade patterns. At scale (millions of rows), Spark distributes processing across nodes — pandas would run out of memory. Building with Spark from the start means the pipeline scales without architectural changes.

**Why separate Gate 4 from Gate 1?**
Gate 1 validates source data quality. Gate 4 validates that Spark did not introduce new problems during transformation (e.g., incorrect deduplication removing too many rows, normalization producing unexpected nulls). These are two distinct failure modes requiring separate checks.
