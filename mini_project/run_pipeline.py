# FILE      : run_pipeline.py
# ROLE      : orchestrator — entry point for the full pipeline
# CONFIG    : all dataset-specific config lives here
# __main__  : present — this is the pipeline entry point
# CALLS     : jobs/gcs_uploader.py, gates/gate1_validator.py,
#             gates/gate2_anomaly.py, gates/gate3_scorer.py,
#             jobs/spark_transform.py, jobs/bq_loader.py

import sys
import pandas as pd
from datetime import datetime, timezone
from google.cloud import storage
from io import StringIO
import os

from jobs.gcs_uploader import upload_to_bronze
from gates.gate1_validator import run_gate1
from gates.gate2_anomaly import run_gate2
from gates.gate3_scorer import run_gate3
from jobs.spark_transform import run_spark_transform
from jobs.bq_loader import read_parquet_from_gcs, run_gate4, load_to_gold, save_quality_history
from src.quality_scorer import (
    ScorerConfig,
    DimensionConfig,
    measure_completeness,
    measure_validity,
    measure_accuracy,
    measure_consistency,
    measure_uniqueness,
    measure_timeliness,
)


### =================== CONFIGURATION ===================

# ─────────────────────────────────────────────
# INFRASTRUCTURE CONFIG
# ─────────────────────────────────────────────
LOCAL_CSV_PATH = "data/orders.csv"
CONTRACT_PATH = "contracts/orders_contract.yaml"

BUCKET_NAME = "retailco-raw-najma"
BQ_PROJECT = "najma-de-learning"
BQ_DATASET = "retailco_raw"

BRONZE_BLOB = "raw/orders/orders.csv"
SILVER_GCS_PATH = f"gs://{BUCKET_NAME}/processed/orders/"
GOLD_TABLE = "orders_gold"
HISTORY_TABLE = "quality_history"

GCS_CONNECTOR_JAR = os.path.expanduser(
    "~/de_learning/bulan_3/retailco-modern-data-stack/jars/gcs-connector.jar"
)
GCS_KEY_PATH = os.path.expanduser(
    "~/de_learning/bulan_3/keys/najma-de-learning-key.json"
)


# ─────────────────────────────────────────────
# GATE 1 + GATE 4 CONFIG (Great Expectations)
# ─────────────────────────────────────────────
REQUIRED_COLUMNS = [
    "order_id", "customer_id", "product_id",
    "quantity", "unit_price", "total_amount",
    "order_date", "status", "customer_email",
]

EXPECTED_TYPES = {
    "quantity":     "numeric",
    "unit_price":   "numeric",
    "total_amount": "numeric",
    "order_id":     "string",
    "status":       "string",
}

NON_NULLABLE_COLUMNS = [
    "order_id", "quantity", "unit_price",
    "total_amount", "order_date", "status",
]


# ─────────────────────────────────────────────
# GATE 2 CONFIG (Anomaly Detection)
# ─────────────────────────────────────────────
ANOMALY_NUMERIC_COLUMNS         = ["quantity", "unit_price", "total_amount"]
ANOMALY_METHOD                  = "iqr"
ANOMALY_MULTIPLIER              = 1.5
ANOMALY_ZSCORE_THRESHOLD        = 3.0
ANOMALY_QUARANTINE_THRESHOLD    = 0.2


# ─────────────────────────────────────────────
# GATE 3 CONFIG (Quality Scorer)
# ─────────────────────────────────────────────
ORDERS_SCORER_CONFIG = ScorerConfig(
    pipeline_name="orders_pipeline",
    dimensions={
        "completeness": DimensionConfig(weight=0.30, hard_block_threshold=0.80),
        "validity":     DimensionConfig(weight=0.20),
        "accuracy":     DimensionConfig(weight=0.20, hard_block_threshold=0.70),
        "consistency":  DimensionConfig(weight=0.15),
        "uniqueness":   DimensionConfig(weight=0.10, hard_block_threshold=0.95),
        "timeliness":   DimensionConfig(weight=0.05),
    },
    pass_threshold=0.80,
    warn_threshold=0.60,
)


# ─────────────────────────────────────────────
# SPARK CONFIG
# ─────────────────────────────────────────────
SPARK_APP_NAME      = "orders_pipeline_transform"
DEDUP_COLUMNS       = ["order_id"]
DATE_COLUMNS        = ["order_date"]





### =================== PIPELINE ===================

def run_pipeline() -> None:
    from src.contract_loader import ContractLoader

    contract = ContractLoader(CONTRACT_PATH).load()
    print(f"[INFO] Loaded contract: {contract.name} v{contract.version}")
    print(f"[INFO] Owner: {contract.owner}")
    print(f"[INFO] Schema columns: {[col.name for col in contract.schema]}")

    print("=" * 55)
    print("ORDERS PIPELINE — START")
    print(f"Run at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 55)


    # ── UPLOAD LOCAL DATA TO BRONZE ─────────────────
    print("\n[1/8] Uploading CSV to Bronze (GCS)...")

    upload_to_bronze(
        local_path=LOCAL_CSV_PATH,
        bucket_name=BUCKET_NAME,
        destination_blob=BRONZE_BLOB,
    )


    # ── GATE 1 ──────────────────────────────────────
    print("\n[2/8] Gate 1 — Great Expectations (Bronze)...")

    gate1_result = run_gate1(
        bucket_name=BUCKET_NAME,
        blob_path=BRONZE_BLOB,
        required_columns=[col.name for col in contract.schema],
        expected_types=EXPECTED_TYPES,
        non_nullable_columns=[col.name for col in contract.schema if not col.nullable],
        suite_name="orders_gate1_suite",
        datasource_name="orders_gate1_source",
        asset_name="orders_gate1_asset",
        batch_name="orders_gate1_batch",
        validation_name="orders_gate1_validation",
    )

    # schema & type not passed --> failed and stop
    # null not passed --> just warn
    if not gate1_result["schema_passed"]:
        print("Pipeline stopped at Gate 1 — schema failure.")
        sys.exit(1)
    if not gate1_result["types_passed"]:
        print("Pipeline stopped at Gate 1 — type failure.")
        sys.exit(1)
    if not gate1_result["nulls_passed"]:
        print("Gate 1 [WARN] — null violations detected, pipeline continues.")


    # ── GATE 2 ──────────────────────────────────────
    print("\n[3/8] Gate 2 — Anomaly Detection...")

    ## read data from Bronze Layer
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(BRONZE_BLOB)
    df = pd.read_csv(StringIO(blob.download_as_text()))

    clean_df, quarantine_df, gate2_result = run_gate2(
        df=df,
        numeric_columns=ANOMALY_NUMERIC_COLUMNS,
        method=ANOMALY_METHOD,
        multiplier=ANOMALY_MULTIPLIER,
        zscore_threshold=ANOMALY_ZSCORE_THRESHOLD,
        quarantine_rate_threshold=ANOMALY_QUARANTINE_THRESHOLD,
    )

    if not gate2_result["passed"]:
        print("Pipeline stopped at Gate 2.")
        sys.exit(1)


    # ── GATE 3 ──────────────────────────────────────
    print("\n[4/8] Gate 3 — Quality Scorer...")

    dimension_scores = {
        "completeness": measure_completeness(
            clean_df,
            columns=["order_id", "customer_id", "product_id",
                     "quantity", "unit_price", "total_amount",
                     "order_date", "status", "customer_email"],
        ),
        "validity": measure_validity(
            clean_df,
            type_rules={
                "quantity":     "numeric",
                "unit_price":   "numeric",
                "total_amount": "numeric",
                "order_id":     "string",
                "status":       "string",
                "customer_email": "email",
            },
        ),
        "accuracy": measure_accuracy(
            clean_df,
            range_rules={
                "quantity":     {"min": 1, "max": 100},
                "unit_price":   {"min": 1, "max": 10000},
                "total_amount": {"min": 1, "max": 1_000_000},
            },
            set_rules={
                "status": {"completed", "pending", "cancelled"},
            },
        ),
        "consistency": measure_consistency(
            clean_df,
            rules=[
                lambda row: abs(row["total_amount"] - row["quantity"] * row["unit_price"]) < 0.01
            ],
        ),
        "uniqueness": measure_uniqueness(
            clean_df,
            subset=["order_id"],
        ),
        "timeliness": measure_timeliness(
            latest_timestamp=pd.to_datetime(clean_df["order_date"]).max(),
            sla_hours=contract.sla.freshness_hours,
        ),
    }

    score_result, gate3_result = run_gate3(
        df=clean_df,
        scorer_config=ORDERS_SCORER_CONFIG,
        dimension_scores=dimension_scores,
    )

    if not gate3_result["passed"]:
        print("Pipeline stopped at Gate 3.")
        sys.exit(1)


    # ── SPARK TRANSFORM ─────────────────────────────
    ## transform result saved in Silver Layer
    print("\n[5/8] Spark Transform — Clean, Normalize, Deduplicate...")

    spark_result = run_spark_transform(
        input_df=clean_df,
        output_gcs_path=SILVER_GCS_PATH,
        deduplicate_columns=DEDUP_COLUMNS,
        date_columns=DATE_COLUMNS,
        app_name=SPARK_APP_NAME,
        gcs_connector_jar=GCS_CONNECTOR_JAR,
        gcs_key_path=GCS_KEY_PATH,
    )

    if not spark_result["passed"]:
        print("Pipeline stopped at Spark Transform.")
        sys.exit(1)


    # ── GATE 4 ──────────────────────────────────────
    print("\n[6/8] Gate 4 — Great Expectations (Silver)...")

    silver_df = read_parquet_from_gcs(SILVER_GCS_PATH)

    gate4_result = run_gate4(
        df=silver_df,
        required_columns=[col.name for col in contract.schema],
        expected_types=EXPECTED_TYPES,
        non_nullable_columns=[col.name for col in contract.schema if not col.nullable],
        suite_name="orders_gate4_suite",
        datasource_name="orders_gate4_source",
        asset_name="orders_gate4_asset",
        batch_name="orders_gate4_batch",
        validation_name="orders_gate4_validation",
    )

    if not gate4_result["schema_passed"]:
        print("Pipeline stopped at Gate 4 — schema failure.")
        sys.exit(1)
    if not gate4_result["types_passed"]:
        print("Pipeline stopped at Gate 4 — type failure.")
        sys.exit(1)
    if not gate4_result["nulls_passed"]:
        print("Gate 4 [WARN] — null values detected in Silver, DA/DS to handle downstream.")


    # ── LOAD DATA TO GOLD ───────────────────────────
    print("\n[7/8] Loading Silver → Gold (BigQuery)...")

    load_to_gold(
        silver_gcs_path=SILVER_GCS_PATH,
        bq_project=BQ_PROJECT,
        bq_dataset=BQ_DATASET,
        gold_table=GOLD_TABLE,
    )


    # ── GATE 5: SAVE QUALITY HISTORY ───────────────
    print("\n[8/8] Gate 5 — Saving Quality History to BigQuery...")
    save_quality_history(
        score_result=score_result,
        bq_project=BQ_PROJECT,
        bq_dataset=BQ_DATASET,
        history_table=HISTORY_TABLE,
    )



    print("\n" + "=" * 55)
    print("ORDERS PIPELINE — COMPLETE")
    print("=" * 55)



if __name__ == "__main__":
    run_pipeline()