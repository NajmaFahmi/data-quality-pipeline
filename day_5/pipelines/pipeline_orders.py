from datetime import datetime, timezone
from typing import Optional
import pandas as pd
from src.quality_scorer import (
    DimensionConfig,
    QualityScorer,
    QualityScoreResult,
    ScorerConfig,
    measure_accuracy,
    measure_completeness,
    measure_consistency,
    measure_timeliness,
    measure_uniqueness,
    measure_validity,
    print_score_report,
    save_to_history,
)


### 1. Orders pipeline configuration
## Assigning Weights to Every Columns
# completeness weighted highest — missing order data means lost revenue tracking
# accuracy weighted second — wrong quantity or price corrupts financial reports
ORDERS_PIPELINE_CONFIG = ScorerConfig(
    pipeline_name="orders_pipeline",
    dimensions={
        "completeness": DimensionConfig(weight=0.30, hard_block_threshold=0.90),
        "validity":     DimensionConfig(weight=0.10),
        "accuracy":     DimensionConfig(weight=0.25, hard_block_threshold=0.75),
        "consistency":  DimensionConfig(weight=0.10),
        "uniqueness":   DimensionConfig(weight=0.15, hard_block_threshold=0.95),
        "timeliness":   DimensionConfig(weight=0.10),
    },
    pass_threshold=0.80,
    warn_threshold=0.60,
)

VALID_STATUSES = {"pending", "confirmed", "shipped", "delivered"}


### 2. Function to Run Quality Check
# applied only for this specific order pipeline
def run_quality_check(
    df: pd.DataFrame,
    latest_timestamp: Optional[datetime] = None,
) -> QualityScoreResult:
    """
    Run full quality scoring on an orders DataFrame.
    latest_timestamp: most recent ordered_at value — used for timeliness check.
    """
    if latest_timestamp is None:
        latest_timestamp = df["ordered_at"].max()

    dimension_scores = {
        "completeness": measure_completeness(
            df, ["order_id", "customer_id", "product_id", "quantity", "price", "status"],
        ),
        "validity": measure_validity(df, {
            "quantity": "numeric",
            "price":    "numeric",
            "status":   "string",
        }),
        "accuracy": measure_accuracy(df,
            range_rules={
                "quantity": {"min": 1,   "max": 100},
                "price":    {"min": 1,   "max": 10_000_000},
            },
            set_rules={
                "status": VALID_STATUSES,
            },
        ),
        "consistency": measure_consistency(df, [
            lambda row: not (row["quantity"] > 0 and row["price"] <= 0),
            lambda row: row["status"] in VALID_STATUSES
                        if pd.notna(row["status"]) else True,
        ]),
        "uniqueness": measure_uniqueness(df, subset=["order_id"]),
        "timeliness": measure_timeliness(
            latest_timestamp=latest_timestamp,
            sla_hours=6.0,
        ),
    }

    scorer = QualityScorer(ORDERS_PIPELINE_CONFIG)
    result = scorer.score(dimension_scores, row_count=len(df))

    print_score_report(result)
    save_to_history(result)

    return result



### 3. Run Generator Dataframes, Detect Anomalies & Quality Checker
def run_orders_pipeline() -> None:
    """
    Full orders pipeline: generate → anomaly detection → quality scoring.
    """
    from data.data_generator import generate_orders
    from src.anomaly_detector import detect_anomalies

    df_raw = generate_orders(n_rows=100)

    clean_df, quarantine_df = detect_anomalies(
        df_raw,
        numeric_columns=["quantity", "price"],
        method="iqr",
        multiplier=1.5,
    )

    print(f"Rows after anomaly detection: {len(clean_df)} clean, "
          f"{len(quarantine_df)} quarantined")
    print()

    run_quality_check(clean_df)


if __name__ == "__main__":
    run_orders_pipeline()