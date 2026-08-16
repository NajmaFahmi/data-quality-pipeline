# FILE      : data/inject_history.py
# LIBRARY   : pandas, numpy, google-cloud-bigquery
# CONFIG    : BQ_PROJECT, BQ_DATASET, HISTORY_TABLE, PIPELINE_NAME — via parameters
# __main__  : present — standalone script for injecting dummy quality history
# CALLER    : standalone (run directly to populate BigQuery quality_history)

import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from google.cloud import bigquery


def inject_quality_history(
    bq_project: str,
    bq_dataset: str,
    history_table: str,
    pipeline_name: str,
    baseline_days: int = 14,
    degrading_days: int = 7,
    start_date: datetime = datetime(2026, 7, 25, 9, 0, tzinfo=timezone.utc),
) -> None:
    """
    Inject synthetic quality history into BigQuery for trend analysis testing.
    Generates a healthy baseline period followed by a degrading trend period.

    Args:
        bq_project: BigQuery project ID.
        bq_dataset: BigQuery dataset name.
        history_table: BigQuery table name for quality history.
        pipeline_name: Pipeline name to tag records with.
        baseline_days: Number of healthy baseline days to generate.
        degrading_days: Number of degrading trend days to generate.
        start_date: Start date for the synthetic history.
    """
    client = bigquery.Client(project=bq_project)
    table_ref = f"{bq_project}.{bq_dataset}.{history_table}"
    records = []

    # healthy baseline records
    for i in range(baseline_days):
        score = round(np.random.uniform(92, 96), 2)
        records.append({
            "pipeline_name":       pipeline_name,
            "scored_at":           (start_date + timedelta(days=i)).isoformat(),
            "total_score":         score,
            "status":              "PASS",
            "hard_block_triggered": "",
            "score_completeness":  round(np.random.uniform(0.97, 0.99), 4),
            "score_validity":      round(np.random.uniform(0.97, 0.99), 4),
            "score_accuracy":      round(np.random.uniform(0.97, 0.99), 4),
            "score_consistency":   round(np.random.uniform(0.97, 0.99), 4),
            "score_uniqueness":    round(np.random.uniform(0.97, 0.99), 4),
            "score_timeliness":    0.0,
        })

    # degrading trend records
    for i in range(degrading_days):
        score = round(96 - (i * 3.5), 2)
        records.append({
            "pipeline_name":       pipeline_name,
            "scored_at":           (start_date + timedelta(days=baseline_days + i)).isoformat(),
            "total_score":         score,
            "status":              "PASS" if score >= 80 else "WARN",
            "hard_block_triggered": "",
            "score_completeness":  round(0.99 - (i * 0.02), 4),
            "score_validity":      round(0.99 - (i * 0.01), 4),
            "score_accuracy":      round(0.99 - (i * 0.03), 4),
            "score_consistency":   round(0.99 - (i * 0.01), 4),
            "score_uniqueness":    round(0.99 - (i * 0.01), 4),
            "score_timeliness":    0.0,
        })

    df = pd.DataFrame(records)
    print(f"Injecting {len(df)} records into {table_ref}...")
    print(df[["scored_at", "total_score", "status"]].to_string())

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"\nDone — {len(df)} records injected to {table_ref}")


if __name__ == "__main__":
    inject_quality_history(
        bq_project="najma-de-learning",
        bq_dataset="retailco_raw",
        history_table="quality_history",
        pipeline_name="orders_pipeline",
    )