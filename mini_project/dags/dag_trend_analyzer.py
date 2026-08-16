# FILE      : dags/dag_trend_analyzer.py
# LIBRARY   : apache-airflow, google-cloud-bigquery, src/trend_analyzer.py
# CONFIG    : BQ_PROJECT, BQ_DATASET, HISTORY_TABLE, PIPELINE_NAME,
#             SLACK_WEBHOOK_URL — di atas file
# __main__  : none
# CALLER    : Airflow scheduler — daily 09:00


import sys
import os
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.expanduser(
    "~/de_learning/bulan_4/week_2/mini_project"
))

from src.trend_analyzer import (
    compute_rolling_avg,
    detect_score_anomaly,
    print_trend_report,
)



# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
BQ_PROJECT        = "najma-de-learning"
BQ_DATASET        = "retailco_raw"
HISTORY_TABLE     = "quality_history"
PIPELINE_NAME     = "orders_pipeline"
ROLLING_WINDOW    = 7
BASELINE_DAYS     = 14
Z_THRESHOLD       = 2.0
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
MIN_RECORDS       = 7



# ─────────────────────────────────────────────
# TASK FUNCTIONS (3 TASKS)
# ─────────────────────────────────────────────
def fetch_history(**context) -> None:
    """
    Task 1: Read quality_history from BigQuery.
    Pushes serialized DataFrame as JSON XCom for next task.
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=BQ_PROJECT)
    query = f"""
        SELECT *
        FROM `{BQ_PROJECT}.{BQ_DATASET}.{HISTORY_TABLE}`
        WHERE pipeline_name = '{PIPELINE_NAME}'
        ORDER BY scored_at ASC
    """
    df = client.query(query).to_dataframe()
    print(f"[fetch_history] Loaded {len(df)} records from BigQuery")

    if len(df) < MIN_RECORDS:
        print(f"[fetch_history] Not enough records: {len(df)} < {MIN_RECORDS}. Skipping analysis.")
        context["ti"].xcom_push(key="df_json", value=None)
        context["ti"].xcom_push(key="enough_data", value=False)
        return

    context["ti"].xcom_push(key="df_json", value=df.to_json())
    context["ti"].xcom_push(key="enough_data", value=True)


def analyze_trend(**context) -> None:
    """
    Task 2: Run rolling average and Z-score anomaly detection.
    Pushes trend direction and anomaly count as XCom.
    """
    import pandas as pd
    from io import StringIO

    enough_data = context["ti"].xcom_pull(
        task_ids="fetch_history", key="enough_data"
    )

    if not enough_data:
        print("[analyze_trend] Skipping — not enough data.")
        context["ti"].xcom_push(key="trend_direction", value="insufficient_data")
        context["ti"].xcom_push(key="anomaly_count", value=0)
        context["ti"].xcom_push(key="trend_delta", value=0.0)
        return

    df_json = context["ti"].xcom_pull(task_ids="fetch_history", key="df_json")
    df = pd.read_json(StringIO(df_json))
    df["scored_at"] = pd.to_datetime(df["scored_at"], utc=True)
    df = df.sort_values("scored_at").reset_index(drop=True)

    ## Rolling Average
    df = compute_rolling_avg(df, window=ROLLING_WINDOW)
    ## Z-score Anomaly Detection
    df, mean_bl, std_bl = detect_score_anomaly(
        df,
        baseline_days=BASELINE_DAYS,
        z_threshold=Z_THRESHOLD,
    )

    print_trend_report(df)

    first_rolling = df["rolling_avg"].iloc[min(6, len(df) - 1)]
    last_rolling  = df["rolling_avg"].iloc[-1]
    trend_delta   = last_rolling - first_rolling
    trend_direction = "improving" if trend_delta > 0 else "degrading"
    anomaly_count = int(df["is_anomaly"].sum())

    print(f"[analyze_trend] Trend: {trend_direction} ({trend_delta:+.1f} pts)")
    print(f"[analyze_trend] Anomalous days: {anomaly_count}")

    context["ti"].xcom_push(key="trend_direction", value=trend_direction)
    context["ti"].xcom_push(key="anomaly_count", value=anomaly_count)
    context["ti"].xcom_push(key="trend_delta", value=round(float(trend_delta), 2))
    context["ti"].xcom_push(key="mean_baseline", value=round(float(mean_bl), 2))


def report(**context) -> None:
    """
    Task 3: Send Slack alert if trend is degrading or anomalies detected.
    Stable trend → log only.
    """
    import requests

    trend_direction = context["ti"].xcom_pull(
        task_ids="analyze_trend", key="trend_direction"
    )
    anomaly_count = context["ti"].xcom_pull(
        task_ids="analyze_trend", key="anomaly_count"
    )
    trend_delta = context["ti"].xcom_pull(
        task_ids="analyze_trend", key="trend_delta"
    )
    mean_baseline = context["ti"].xcom_pull(
        task_ids="analyze_trend", key="mean_baseline"
    )

    if trend_direction == "insufficient_data":
        print("[report] Insufficient data for trend analysis — skipping alert.")
        return

    is_degrading  = trend_direction == "degrading"
    has_anomalies = anomaly_count > 0

    ## If stable --> log only
    if not is_degrading and not has_anomalies:
        print(f"[report] Trend stable ({trend_delta:+.1f} pts) — no alert needed.")
        return

    ## If degrading or has anomaly --> alert
    icon = "📉" if is_degrading else "⚠️"
    text = (
        f"{icon} *Quality Trend Alert* — `{PIPELINE_NAME}`\n"
        f"Trend     : {trend_direction} ({trend_delta:+.1f} pts)\n"
        f"Anomalies : {anomaly_count} day(s) flagged\n"
        f"Baseline  : {mean_baseline:.1f} / 100\n"
        f"Action    : Investigate quality_history in BigQuery"
    )

    print(f"[report] Sending Slack alert — {trend_direction}, {anomaly_count} anomalies")

    try:
        response = requests.post(
            SLACK_WEBHOOK_URL,
            json={"text": text},
            timeout=5,
        )
        if response.status_code == 200:
            print("[report] Slack alert sent successfully.")
        else:
            print(f"[report] Slack failed: {response.status_code} {response.text}")
    except Exception as e:
        print(f"[report] Slack error: {e}")



# ─────────────────────────────────────────────
# TREND ANALYZER DAG
# ─────────────────────────────────────────────
with DAG(
    dag_id="dag_trend_analyzer",
    schedule_interval="0 9 * * *",     # daily at 09:00
    start_date=datetime(2026, 8, 16),
    catchup=False,
    tags=["monitoring", "trend", "quality", "orders"],
) as dag:

    task_1 = PythonOperator(
        task_id="fetch_history",
        python_callable=fetch_history,
    )

    task_2 = PythonOperator(
        task_id="analyze_trend",
        python_callable=analyze_trend,
    )

    task_3 = PythonOperator(
        task_id="report",
        python_callable=report,
    )

    task_1 >> task_2 >> task_3
