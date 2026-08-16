# FILE      : dags/dag_freshness_monitor.py
# LIBRARY   : apache-airflow, google-cloud-storage, src/freshness_monitor.py
# CONFIG    : BUCKET_NAME, BLOB_PATH, SLA_HOURS, SLACK_WEBHOOK_URL — di atas file
# __main__  : none
# CALLER    : Airflow scheduler — every 12 hours (SLA = 24 hours)

import sys
import os
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.expanduser(
    "~/de_learning/bulan_4/week_2/mini_project"
))

from src.freshness_monitor import (
    FreshnessSLA,
    FreshnessChecker,
    FreshnessResult,
    AlertDispatcher,
)



# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
BUCKET_NAME       = "retailco-raw-najma"
BLOB_PATH         = "raw/orders/orders.csv"
SLA_HOURS         = 24.0
WARNING_MULT      = 1.0
CRITICAL_MULT     = 2.0
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")



# ─────────────────────────────────────────────
# TASK FUNCTIONS (3 TASKS)
# ─────────────────────────────────────────────
def check_latest_timestamp(**context) -> dict:
    """
    Task 1: Fetch latest file timestamp from GCS BRONZE layer.
    Pushes latest_timestamp as XCom for next task.
    """
    from google.cloud import storage

    client = storage.Client()
    blob = client.bucket(BUCKET_NAME).get_blob(BLOB_PATH)

    if blob is None:
        print(f"[check_latest_ts] No file found at gs://{BUCKET_NAME}/{BLOB_PATH}")
        context["ti"].xcom_push(key="latest_timestamp", value=None)
        return {"latest_timestamp": None}

    latest_ts = blob.updated
    print(f"[check_latest_ts] Latest file timestamp Bronze Layer: {latest_ts}")
    context["ti"].xcom_push(
        key="latest_timestamp",
        value=latest_ts.isoformat(),
    )

    return {"latest_timestamp": latest_ts.isoformat()}


def evaluate_sla(**context) -> str:
    """
    Task 2: Run FreshnessChecker against SLA config.
    Pushes severity as XCom for next task.
    """
    ts_str = context["ti"].xcom_pull(
        task_ids="check_latest_ts", key="latest_timestamp"
    )

    if ts_str is not None:
        latest_ts = datetime.fromisoformat(ts_str)
        if latest_ts.tzinfo is None:
            latest_ts = latest_ts.replace(tzinfo=timezone.utc)
    else:
        latest_ts = None 

    sla = FreshnessSLA(
        table_name="orders_bronze",
        sla_hours=SLA_HOURS,
        warning_multiplier=WARNING_MULT,
        critical_multiplier=CRITICAL_MULT,
    )

    checker = FreshnessChecker(sla)
    result = checker.check(latest_ts)

    print(f"[evaluate_sla] {result.message}")
    context["ti"].xcom_push(key="severity", value=result.severity)
    context["ti"].xcom_push(key="message", value=result.message)
    context["ti"].xcom_push(key="age_hours", value=result.age_hours)
    context["ti"].xcom_push(key="is_breached", value=result.is_breached)
    context["ti"].xcom_push(key="latest_timestamp", value=ts_str)

    return result.severity


def dispatch_alert(**context) -> None:
    """
    Task 3: Dispatch alert based on severity from Task 2.
    OK → log only. WARNING → Slack. CRITICAL → Slack + PagerDuty.
    """
    severity  = context["ti"].xcom_pull(task_ids="evaluate_sla", key="severity")
    message   = context["ti"].xcom_pull(task_ids="evaluate_sla", key="message")
    age_hours = context["ti"].xcom_pull(task_ids="evaluate_sla", key="age_hours")
    is_breached = context["ti"].xcom_pull(task_ids="evaluate_sla", key="is_breached")
    latest_ts_str = context["ti"].xcom_pull(task_ids="evaluate_sla", key="latest_timestamp")
    latest_ts = datetime.fromisoformat(latest_ts_str) if latest_ts_str else None
    
    result = FreshnessResult(
        table_name="orders_bronze",
        checked_at=datetime.now(timezone.utc),
        latest_timestamp=latest_ts,
        age_hours=age_hours,
        sla_hours=SLA_HOURS,
        severity=severity,
        is_breached=is_breached,
        message=message,
    )

    dispatcher = AlertDispatcher(slack_webhook_url=SLACK_WEBHOOK_URL)
    dispatcher.dispatch(result)
    print(f"[dispatch_alert] Alert dispatched — severity: {severity}")



# ─────────────────────────────────────────────
# FRESHNESS MONITORING DAG
# ─────────────────────────────────────────────
with DAG(
    dag_id="dag_freshness_monitor",
    schedule_interval="0 */12 * * *",   # every 12 hours
    start_date=datetime(2026, 8, 16),   # today on 16th August 2026
    catchup=False,
    tags=["monitoring", "freshness", "orders"],
) as dag:

    task_1 = PythonOperator(
        task_id="check_latest_ts",
        python_callable=check_latest_timestamp,
    )

    task_2 = PythonOperator(
        task_id="evaluate_sla",
        python_callable=evaluate_sla,
    )

    task_3 = PythonOperator(
        task_id="dispatch_alert",
        python_callable=dispatch_alert,
    )

    task_1 >> task_2 >> task_3