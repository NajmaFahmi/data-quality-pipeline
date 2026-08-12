import logging
import pandas as pd
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional


## 1. Create Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


## 2. Dataclass: SLA configuration per table
@dataclass
class FreshnessSLA:
    """
    SLA configuration for one table.
    Each table gets its own instance — never share across tables.
    """
    table_name: str 
    sla_hours: float 
    warning_multiplier: float = 1.0
    critical_multiplier: float = 2.0


## 3. Dataclass: result of one freshness check
@dataclass
class FreshnessResult:
    """
    Result of one freshness check.
    Passed to AlertDispatcher and save_freshness_history.
    """
    table_name: str
    checked_at: datetime
    latest_timestamp: Optional[datetime]
    age_hours: float
    sla_hours: float
    severity: str
    is_breached: bool
    message: str


## 4. Checker: computes age and severity
class FreshnessChecker:
    """
    Checks data freshness for a single table against its SLA.
    Does not fetch data — caller is responsible for providing latest_timestamp.
    """

    def __init__(self, sla: FreshnessSLA):
        self.sla = sla 

    def compute_severity(self, age_hours: float) -> tuple[str, bool]:
        """Determine severity based on age vs SLA multipliers."""
        warning_threshold  = self.sla.sla_hours * self.sla.warning_multiplier
        critical_threshold = self.sla.sla_hours * self.sla.critical_multiplier

        if age_hours >= critical_threshold:
            return "CRITICAL", True
        if age_hours >= warning_threshold:
            return "WARNING", True

        return "OK", False

    def check(self, latest_timestamp: Optional[datetime]) -> FreshnessResult:
        """Run freshness check given the latest timestamp in the table."""
        now = datetime.now(timezone.utc)

        # no data in table = no latest timestamp - critical
        if latest_timestamp is None:
            return FreshnessResult(
                table_name=self.sla.table_name,
                checked_at=now,
                latest_timestamp=None,
                age_hours=float("inf"),
                sla_hours=self.sla.sla_hours,
                severity="CRITICAL",
                is_breached=True,
                message=f"{self.sla.table_name}: no data found — CRITICAL",
            )

        # can't read timezone info - replace with utc
        if latest_timestamp.tzinfo is None:
            latest_timestamp = latest_timestamp.replace(tzinfo=timezone.utc)

        # found data in table = found latest timestamp
        age_hours = (now - latest_timestamp).total_seconds() / 3600
        severity, is_breached = self.compute_severity(age_hours)
        message = (
            f"{self.sla.table_name}: {age_hours:.1f}h old, "
            f"SLA {self.sla.sla_hours}h — {severity}"
        )

        return FreshnessResult(
            table_name=self.sla.table_name,
            checked_at=now,
            latest_timestamp=latest_timestamp,
            age_hours=round(age_hours, 2),
            sla_hours=self.sla.sla_hours,
            severity=severity,
            is_breached=is_breached,
            message=message,
        )


## 5. AlertDispatcher: routes alerts based on severity
class AlertDispatcher:
    """
    Routes freshness alerts to appropriate channels.
    In production: replace _send_slack and _page_oncall with real integrations.
    """

    def dispatch(self, result: FreshnessResult) -> None:
        """Send alert based on severity. OK results are logged only."""
        if result.severity == "OK":
            logger.info(f"[OK] {result.message}")
            return

        if result.severity == "WARNING":
            self._send_slack(result, channel="#data-alerts")
            return

        if result.severity == "CRITICAL":
            self._send_slack(result, channel="#data-alerts")
            self._page_oncall(result)

    def _send_slack(self, result: FreshnessResult, channel: str) -> None:
        """
        Send Slack notification.
        In production: POST to Slack webhook URL via requests.post().
        """
        icon = "⚠️" if result.severity == "WARNING" else "🚨"
        logger.warning(f"[SLACK → {channel}] {icon} {result.message}")

    def _page_oncall(self, result: FreshnessResult) -> None:
        """
        Page on-call engineer.
        In production: POST to PagerDuty events API.
        """
        logger.critical(f"[PAGERDUTY] 🚨 CRITICAL — {result.message}")


## 6. History: append result to CSV log
def save_freshness_history(
    result: FreshnessResult,
    history_path: str = "freshness_history.csv",
) -> None:
    """
    Append freshness check result to CSV history.
    In production: insert to BigQuery table instead.
    """
    record = {
        "table_name":  result.table_name,
        "checked_at":  result.checked_at.isoformat(),
        "age_hours":   result.age_hours,
        "sla_hours":   result.sla_hours,
        "severity":    result.severity,
        "is_breached": result.is_breached,
        "message":     result.message,
    }
    df = pd.DataFrame([record])
    df.to_csv(
        history_path,
        mode="a",
        header=not pd.io.common.file_exists(history_path),
        index=False,
    )


## 7. Entry point: simulate monitoring multiple tables
if __name__ == "__main__":
    dispatcher = AlertDispatcher()

    now = datetime.now(timezone.utc)

    scenarios = [
        # (table_name, sla_hours, latest_timestamp)
        # example of latest_timestamp = datetime(2026, 8, 4, 13, 30, tzinfo=timezone.utc)
        # turn into now - timedelta for our exercise
        ("transactions",  1.0,  now - timedelta(minutes=20)),   # OK — 20 mins, SLA 1 hour
        ("transactions",  1.0,  now - timedelta(minutes=75)),   # WARNING — 1.25 hours, SLA 1 hour
        ("transactions",  1.0,  now - timedelta(hours=3)),      # CRITICAL — 3 jhoursam, SLA 1 hour
        ("daily_reports", 24.0, now - timedelta(hours=20)),     # OK — 20 hours, SLA 24 hours
        ("daily_reports", 24.0, now - timedelta(hours=30)),     # WARNING — 30 hours, SLA 24 hours
        ("user_events",   6.0,  None),                         # CRITICAL — no data
    ]

    print("=" * 60)
    print("FRESHNESS MONITORING RUN")
    print("=" * 60)

    for table_name, sla_hours, latest_ts in scenarios:
        sla     = FreshnessSLA(table_name=table_name, sla_hours=sla_hours)
        checker = FreshnessChecker(sla)
        result  = checker.check(latest_ts)

        dispatcher.dispatch(result)
        save_freshness_history(result)

    print("=" * 60)
    print("History saved to freshness_history.csv")