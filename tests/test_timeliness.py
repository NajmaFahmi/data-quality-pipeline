import pandas as pd
from datetime import datetime, timezone, timedelta


## Timeliness check
# Measures how fresh the data is relative to a defined SLA window
def measure_timeliness(latest_timestamp: datetime, sla_hours: float) -> float:
    """
    Measure timeliness based on data freshness vs SLA.
    Returns 0.0 if data is older than SLA, scales linearly otherwise.
    """
    now = datetime.now(timezone.utc)
    # if the timestamp has no timezone info, assume UTC (general time formatting)
    if latest_timestamp.tzinfo is None:
        latest_timestamp = latest_timestamp.replace(tzinfo=timezone.utc)

    # calculate how many hours old the data is
    age_hours = (now - latest_timestamp).total_seconds() / 3600
    # score freshness from 1.0 (just arrived) to 0.0 (at or beyond SLA limit)
    return max(0.0, 1.0 - (age_hours / sla_hours))


## Run the Function
if __name__ == "__main__":

    ## SCENARIO 1 -- Real Timestamp Input
    ## simulates how the function behaves in a live pipeline
    now = datetime.now(timezone.utc)

    scenarios = {
    "30 minutes ago":  now - timedelta(minutes=30),
    "2 hours ago":     now - timedelta(hours=2),
    "4 hours ago":     now - timedelta(hours=4),
    "6 hours ago":     now - timedelta(hours=6),
    "10 hours ago":    now - timedelta(hours=10),
    "timezone naive":  datetime(2026, 7, 30, 10, 0),
    }

    ## Freshness limit (SLA)
    SLA_HOURS = 6.0

    ## Score results
    print("=" * 55)
    print(f"Maximum SLA: {SLA_HOURS} hours")
    print("=" * 55)

    for label, timestamp in scenarios.items():
        score = measure_timeliness(timestamp, SLA_HOURS)

        ## print results
        if timestamp.tzinfo is None:
            age_display = "(timezone naive — assumed UTC)"
        else:
            age_hours = (now - timestamp).total_seconds() / 3600
            age_display = f"data age: {age_hours:.1f} hours"

        status = "PASS" if score >= 0.8 else ("WARN" if score >= 0.6 else "FAIL")
        print(f"  {label:<20} {age_display:<35} score: {score:.2f}  → {status}")


    ## SCENARIO 2 -- Manual Formula Walkthrough
    ## uses fixed hour values to show how the score changes mathematically
    print()
    print("=" * 55)
    print("FORMULA: max(0.0, 1.0 - (age_hours / sla_hours))")
    print("=" * 55)

    test_cases = [
    ("Data 0h",  0,   SLA_HOURS),
    ("Data 3h",  3,   SLA_HOURS),
    ("Data 6h",  6,   SLA_HOURS),
    ("Data 9h",  9,   SLA_HOURS),
    ("Data 12h", 12,  SLA_HOURS),
    ]

    for label, age, sla in test_cases:
        raw = 1.0 - (age / sla)
        final = max(0.0, raw)
        print(f"  {label}: 1 - ({age}/{sla}) = {raw:.2f} → max(0, {raw:.2f}) = {final:.2f}")
    

    ## SCENARIO 3 -- Comparing Different SLA
    # shows that timeliness score is meaningless without knowing the SLA context
    print()
    print("=" * 55)
    print("SLA COMPARISON — score changes based on different SLA windows")
    print("=" * 55)

    timestamp_3h = now - timedelta(hours=3)
    for sla in [1.0, 3.0, 6.0, 12.0, 24.0]:
        score = measure_timeliness(timestamp_3h, sla)
        print(f"  Data 3h, SLA {sla:>5.1f}h → score: {score:.2f}")