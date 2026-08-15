# jobs/run_trend.py

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from data.history_simulator import simulate_history
from src.trend_analyzer import (
    compute_rolling_avg,
    detect_score_anomaly,
    print_trend_report,
)


### 1. Define Quality History Path
ROOT     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(ROOT, "quality_history.csv")


### 2. Trend Analysis
def run_trend_analysis(
    pipeline_name: str = "orders_pipeline",
    simulate: bool = False,
) -> None:
    """
    Run trend analysis for a given pipeline.
    simulate: if True, inject synthetic history before analysis (for demo only)
    """
    if simulate:
        simulate_history(pipeline_name=pipeline_name, csv_path=CSV_PATH)

    df = pd.read_csv(CSV_PATH)
    df = df[df["pipeline_name"] == pipeline_name].copy()
    df["scored_at"] = pd.to_datetime(df["scored_at"], utc=True)
    df = df.sort_values("scored_at").reset_index(drop=True)

    if len(df) < 7:
        print(f"Not enough history: {len(df)} records. Need at least 7.")
        return

    print(f"Total records loaded: {len(df)}")
    print()

    df = compute_rolling_avg(df, window=7)
    df, mean_bl, std_bl = detect_score_anomaly(df, baseline_days=14)

    print(f"Baseline (first 14 days): mean={mean_bl:.1f}, std={std_bl:.2f}")
    print()

    print_trend_report(df)



if __name__ == "__main__":
    run_trend_analysis(pipeline_name="orders_pipeline", simulate=True)