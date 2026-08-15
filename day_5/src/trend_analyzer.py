import pandas as pd
import numpy as np



### 1. Compute Rolling Average for Total Score
# for example: window = 7 --> 7 latest data 
def compute_rolling_avg(
        df: pd.DataFrame,
        window: int = 7,
) -> pd.DataFrame:
    """
    Compute rolling average of total_score over N days.
    Smooths daily fluctuations to reveal long-term trend.
    df must have columns: scored_at, total_score
    """
    df = df.copy()
    df["scored_at"] = pd.to_datetime(df["scored_at"], utc=True)
    df = df.sort_values("scored_at").reset_index(drop=True)
    df["rolling_avg"] = (
        df["total_score"].rolling(window=window, min_periods=1).mean()
    )

    return df



### 2. Detect Anomalous Scores using Z-score against Historical Baseline
def detect_score_anomaly(
        df: pd.DataFrame,
        baseline_days: int = 14,
        z_threshold: float = 2.0,
) -> tuple[pd.DataFrame, float, float]:
    """
    Flag days where total_score is anomalously low vs historical baseline.
    baseline_days : how many days to use as normal reference
    z_threshold   : how many std devs below mean counts as anomaly
    Returns (df_with_flags, mean_baseline, std_baseline)
    """
    df = df.copy()

    # using the 14 earliest data point where data still healthy
    baseline = df.head(baseline_days)
    mean_baseline = baseline["total_score"].mean()
    std_baseline = baseline["total_score"].std()

    df["z_score"]    = (df["total_score"] - mean_baseline) / std_baseline
    df["is_anomaly"] = df["z_score"] < -z_threshold

    return df, mean_baseline, std_baseline



### 3. Print Trend Report
def print_trend_report(df: pd.DataFrame) -> None:
    """
    Print trend analysis report.
    df must have: scored_at, total_score, rolling_avg,
    z_score, is_anomaly, pipeline_name, score_* per dimension
    """
    print("=" * 60)
    print("QUALITY TREND ANALYSIS")
    print("=" * 60)
    print(f"Pipeline        : {df['pipeline_name'].iloc[0]}")
    print(f"Period analyzed : {df['scored_at'].min().date()} "
          f"→ {df['scored_at'].max().date()}")
    print(f"Total records   : {len(df)}")
    print()

    first_rolling   = df["rolling_avg"].iloc[6]
    last_rolling    = df["rolling_avg"].iloc[-1]
    trend_delta     = last_rolling - first_rolling
    trend_direction = "↑ improving" if trend_delta > 0 else "↓ degrading"

    print("ROLLING AVERAGE (7-day window):")
    print(f"  Week 1 avg : {first_rolling:.1f}")
    print(f"  Latest avg : {last_rolling:.1f}")
    print(f"  Trend      : {trend_direction} ({trend_delta:+.1f} points)")
    print()

    anomalies = df[df["is_anomaly"]]
    print(f"ANOMALOUS DAYS DETECTED (Z < -2.0): {len(anomalies)}")
    if not anomalies.empty:
        for _, row in anomalies.iterrows():
            print(f"  {row['scored_at'].date()}  "
                  f"score={row['total_score']:.1f}  "
                  f"Z={row['z_score']:.2f}")
    print()

    dim_cols = [c for c in df.columns if c.startswith("score_")]
    print("DIMENSION TREND (first 7 days vs last 7 days):")
    print("-" * 60)

    first_7 = df.head(7)
    last_7  = df.tail(7)

    for col in dim_cols:
        dim_name  = col.replace("score_", "")
        avg_first = first_7[col].mean()
        avg_last  = last_7[col].mean()
        delta     = avg_last - avg_first
        arrow     = "↑" if delta > 0.005 else "↓" if delta < -0.005 else "→"
        alert     = " ⚠️" if delta < -0.02 else ""
        print(f"  {dim_name:<14} {avg_first*100:.1f}% → "
              f"{avg_last*100:.1f}%  {arrow} ({delta*100:+.1f}%){alert}")

    print("=" * 60)