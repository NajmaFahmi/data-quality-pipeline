import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone


def simulate_history(
    pipeline_name: str,
    n_days: int = 29,
    weights: dict = None,
    csv_path: str = "quality_history.csv",
) -> pd.DataFrame:
    """
    Generate synthetic quality score history for any pipeline.
    Simulates healthy scores for first half, gradual degradation for second half.
    pipeline_name : name to tag all records with
    weights       : dimension weights for total score calculation
    csv_path      : path to append history CSV
    """
    if weights is None:
        weights = {
            "completeness": 0.30,
            "validity":     0.10,
            "accuracy":     0.25,
            "consistency":  0.10,
            "uniqueness":   0.15,
            "timeliness":   0.10,
        }

    now     = datetime.now(timezone.utc)
    records = []

    for day in range(n_days, 0, -1):
        scored_at   = now - timedelta(days=day)
        degradation = max(0, (n_days - day - 14) * 0.015)

        base = {
            "completeness": min(1.0, 0.97 + np.random.uniform(-0.02, 0.02)),
            "validity":     min(1.0, 0.96 + np.random.uniform(-0.02, 0.02)),
            "accuracy":     min(1.0, 0.95 + np.random.uniform(-0.02, 0.02)),
            "consistency":  min(1.0, 0.97 + np.random.uniform(-0.01, 0.01)),
            "uniqueness":   min(1.0, 0.98 + np.random.uniform(-0.01, 0.01)),
            "timeliness":   min(1.0, 0.85 + np.random.uniform(-0.05, 0.05)),
        }

        # apply degradation to most critical dimensions
        base["completeness"] = max(0, base["completeness"] - degradation)
        base["accuracy"]     = max(0, base["accuracy"] - degradation * 1.2)

        total  = sum(base[d] * w for d, w in weights.items()) * 100
        status = "PASS" if total >= 80 else "WARN" if total >= 60 else "FAIL"

        records.append({
            "pipeline_name":        pipeline_name,
            "scored_at":            scored_at.isoformat(),
            "total_score":          round(total, 2),
            "status":               status,
            "hard_block_triggered": "",
            **{f"score_{k}": round(v, 4) for k, v in base.items()},
        })

    history_df = pd.DataFrame(records)
    history_df.to_csv(
        csv_path,
        mode="a",
        header=not pd.io.common.file_exists(csv_path),
        index=False,
    )
    print(f"Simulated {n_days} days of history for '{pipeline_name}' → {csv_path}")
    return history_df


if __name__ == "__main__":
    simulate_history(pipeline_name="orders_pipeline")
    print("history_simulator.py verified.")