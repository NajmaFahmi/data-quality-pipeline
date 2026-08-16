# FILE      : gates/gate2_anomaly.py
# LIBRARY   : src/anomaly_detector.py
# CONFIG    : numeric_columns, method, multiplier, zscore_threshold,
#             quarantine_rate_threshold — via function parameters
# __main__  : none
# CALLER    : run_pipeline.py

import pandas as pd
from src.anomaly_detector import detect_anomalies


def run_gate2(
        df: pd.DataFrame,
        numeric_columns: list,
        method: str = "iqr",
        multiplier: float = 1.5,
        zscore_threshold: float = 3.0,
        quarantine_rate_threshold: float = 0.2,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Run Gate 2 anomaly detection and quarantine.

    Args:
        df: Input DataFrame from Gate 1.
        numeric_columns: Columns to run anomaly detection on.
        method: Detection method — 'iqr', 'zscore', or 'both'.
        multiplier: IQR multiplier for outlier bounds.
        zscore_threshold: Z-score threshold for outlier flagging.
        quarantine_rate_threshold: Max acceptable quarantine rate before FAIL.

    Returns:
        Tuple of (clean_df, quarantine_df, result_dict).
    """

    clean_df, quarantine_df = detect_anomalies(
        df,
        numeric_columns,
        method,
        multiplier,
        zscore_threshold,
    )

    quarantine_rate = round(len(quarantine_df) / len(df), 4) if len(df) > 0 else 0.0
    passed = quarantine_rate <= quarantine_rate_threshold

    result = {
        "passed": passed,
        "total_input": len(df),
        "clean_records": len(clean_df),
        "quarantine_records": len(quarantine_df),
        "quarantine_rate": quarantine_rate,
        "quarantine_rate_threshold": quarantine_rate_threshold,
    }

    status = "PASS" if passed else "FAIL"
    print(f"Gate 2 [{status}] — {len(clean_df)} clean, {len(quarantine_df)} quarantined ({quarantine_rate:.1%})")

    if not passed:
        print(f"  Quarantine rate {quarantine_rate:.1%} exceeds threshold {quarantine_rate_threshold:.1%}")

    return clean_df, quarantine_df, result