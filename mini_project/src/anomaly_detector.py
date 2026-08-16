import pandas as pd
import numpy as np


# 1. Detect Anomalies using Z-Score Method
def detect_zscore(
        df: pd.DataFrame,
        column: str,
        threshold: float = 3.0,
) -> pd.DataFrame:
    """
    Detect point anomalies using Z-score method.
    Flags values where |Z| exceeds threshold (default: 3.0).
    """
    df = df.copy()
    series = pd.to_numeric(df[column], errors="coerce")
    mean = series.mean()
    std = series.std()

    df[f"z_score_{column}"] = (series - mean) / std 
    df[f"is_anomaly_zscore_{column}"] = df[f"z_score_{column}"].abs() > threshold 

    return df 



# 2. Detect Anomalies Using IQR Method
def detect_iqr(
        df: pd.DataFrame,
        column: str,
        multiplier: float = 1.5,
) -> pd.DataFrame:
    """
    Detect point anomalies using IQR method.
    Flags values outside Q1 - multiplier*IQR and Q3 + multiplier*IQR.
    """
    df = df.copy()
    series = pd.to_numeric(df[column], errors="coerce")
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1 

    lower = q1 - multiplier * iqr 
    upper = q3 + multiplier * iqr 

    df[f"iqr_lower_{column}"] = lower
    df[f"iqr_upper_{column}"] = upper 
    df[f"is_anomaly_iqr_{column}"] = (series < lower) | (series > upper)

    return df 



# 3. Run Detection Across Multiple Columns
def run_detection(
        df: pd.DataFrame,
        numeric_columns: list,
        method: str = "iqr",
        multiplier: float = 1.5,
        zscore_threshold: int = 3.0,
) -> pd.DataFrame:
    """
    Run anomaly detection across multiple numeric columns.
    method: 'iqr', 'zscore', or 'both'
    Adds is_anomaly column = True if flagged by any column and any method.
    """

    df = df.copy()

    anomaly_cols = []

    for col in numeric_columns:
        # if column can't be found in dataframe
        if col not in df.columns:
            continue

        # iqr method
        if method in ("iqr", "both"):
            df = detect_iqr(df, col, multiplier)
            anomaly_cols.append(f"is_anomaly_iqr_{col}")

        # z-score method
        if method in ("zscore", "both"):
            df = detect_zscore(df, col, zscore_threshold)
            anomaly_cols.append(f"is_anomaly_zscore_{col}")


    # a row is anomalous if flagged by ANY method or ANY column
    df["is_anomaly"] = df[anomaly_cols].any(axis=1)

    return df 



# 4. Separate Clean and Quarantine Dataframes
def quarantine(
        df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split DataFrame into clean and quarantine based on is_anomaly flag.
    Returns (clean_df, quarantine_df).
    clean_df      : rows with no anomaly detected
    quarantine_df : rows flagged as anomaly, removed from pipeline
    """
    if "is_anomaly" not in df.columns:
        raise ValueError("Run run_detection() before quarantine()")

    clean_df      = df[~df["is_anomaly"]].copy()
    quarantine_df = df[df["is_anomaly"]].copy()

    # drop detection columns from clean data — not needed downstream
    detection_cols = [c for c in df.columns if c.startswith(
        ("z_score_", "is_anomaly_", "iqr_lower_", "iqr_upper_")
    )]
    clean_df = clean_df.drop(columns=detection_cols)

    return clean_df, quarantine_df



# 5. Run Anomaly Detector
## Single entry point for anomaly detection + quarantine
def detect_anomalies(
    df: pd.DataFrame,
    numeric_columns: list,
    method: str = "iqr",
    multiplier: float = 1.5,
    zscore_threshold: float = 3.0,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Convenience wrapper: run detection then quarantine in one call.
    Returns (clean_df, quarantine_df).
    """
    df = run_detection(
        df,
        numeric_columns=numeric_columns,
        method=method,
        multiplier=multiplier,
        zscore_threshold=zscore_threshold,
    )
    return quarantine(df)