import pandas as pd
import numpy as np


## Completeness check
# Measures proportion of expected values that are present (not missing)
def measure_completeness(df: pd.DataFrame, columns: list) -> float:
    """
    Measure completeness across specified columns.
    Catches all forms of missing: None, NaN, empty string, whitespace,
    common placeholders like 'N/A', 'unknown', 'null', '-'.
    """
    placeholders = {"n/a", "unknown", "null", "-", "none", ""}  # extend this set to match your need
    missing = 0

    for col in columns:
        # if the column is missing
        if col not in df.columns:
            # all rows / data in the column is missing too
            missing += len(df)
            continue 
        # for every row in that column
        for val in df[col]:
            # catches Python None and numpy/pandas NaN
            if val is None or (isinstance(val, float) and np.isnan(val)):
                missing += 1
            # catches strings that are technically present but semantically empty (e.g. "N/A", "unknown")
            elif isinstance(val, str) and val.strip().lower() in placeholders:
                missing += 1

    return 1.0 - (missing / total_values) if total_values > 0 else 0.0


## Run the Function
if __name__ == "__main__":
    df = pd.DataFrame({
        "name":   ["Andi", "Budi", None, "Dina", ""],
        "email":  ["a@b.com", "N/A", "c@d.com", "unknown", "e@f.com"],
        "amount": [150000, 200000, 300000, None, 175000],
        "city":   ["Jakarta", "Bandung", "-", "Surabaya", "null"],
    })

    print("DataFrame:")
    print(df.to_string())
    print()

    columns_to_check = ["name", "email", "amount", "city"]
    score_completeness = measure_completeness(df, columns_to_check)

    print(f"Columns checked : {columns_to_check}")
    print(f"Total values    : {len(df)} rows × {len(columns_to_check)} cols = {len(df) * len(columns_to_check)}")
    print(f"Completeness    : {score_completeness * 100:.1f}%")
    print()

    print("Manual check — missing values per column:")
    placeholders = {"n/a", "unknown", "null", "-", "none", ""}
    for col in columns_to_check:
        missing_in_col = []
        for i, val in enumerate(df[col]):
            if val is None or (isinstance(val, float) and np.isnan(val)):
                missing_in_col.append(f"row {i}: {repr(val)} (None/NaN)")
            elif isinstance(val, str) and val.strip().lower() in placeholders:
                missing_in_col.append(f"row {i}: {repr(val)} (placeholder)")
        print(f"  {col}: {len(missing_in_col)} missing → {missing_in_col if missing_in_col else 'none'}")