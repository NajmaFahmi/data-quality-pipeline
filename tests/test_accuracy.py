import pandas as pd
import numpy as np
from typing import Dict, Optional



## Accuracy check
# Validates whether values are correct relative to real-world expectations
def measure_accuracy(
        df: pd.DataFrame, 
        range_rules: Optional[Dict[str, Dict]] = None,
        set_rules: Optional[Dict[str, set]] = None,
    ) -> float:
    """
    Measure accuracy via two methods:
    - range_rules: numeric columns must fall within min/max bounds
    - set_rules: string columns must belong to a valid reference set
    """
    violations = 0
    total_checked = 0

    # check numeric columns
    if range_rules:
        for col, bounds in range_rules.items():
            # skip missing columns — accuracy only evaluates values that exist
            if col not in df.columns:
                continue 
            # coerce non-numeric values to NaN instead of raising an error
            series = pd.to_numeric(df[col], errors="coerce") 
            total_checked += len(series)
            if "min" in bounds:
                # values below the minimum bound
                violations += (series < bounds["min"]).sum()  
            if "max" in bounds:
                # values above the maximum bound
                violations += (series > bounds["max"]).sum()
            # values that failed numeric conversion (coerced to NaN) — counted as violations
            violations += series.isna().sum()

    ## check string columns
    if set_rules:
        for col, valid_values in set_rules.items():
            if col not in df.columns:
                continue 
            total_checked += len(df)
            violations += (~df[col].isin(valid_values)).sum()


    return 1.0 - (violations / total_checked) if total_checked > 0 else 0.0



## Run the Function
if __name__ == "__main__":
    df = pd.DataFrame({
        "amount":  [150000, 200000, 999999999, -500, 175000, "tidak_valid"],
        "age":     [25, 200, 28, 35, -1, 30],
        "country": ["Indonesia", "Malaysia", "Atlantis", "Singapore", "Narnia", "Thailand"],
        "status":  ["active", "inactive", "suspended", "aktif", "active", "unknown"],
    })
    print("DataFrame:")
    print(df.to_string())
    print()


    ## Set Rules
    VALID_COUNTRIES = {"Indonesia", "Malaysia", "Singapore", "Thailand", "Vietnam", "Philippines"}
    VALID_STATUSES  = {"active", "inactive", "suspended"}

    range_rules = {
        "amount": {"min": 1,   "max": 100_000_000},
        "age":    {"min": 0,   "max": 120},
    }
    set_rules = {
        "country": VALID_COUNTRIES,
        "status":  VALID_STATUSES,
    }


    ## Proof Range Rules
    print("=" * 55)
    print("RANGE RULES — numeric columns")
    print("=" * 55)
    for col, bounds in range_rules.items():
        series = pd.to_numeric(df[col], errors="coerce")
        total_below = (series < bounds["min"]).sum() if "min" in bounds else 0
        total_above = (series > bounds["max"]).sum() if "max" in bounds else 0
        total_nan = series.isna().sum()
        print(f"\n  {col}:")
        print(f"    after to_numeric : {series.tolist()}")
        print(f"    below min ({bounds.get('min'):>12,}) : {total_below} violation(s)")
        print(f"    above max ({bounds.get('max'):>12,}) : {total_above} violation(s)")
        print(f"    failed to convert : {total_nan} violation(s)")


    ## Proof Set Rules
    print()
    print("=" * 55)
    print("SET RULES — string columns")
    print("=" * 55)
    for col, valid_values in set_rules.items():
        invalid_rows = ~df[col].isin(valid_values)
        invalid_vals = df[col][invalid_rows].tolist()
        print(f"\n  {col}:")
        print(f"    valid set    : {sorted(valid_values)}")
        print(f"    violations   : {len(invalid_vals)} → {invalid_vals}")


    ## Score Results
    print()
    score_numeric_only = measure_accuracy(df, range_rules=range_rules)
    score_string_only  = measure_accuracy(df, set_rules=set_rules)
    score_combined     = measure_accuracy(df, range_rules=range_rules, set_rules=set_rules)

    print("=" * 55)
    print("ACCURACY SCORES")
    print("=" * 55)
    print(f"  Numeric only (range)  : {score_numeric_only * 100:.1f}%")
    print(f"  String only (set)     : {score_string_only * 100:.1f}%")
    print(f"  Combined              : {score_combined * 100:.1f}%")