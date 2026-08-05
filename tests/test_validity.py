import re
import pandas as pd
from typing import Dict



## Validity check
# Measures whether values conform to expected data types and formats
def measure_validity(df: pd.DataFrame, type_rules: Dict[str, str]) -> float:
    """
    Measure validity — whether values conform to expected data types and formats.
    type_rules format: {"column": "numeric"/"string"/"email"}
    """
    violations = 0
    total_checked = 0
    EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

    for col, expected_type in type_rules.items():
        # if the columm isn't in the data, skip
        if col not in df.columns:
            continue 

        total_checked += len(df)

        ## numeric columns — check if values can be converted to a number
        if expected_type == "numeric":
            # coerce non-numeric values to NaN; count NaN as violations
            converted = pd.to_numeric(df[col], errors="coerce")
            violations += converted.isna().sum()

        ## string columns — check if values are non-empty strings
        elif expected_type == "string":
            # flags None, non-string types, and whitespace-only strings
            violations += df[col].apply(
                lambda x: not isinstance(x, str) or len(str(x).strip()) == 0
            ).sum()

        ## email columns — check if values match a valid email pattern
        elif expected_type == "email":
            # None/NaN values are counted as violations 
            violations += df[col].apply(
                lambda x: not bool(EMAIL_PATTERN.match(str(x)))
                if pd.notna(x) else 1
            ).sum()

    return 1.0 - (violations / total_checked) if total_checked > 0 else 0.0



## Run the Function
if __name__ == "__main__":
    df = pd.DataFrame({
        "amount":   [150000, "tidak_valid", 200000, None, "abc", 175000],
        "name":     ["Andi", "", "Budi", None, "  ", "Dina"],
        "email":    ["a@b.com", "bukan-email", "c@d.com", None, "@domain.com", "e@f.com"],
    })
    print("DataFrame:")
    print(df.to_string())
    print()


    ## Set rules
    type_rules = {
        "amount": "numeric",
        "name":   "string",
        "email":  "email",
    }
    EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


    ## Proof with Manual Check
    print("=" * 55)
    print("MANUAL CHECK — violations per column")
    print("=" * 55)

    print("\n  amount (expected: numeric)")
    converted = pd.to_numeric(df["amount"], errors="coerce")
    print(f"    after to_numeric : {converted.tolist()}")
    for i, (orig, conv) in enumerate(zip(df["amount"], converted)):
        if pd.isna(conv):
            print(f"    row {i}: {repr(orig)} → failed to convert → violation")

    print("\n  name (expected: string)")
    for i, val in enumerate(df["name"]):
        is_violation = not isinstance(val, str) or len(str(val).strip()) == 0
        if is_violation:
            print(f"    row {i}: {repr(val)} → violation")

    print("\n  email (expected: email format)")
    for i, val in enumerate(df["email"]):
        if pd.isna(val):
            print(f"    row {i}: {repr(val)} → None/NaN → violation")
        elif not bool(EMAIL_PATTERN.match(str(val))):
            print(f"    row {i}: {repr(val)} → format invalid → violation")


    ## Score results
    print()
    print("=" * 55)
    print("VALIDITY SCORES")
    print("=" * 55)

    score_numeric = measure_validity(df, {"amount": "numeric"})
    score_string  = measure_validity(df, {"name": "string"})
    score_email   = measure_validity(df, {"email": "email"})
    score_combined = measure_validity(df, type_rules)

    print(f"  amount (numeric) : {score_numeric * 100:.1f}%")
    print(f"  name   (string)  : {score_string * 100:.1f}%")
    print(f"  email  (email)   : {score_email * 100:.1f}%")
    print(f"  combined         : {score_combined * 100:.1f}%")


    ## Validity VS Accuracy
    print()
    print("=" * 55)
    print("VALIDITY vs ACCURACY — same column, different question")
    print("=" * 55)
    print("  amount row 1: 'tidak_valid'")
    print("    validity  → VIOLATION (bukan angka, format salah)")
    print("    accuracy  → VIOLATION (gagal konversi, dihitung sebagai NaN)")
    print()
    print("  amount row 0: 150000")
    print("    validity  → PASS (bisa dikonversi ke numerik)")
    print("    accuracy  → PASS (dalam range 1 - 100,000,000)")
    print()
    print("  amount row 2: 999999999 (jika ada di data)")
    print("    validity  → PASS (bisa dikonversi ke numerik)")
    print("    accuracy  → VIOLATION (di atas max 100,000,000)")