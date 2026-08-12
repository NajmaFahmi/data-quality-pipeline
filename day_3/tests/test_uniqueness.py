import pandas as pd


## Uniqueness check
# Measures what proportion of rows are non-duplicate 
def measure_uniqueness(df: pd.DataFrame, subset: list) -> float:
    """
    Measure uniqueness — proportion of non-duplicate rows.
    subset: columns to consider when identifying duplicates.
    """
    # return 0.0 for empty DataFrames 
    if df.empty:
        return 0.0

    # check duplicates in column subset (keep='first' by default)
    duplicates = df.duplicated(subset=subset).sum()

    # score = proportion of rows that are not duplicates
    return 1.0 - (duplicates / len(df))


## Run the Function
if __name__ == "__main__":
    ## Dataframe
    df = pd.DataFrame({
        "transaction_id": ["T001", "T002", "T003", "T002", "T004", "T001"],
        "amount":         [150000, 200000, 300000, 200000, 175000, 999999],
        "customer_id":    ["C01", "C02", "C03", "C02", "C04", "C01"],
        "email":          ["a@b.com", "c@d.com", "e@f.com",
                           "c@d.com", "g@h.com", "a@b.com"],
    })
    print("DataFrame:")
    print(df.to_string())
    print()

    ## Proof with Manual Check
    print("=" * 55)
    print("SCORE CHECK — duplicates per subset")
    print("=" * 55)

    subsets = {
        "transaction_id saja":        ["transaction_id"],
        "customer_id saja":           ["customer_id"],
        "transaction_id + amount":    ["transaction_id", "amount"],
        "semua kolom":                ["transaction_id", "amount", "customer_id", "email"],
    }

    for label, subset in subsets.items():
        # score results
        score = measure_uniqueness(df, subset)
        # show duplicates data
        dup_mask = df.duplicated(subset=subset)
        dup_rows = df[dup_mask]
        # print results
        print(f"\n  Subset: {label}")
        print(f"  Duplicates flagged: {dup_mask.sum()} baris")
        if not dup_rows.empty:
            print("  Flagged rows:")
            for i, row in dup_rows.iterrows():
                print(f"    row {i}: {dict(row)}")
        print(f"  Uniqueness score: {score * 100:.1f}%")


    ## Notes
    print()
    print("=" * 55)
    print("NOTE: duplicated() flags the SECOND occurrence onward, not the first")
    print("=" * 55)
    print()

    dup_full = df.duplicated(subset=["transaction_id"], keep="first")
    dup_all  = df.duplicated(subset=["transaction_id"], keep=False)

    print("  keep='first' (default) — flags second and subsequent occurrences:")
    for i, (flagged, row) in enumerate(zip(dup_full, df["transaction_id"])):
        print(f"    row {i}: {row} → {'DUPLICATE (flagged)' if flagged else 'first occurrence (not flagged)'}")

    print()
    print("  keep=False — flags ALL occurrences including the first:")
    for i, (flagged, row) in enumerate(zip(dup_all, df["transaction_id"])):
        print(f"    row {i}: {row} → {'DUPLICATE (flagged)' if flagged else 'unique'}")

    print()
    print("=" * 55)
    print("WHY keep='first' is used in measure_uniqueness?")
    print("=" * 55)
    print("  Goal: count how many rows are excess and should be removed.")
    print("  The first occurrence is the legitimate record — it stays.")
    print("  Only the second, third, and subsequent occurrences are flagged.")
    print("  Score = proportion of rows that are not excess out of total rows.")
        
