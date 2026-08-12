import pandas as pd



## Consistency check
# Measures whether values across columns follow defined logical rules
# counts rows that violate at least one rule (not total rule violations)
def measure_consistency(df: pd.DataFrame, rules: list) -> float:
    """
    Measure consistency via cross-column logical rules.
    rules: list of lambda functions returning True if row is consistent.
    """
    # if there is no rule, then pass the test
    if not rules:
        return 1.0

    violations = 0

    # for every row in the data
    for _, row in df.iterrows():
        # apply rules
        for rule in rules:
            try:
                # if violate the rule
                if not rule(row):
                    # once a row violates one rule, stop checking further rules for that row
                    # we count total inconsistent rows, not total violations 
                    violations += 1
                    break 
            except Exception:
                violations += 1
                break 

    return 1.0 - (violations / len(df)) if len(df) > 0 else 0.0



## Run the Function
if __name__ == "__main__":
    ## Dataframe
    df = pd.DataFrame({
        "age":        [25, 15, 30, 10, 35, 28],
        "is_adult":   [True, True, True, False, True, True],
        "order_date": ["2026-01-10", "2026-01-15", "2026-01-20",
                       "2026-01-05", "2026-01-08", "2026-01-12"],
        "ship_date":  ["2026-01-12", "2026-01-14", "2026-01-25",
                       "2026-01-03", "2026-01-10", "2026-01-15"],
        "country":    ["Indonesia", "Malaysia", "Indonesia",
                       "Singapore", "Thailand", "Indonesia"],
        "currency":   ["IDR", "MYR", "USD", "SGD", "THB", "IDR"],
    })
    print("DataFrame:")
    print(df.to_string())
    print()


    ## Set rules
    rules = [
        lambda row: not (row["age"] < 18 and row["is_adult"] == True),
        lambda row: row["order_date"] <= row["ship_date"],
        lambda row: (row["country"] == "Indonesia" and row["currency"] == "IDR")
                    or (row["country"] == "Malaysia" and row["currency"] == "MYR")
                    or (row["country"] == "Singapore" and row["currency"] == "SGD")
                    or (row["country"] == "Thailand" and row["currency"] == "THB"),
    ]
    rule_names = [
    "age < 18 but is_adult = True",
    "ship_date is before order_date",
    "country and currency do not match",
    ]


    ## Proof with manual check
    print("=" * 55)
    print("MANUAL CHECK — violations per rule")
    print("=" * 55)

    for rule, name in zip(rules, rule_names):
        print(f"\n  Rule: {name}")
        for i, (_, row) in enumerate(df.iterrows()):
            try:
                result = rule(row)
                if not result:
                    print(f"    row {i}: VIOLATION → {dict(row)}")
            except Exception as e:
                print(f"    row {i}: ERROR → {e}")


    ## Score results
    print()
    print("=" * 55)
    print("CONSISTENCY SCORES")
    print("=" * 55)

    score_rule1 = measure_consistency(df, [rules[0]])
    score_rule2 = measure_consistency(df, [rules[1]])
    score_rule3 = measure_consistency(df, [rules[2]])
    score_all   = measure_consistency(df, rules)

    print(f"  Rule 1 (age vs is_adult)          : {score_rule1 * 100:.1f}%")
    print(f"  Rule 2 (order vs ship date)        : {score_rule2 * 100:.1f}%")
    print(f"  Rule 3 (country vs currency)       : {score_rule3 * 100:.1f}%")
    print(f"  Combined (all rules, break on first): {score_all * 100:.1f}%")

    print()
    print("=" * 55)
    print("NOTE: break on first violation per row")
    print("=" * 55)
    print("  One row violating 3 rules = 1 violation, not 3.")
    print("  Row 1: age=15, is_adult=True → breaks rule 1, loop breaks immediately.")
    print("  Remaining rules are not evaluated for that row.")