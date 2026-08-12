import pandas as pd
from datetime import datetime, timezone
from quality_scorer import (
    ScorerConfig,
    DimensionConfig,
    QualityScorer,
    measure_completeness,
    measure_validity,
    measure_accuracy,
    measure_consistency,
    measure_uniqueness,
    measure_timeliness,
    print_score_report,
    save_to_history,
)



### Use Case -- Transaction Pipeline Configuration
# accuracy and uniqueness have hard blocks — wrong values or duplicates = direct financial loss
TRANSACTION_PIPELINE_CONFIG = ScorerConfig(
    pipeline_name="transaction_pipeline",
    dimensions={
        "completeness": DimensionConfig(weight=0.20, hard_block_threshold=0.85),
        "validity":     DimensionConfig(weight=0.15),
        "accuracy":     DimensionConfig(weight=0.30, hard_block_threshold=0.70),
        "consistency":  DimensionConfig(weight=0.15),
        "uniqueness":   DimensionConfig(weight=0.10, hard_block_threshold=0.95),
        "timeliness":   DimensionConfig(weight=0.10),
    },
    pass_threshold=0.80,
    warn_threshold=0.60,
)

def run_quality_check(df: pd.DataFrame) -> None:
    """
    Run full quality scoring on a transaction DataFrame.
    Computes all dimension scores, applies pipeline config, prints report, saves history.
    """
    ## Count Dimension Scores
    dimension_scores = {
        "completeness": measure_completeness(
            df, 
            columns=["transaction_id", "amount", "age", "email"]
        ),
        "validity": measure_validity(
            df, 
            type_rules={"amount": "numeric", "email": "email"}
        ),
        "accuracy": measure_accuracy(
            df,
            range_rules={
                "amount": {"min": 1, "max": 100_000_000},
                "age": {"min": 0, "max": 120},
            }
        ),
        "consistency": measure_consistency(
            df,
            rules=[lambda row: not (row["age"] < 18 and row["is_adult"] is True)]
        ),
        "uniqueness": measure_uniqueness(
            df,
            subset=["transaction_id"]
        ),
        "timeliness": measure_timeliness(
            latest_timestamp=df["updated_at"].max(),
            sla_hours=6.0
        ),
    }

    ## Count Score * Weights
    scorer = QualityScorer(TRANSACTION_PIPELINE_CONFIG)
    result = scorer.score(dimension_scores, row_count=len(df))

    ## Print Score Report
    print_score_report(result)
    save_to_history(result)



## Sample Transaction Data -- for testing
if __name__ == "__main__":
    df = pd.DataFrame({
        "transaction_id": ["T001", "T002", "T003", "T004", "T005",
                           "T006", "T007", "T008", "T009", "T002"],
        "amount":         [150000, 200000, None, 999999999, 175000,
                           300000, 250000, 180000, 210000, 200000],
        "age":            [25, 30, 28, 200, 35, 27, 31, 29, 26, 30],
        "email":          ["a@b.com", "c@d.com", "bad-email", "e@f.com", "g@h.com",
                           "i@j.com", "k@l.com", "m@n.com", "o@p.com", "c@d.com"],
        "is_adult":       [True, True, True, True, True,
                           True, True, True, True, True],
        "updated_at":     [datetime(2026, 7, 30, 10, 0, tzinfo=timezone.utc)] * 10,
    })

    run_quality_check(df)
