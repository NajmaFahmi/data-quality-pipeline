# FILE      : gates/gate3_scorer.py
# LIBRARY   : src/quality_scorer.py
# CONFIG    : scorer_config, dimension_scores — via function parameters
# __main__  : none
# CALLER    : run_pipeline.py

import pandas as pd
from src.quality_scorer import (
    ScorerConfig,
    QualityScorer,
    QualityScoreResult,
    print_score_report,
)


def run_gate3(
    df: pd.DataFrame,
    scorer_config: ScorerConfig,
    dimension_scores: dict,
) -> tuple[QualityScoreResult, dict]:
    """
    Run Gate 3 quality scoring.

    Args:
        df: Clean DataFrame from Gate 2.
        scorer_config: ScorerConfig instance defining weights and thresholds.
        dimension_scores: Pre-computed dimension scores from run_pipeline.py.

    Returns:
        Tuple of (QualityScoreResult, result_dict).
    """
    
    scorer = QualityScorer(scorer_config)
    result = scorer.score(dimension_scores, row_count=len(df))

    print_score_report(result)

    return result, {
        "passed": result.status in ("PASS", "WARN"),
        "status": result.status,
        "total_score": result.total_score,
        "hard_block_triggered": result.hard_block_triggered,
    }