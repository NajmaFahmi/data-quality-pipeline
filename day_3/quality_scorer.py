import re
import pandas as pd
import numpy as np
from dataclasses import dataclass
from typing import Dict, Optional
from datetime import datetime, timezone



### 1. DIMENSION CONFIG
## Dataclass configuration for a single quality dimension
@dataclass 
class DimensionConfig:
    """
    Configuration for a single quality dimension.
    weight: contribution to total score (all weights must sum to 1.0)
    hard_block_threshold: if score falls below this, pipeline FAILs regardless of total score
    """
    weight: float 
    hard_block_threshold: Optional[float] = None 



### 2. SCORER CONFIG
@dataclass
class ScorerConfig:
    """
    Full quality scorer configuration for one pipeline.
    Different pipelines must have different configs — never share configs across pipelines.
    Value in pass and warn threshold can be override.
    """
    pipeline_name: str
    dimensions: Dict[str, DimensionConfig]
    pass_threshold: float = 0.80
    warn_threshold: float = 0.60

    def validate(self) -> None:
        """Ensure weights sum to 1.0 before scoring runs"""
        total_weight = sum(d.weight for d in self.dimensions.values())
        # if the total is not 1
        if not abs(total_weight - 1.0) < 1e-9:
            raise ValueError(
                f"Dimension weights must sum to 1.0, got {total_weight:.4f}"
            )



### 3. DIMENSIONS CHECK
## - Completeness Check
# measures what proportion of expected values are present (not missing)
def measure_completeness(df: pd.DataFrame, columns: list) -> float:
    """
    Measure completeness across specified columns.
    Catches all forms of missing: None, NaN, empty string, whitespace,
    common placeholders like 'N/A', 'unknown', 'null', '-'.
    """
    placeholders = {"n/a", "unknown", "null", "-", "none", ""}
    total_values = len(df) * len(columns)
    missing = 0

    for col in columns:
        if col not in df.columns:
            missing += len(df)
            continue
        for val in df[col]:
            if val is None or (isinstance(val, float) and np.isnan(val)):
                missing += 1
            elif isinstance(val, str) and val.strip().lower() in placeholders:
                missing += 1

    return 1.0 - (missing / total_values) if total_values > 0 else 0.0


## - Validity Check
# measures whether values conform to expected data types and formats
def measure_validity(df: pd.DataFrame, type_rules: Dict[str, str]) -> float:
    """
    Measure validity — whether values conform to expected data types and formats.
    type_rules format: {"column": "numeric"/"string"/"email"}
    """
    EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    violations = 0
    total_checked = 0

    for col, expected_type in type_rules.items():
        if col not in df.columns:
            continue
        total_checked += len(df)

        # numeric columns
        if expected_type == "numeric":
            converted = pd.to_numeric(df[col], errors="coerce")
            violations += converted.isna().sum()

        # string columns
        elif expected_type == "string":
            violations += df[col].apply(
                lambda x: not isinstance(x, str) or len(str(x).strip()) == 0
            ).sum()

        # email columns
        elif expected_type == "email":
            violations += df[col].apply(
                lambda x: not bool(EMAIL_PATTERN.match(str(x)))
                if pd.notna(x) else 1
            ).sum()

    return 1.0 - (violations / total_checked) if total_checked > 0 else 0.0


## - Accuracy Check
# validates whether values are correct relative to real-world expectations
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

    # numeric data
    if range_rules:
        for col, bounds in range_rules.items():
            # accuracy only evaluates values that exist — completeness handles missing columns
            if col not in df.columns:
                continue
            # coerce non-numeric values to NaN instead of raising an error
            series = pd.to_numeric(df[col], errors="coerce")
            total_checked += len(series)
            if "min" in bounds:
                violations += (series < bounds["min"]).sum()
            if "max" in bounds:
                violations += (series > bounds["max"]).sum()
            # values that failed numeric conversion — counted as accuracy violations
            violations += series.isna().sum()

    # string data
    if set_rules:
        for col, valid_values in set_rules.items():
            if col not in df.columns:
                continue
            total_checked += len(df)
            violations += (~df[col].isin(valid_values)).sum()

    return 1.0 - (violations / total_checked) if total_checked > 0 else 0.0


## - Consistency Check
# measures whether values across columns follow defined logical rules (consistent)
def measure_consistency(df: pd.DataFrame, rules: list) -> float:
    """
    Measure consistency via cross-column logical rules.
    rules: list of lambda functions returning True if row is consistent.
    """
    if not rules:
        return 1.0

    violations = 0
    for _, row in df.iterrows():
        for rule in rules:
            try:
                if not rule(row):
                    violations += 1
                    break
            except Exception:
                violations += 1
                break

    return 1.0 - (violations / len(df)) if len(df) > 0 else 0.0


## - Uniqueness Check
# measures what proportion of rows are non-duplicate (unique)
def measure_uniqueness(df: pd.DataFrame, subset: list) -> float:
    """
    Measure uniqueness — proportion of non-duplicate rows.
    subset: columns to consider when identifying duplicates.
    """
    if df.empty:
        return 0.0

    duplicates = df.duplicated(subset=subset).sum()
    return 1.0 - (duplicates / len(df))


## - Timeliness Check
# measures how fresh the data is relative to a defined SLA window
def measure_timeliness(latest_timestamp: datetime, sla_hours: float) -> float:
    """
    Measure timeliness based on data freshness vs SLA.
    Returns 0.0 if data is older than SLA, scales linearly otherwise.
    """
    now = datetime.now(timezone.utc)

    if latest_timestamp.tzinfo is None:
        latest_timestamp = latest_timestamp.replace(tzinfo=timezone.utc)

    age_hours = (now - latest_timestamp).total_seconds() / 3600
    return max(0.0, 1.0 - (age_hours / sla_hours))



### 4. QUALITY SCORE CONFIG
@dataclass
class QualityScoreResult:
    """Full result of one quality scoring run — stored to history after every run."""
    pipeline_name: str 
    scored_at: datetime
    dimension_scores: Dict[str, float]
    total_score: float 
    status: str 
    hard_block_triggered: Optional[str]
    row_count: int 



### 5. QUALITY SCORER
## Orchestrator class that combines all measure functions into a single score
class QualityScorer:
    """
    Runs all dimension checks, applies weights, checks hard blocks,
    and determines final pipeline status.
    """

    # apply scorerconfig
    def __init__(self, config: ScorerConfig):
        # validate weights
        config.validate()
        self.config = config 

    # check hardblock
    def compute_status(
            self,
            total_score: float,
            hard_block_triggered: Optional[str]
    ) -> str:
        """Determine PASS/WARN/FAIL — hard block always overrides threshold check."""
        if hard_block_triggered:
            return "FAIL"
        if total_score >= self.config.pass_threshold:
            return "PASS"
        if total_score >= self.config.warn_threshold:
            return "WARN"
        return "FAIL"

    # runs all checks and scoring
    def score(
            self,
            dimension_scores: Dict[str, float],
            row_count: int = 0
    ) -> QualityScoreResult:
        """Compute weighted score, check hard blocks, return full result."""
        # configuration
        hard_block_triggered = None 
        weighted_total = 0.0

        for dim_name, dim_config in self.config.dimensions.items():
            # 1. count weighted total
            score = dimension_scores.get(dim_name, 0.0)
            weighted_total += score * dim_config.weight

            # 2. check hard block (one violation gonna fail the full pipeline)
            if (dim_config.hard_block_threshold is not None
                    and score < dim_config.hard_block_threshold
                    and hard_block_triggered is None):
                hard_block_triggered = (
                    f"{dim_name} score {score:.2f}"
                    f" below hard block {dim_config.hard_block_threshold}"
                )

        # 3. check status
        status = self.compute_status(weighted_total, hard_block_triggered)

        return QualityScoreResult(
            pipeline_name=self.config.pipeline_name,
            scored_at=datetime.now(timezone.utc),
            dimension_scores=dimension_scores,
            total_score=round(weighted_total * 100, 2),
            status=status,
            hard_block_triggered=hard_block_triggered,
            row_count=row_count,
        )



### 6. SCORE RESULT REPORT
def print_score_report(result: QualityScoreResult) -> None:
    """Print human-readable scoring report to terminal."""
    status_symbol = {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌"}.get(result.status, "?")

    print("=" * 55)
    print(f"QUALITY SCORE REPORT — {result.pipeline_name.upper()}")
    print("=" * 55)
    print(f"Scored at   : {result.scored_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Row count   : {result.row_count}")
    print(f"Total score : {result.total_score:.1f} / 100")
    print(f"Status      : {status_symbol} {result.status}")

    if result.hard_block_triggered:
        print(f"Hard block  : {result.hard_block_triggered}")

    print()
    print("DIMENSION SCORES:")
    print("-" * 55)
    for dim, score in result.dimension_scores.items():
        bar_len = int(score * 20)
        bar = "█" * bar_len + "░" * (20 - bar_len)
        print(f"  {dim:<14} {bar}  {score*100:.1f}%")
    print("=" * 55)



### 7. SAVE RESULT REPORT
def save_to_history(
        result: QualityScoreResult,
        history_path: str = "quality_history.csv"
) -> None:
    """
    Append score result to CSV history file.
    In production: insert to BigQuery table instead.
    """
    record = {
        "pipeline_name": result.pipeline_name,
        "scored_at": result.scored_at.isoformat(),
        "total_score": result.total_score,
        "status": result.status,
        "hard_block_triggered": result.hard_block_triggered or "",
        **{f"score_{k}": v for k, v in result.dimension_scores.items()},
    }
    history_df = pd.DataFrame([record])
    history_df.to_csv(
        history_path,
        mode="a",
        header=not pd.io.common.file_exists(history_path),
        index=False,
    )
    print(f"Score saved to {history_path}")
