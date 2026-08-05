# Data Quality Scoring System

A production-grade data quality framework that measures six dimensions of data health, computes a weighted quality score, enforces hard-block thresholds on critical dimensions, and maintains a full audit trail for trend detection.

Built as part of a modern data stack pipeline handling transaction data at scale.

---

## Architecture

```
pipeline_transaction.py          ← pipeline-specific config + entry point
│
├── measure_completeness()       ┐
├── measure_validity()           │
├── measure_accuracy()           ├─ six independent measure functions
├── measure_consistency()        │  each returns float 0.0–1.0
├── measure_uniqueness()         │
└── measure_timeliness()         ┘
         │
         ▼ dimension_scores (dict)
         │
quality_scorer.py
│
├── ScorerConfig                 ← weights + thresholds per pipeline
├── DimensionConfig              ← weight + hard block per dimension
├── QualityScorer.score()        ← weighted avg + hard block check + status
├── QualityScoreResult           ← result container (dataclass)
│
├── print_score_report()         → terminal report with visual bars
└── save_to_history()            → appends to quality_history.csv
```

---

## Quality Dimensions

Six dimensions are measured independently. Each captures a distinct aspect of data health that the others cannot detect.

| Dimension | Question answered | Hard block |
|---|---|---|
| **Completeness** | Are all expected values present? | ≥ 0.85 |
| **Validity** | Do values conform to expected types and formats? | — |
| **Accuracy** | Are values correct relative to real-world rules? | ≥ 0.70 |
| **Consistency** | Do values across columns follow logical rules? | — |
| **Uniqueness** | Are there no unwanted duplicate records? | ≥ 0.95 |
| **Timeliness** | Is the data fresh enough for the decision at hand? | — |

> Dimensions are evaluated in this order by design: completeness and validity failures make downstream checks meaningless, so they run first.

---

## Scoring Model

```
Quality Score = Σ (dimension_score × weight) × 100
```

### Transaction Pipeline Weights

| Dimension | Weight | Rationale |
|---|---|---|
| Completeness | 20% | Missing values propagate to all downstream reports |
| Validity | 15% | Format errors invalidate downstream processing |
| Accuracy | 30% | Incorrect transaction values = direct financial loss |
| Consistency | 15% | Cross-column conflicts produce misleading aggregations |
| Uniqueness | 10% | Duplicate transactions = double-charging customers |
| Timeliness | 10% | Stale data leads to decisions based on outdated state |

### Status Thresholds

```
Score ≥ 80   →  ✅ PASS    pipeline continues normally
Score 60–79  →  ⚠️  WARN    pipeline continues, alert sent to team
Score < 60   →  ❌ FAIL    pipeline halts, data quarantined
```

### Hard Block Override

Certain dimensions have a hard block threshold that overrides the total score. If any hard-blocked dimension falls below its threshold, the pipeline status is immediately set to **FAIL** regardless of the weighted total.

```
Example:
  Total score      = 82.5  →  would normally be PASS
  Uniqueness score = 0.90  →  below hard block 0.95
  Final status     = FAIL  ←  hard block wins
```

This prevents scenarios where high scores in non-critical dimensions mask a catastrophic failure in a critical one.

---

## Pipeline Flow

```
DataFrame (raw batch)
      │
      ├─ measure_completeness()  →  0.975
      ├─ measure_validity()      →  0.900
      ├─ measure_accuracy()      →  0.850
      ├─ measure_consistency()   →  1.000
      ├─ measure_uniqueness()    →  0.900
      └─ measure_timeliness()    →  0.000
                │
                ▼ dimension_scores dict
                │
        QualityScorer.score()
                │
         ┌──────┴──────┐
         ▼             ▼
  print_score_report   save_to_history
  (terminal output)    (quality_history.csv)
```

---

## Position in the Full Data Stack

This quality scoring system sits at **Gate 3** in the broader pipeline architecture:

```
[SOURCE] Raw data
      ↓
[BRONZE] GCS raw layer — stored as-is, never modified
      ↓
[GATE 1] Great Expectations — schema validation, hard null checks
      ↓
[GATE 2] Anomaly Detection — per-row Z-score / IQR flagging
      ↓
[GATE 3] Quality Scorer ← this system
      ↓  PASS/WARN → continue   |   FAIL → halt + alert
[TRANSFORM] Apache Spark — clean, normalize, deduplicate
      ↓
[SILVER] GCS processed layer
      ↓
[GATE 4] GE post-transform validation
      ↓
[GOLD] BigQuery — production-ready for analysts and ML
      ↓
[GATE 5] Quality score history — trend detection
```

---

## Design Decisions

**Separation of concern between scorer and pipeline config.** `quality_scorer.py` contains only the scoring infrastructure, it has no knowledge of any specific pipeline. Pipeline-specific configuration (column names, rules, weights) lives in `pipeline_transaction.py`. A new pipeline requires only a new config file, not changes to the scorer.

**Measure functions do not modify data.** Every `measure_*` function reads the DataFrame and returns a score. No rows are dropped, imputed, or modified. Data cleaning is the responsibility of the Spark transform layer downstream.

**Hard blocks are pipeline-specific.** Not every dimension needs a hard block. Only dimensions where a low score represents unacceptable risk to the business (financial loss, regulatory violation, double-charging) are given hard block thresholds.

**History logging enables trend detection.** Every scoring run appends one row to `quality_history.csv`. This allows detecting gradual quality degradation, a score trending from 95 to 80 over two weeks is a signal worth investigating before it becomes a failure.

---

## Project Structure

```
week_2/day_3/
├── quality_scorer.py          # scoring infrastructure (reusable across pipelines)
├── pipeline_transaction.py    # transaction pipeline config + entry point
├── tests/
│   ├── test_completeness.py
│   ├── test_accuracy.py
│   ├── test_validity.py
│   ├── test_consistency.py
│   ├── test_timeliness.py
│   └── test_uniqueness.py
└── README.md
```

---

## How to Run

```bash
# activate virtual environment
source ~/de_learning/bulan_4/week_1/ai_adjacent/venv_ai/bin/activate

# run quality check on sample transaction data
cd bulan_4/week_2/day_3
python pipeline_transaction.py
```

Expected output:

```
=======================================================
QUALITY SCORE REPORT — TRANSACTION_PIPELINE
=======================================================
Scored at   : 2026-08-04 14:40:19 UTC
Row count   : 10
Total score : 82.5 / 100
Status      : ❌ FAIL
Hard block  : uniqueness score 0.90 below hard block 0.95

DIMENSION SCORES:
-------------------------------------------------------
  completeness   ████████████████████  97.5%
  validity       ██████████████████░░  90.0%
  accuracy       █████████████████░░░  85.0%
  consistency    ████████████████████  100.0%
  uniqueness     ██████████████████░░  90.0%
  timeliness     ░░░░░░░░░░░░░░░░░░░░  0.0%
=======================================================
Score saved to quality_history.csv
```

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.11 | Core implementation |
| pandas | DataFrame operations and CSV I/O |
| numpy | Numeric type handling and NaN detection |
| dataclasses | Config and result containers |
| Apache Spark | Downstream data cleaning (transform layer) |
| Apache Airflow | Pipeline orchestration and scheduling |
| Google BigQuery | Production data warehouse target |
| Great Expectations | Gate 1 and Gate 4 schema validation |
