# FILE      : jobs/bq_loader.py
# LIBRARY   : google-cloud-bigquery, great_expectations, pandas
# CONFIG    : silver_gcs_path, bq_project, bq_dataset, gold_table,
#             history_table, required_columns, expected_types,
#             non_nullable_columns, pipeline_name, datasource_name,
#             asset_name, batch_name, validation_name — via function parameters
# __main__  : none
# CALLER    : run_pipeline.py


import pandas as pd
from google.cloud import bigquery
from gates.gate1_validator import build_expectation_suite
import great_expectations as gx
from src.quality_scorer import QualityScoreResult
from typing import Optional



## ===================== 1st JOB =====================
## GATE 4 (GE Validation on Silver Layer output)
## only validate data, not changing anything


### 1. Read Parquet Data from GCS
def read_parquet_from_gcs(gcs_path: str) -> pd.DataFrame:
    """Read Parquet file from GCS Silver layer into pandas DataFrame."""
    return pd.read_parquet(gcs_path)


### 2. Run Gate 4 Validation
## same as Gate 1, but different input data (Spark transformation results)
def run_gate4(
        df: pd.DataFrame,
        required_columns: list,
        expected_types: list,
        non_nullable_columns: list,
        suite_name: str,
        datasource_name: str,
        asset_name: str,
        batch_name: str,
        validation_name: str,
) -> dict:
    """
    Run Gate 4 — GE validation on Silver layer output.
    Same logic as Gate 1 but runs post-transform to catch Spark bugs.

    Args:
        df: DataFrame read from GCS Silver (Parquet).
        required_columns: Columns that must exist post-transform.
        expected_types: Dict mapping column names to expected types.
        non_nullable_columns: Columns that must not contain nulls.
        suite_name: GE suite name.
        datasource_name: GE datasource name.
        asset_name: GE asset name.
        batch_name: GE batch definition name.
        validation_name: GE validation definition name.

    Returns:
        Validation result dict with passed status and details.
    """

    ## Define Source Data
    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas(name=datasource_name)
    data_asset = data_source.add_dataframe_asset(name=asset_name)
    batch_definition = data_asset.add_batch_definition_whole_dataframe(name=batch_name)

    ## Create Suite
    suite = build_expectation_suite(
        required_columns,
        expected_types,
        non_nullable_columns,
        suite_name
    )
    suite = context.suites.add(suite)

    ## Validate Data
    # from Spark transformation results
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(
            name=validation_name,
            data=batch_definition,
            suite=suite,
        )
    )
    result = validation_definition.run(
                batch_parameters={"dataframe": df}
    )

    ## Result
    overall_passed = result.success
    status = "PASS" if overall_passed else "FAIL"
    print(f"Gate 4 [{status}] — {len(df)} records checked post-transform")

    failed_expectations = [r for r in result.results if not r.success]
    for r in failed_expectations:
        print(f"  FAIL: {r.expectation_config.type} — {r.expectation_config.kwargs}")

    ## Return
    return {
        "passed": overall_passed,
        "total_records": len(df),
        "failed_expectations": len(failed_expectations),
        "schema_passed": all(
            r.success for r in result.results
            if r.expectation_config.type == "expect_column_to_exist"
        ),
        "types_passed": all(
            r.success for r in result.results
            if r.expectation_config.type == "expect_column_values_to_be_in_type_list"
        ),
        "nulls_passed": all(
            r.success for r in result.results
            if r.expectation_config.type == "expect_column_values_to_not_be_null"
        ),
    }




## ===================== 2nd JOB =====================
## Load Silver Dataframe to BigQuery (Gold Layer)
## from silver layer to gold layer (afer passing validation)


def load_to_gold(
        silver_gcs_path: str,
        bq_project: str,
        bq_dataset: str,
        gold_table: str,
) -> dict:
    """
    Load Silver Parquet from GCS directly to BigQuery Gold table.

    Args:
        silver_gcs_path: GCS URI of Silver Parquet files.
        bq_project: BigQuery project ID.
        bq_dataset: BigQuery dataset name.
        gold_table: BigQuery table name for Gold layer.

    Returns:
        result_dict with load status and record count.
    """

    ## 1. Setup BigQuery
    client = bigquery.Client(project=bq_project)
    table_ref = f"{bq_project}.{bq_dataset}.{gold_table}"

    ## 2. Define Job Configuration
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    ## 3. Delete existing table first to ensure clean load
    try:
        client.delete_table(table_ref)
        print(f"  Existing table deleted")
    except Exception:
        pass  # table doesn't exist yet, fine

    ## 4. Write Table to BigQuery
    # BigQuery needs wildcard to read all part files
    bq_uri = silver_gcs_path.rstrip("/") + "/*.parquet"
    job = client.load_table_from_uri(bq_uri, table_ref, job_config=job_config)
    job.result()

    ## 5. Print Resulted Table
    table = client.get_table(table_ref)
    print(f"Gold load complete — {table.num_rows} records -> {table_ref}")


    return {
        "passed": True,
        "records_loaded": table.num_rows,
        "table": table_ref,
    }





## ===================== 3rd JOB =====================
## Save Quality History Data to BigQuery
## from: quality scoring Gate 3 results --> to: check for trend analyzer


def save_quality_history(
    score_result: QualityScoreResult,
    bq_project: str,
    bq_dataset: str,
    history_table: str,
) -> dict:
    """
    Gate 5 — Save quality score history to BigQuery.

    Args:
        score_result: QualityScoreResult from Gate 3.
        bq_project: BigQuery project ID.
        bq_dataset: BigQuery dataset name.
        history_table: BigQuery table name for quality history.

    Returns:
        result_dict with save status.
    """

    ## 1. Setup BigQuery
    client = bigquery.Client(project=bq_project)
    table_ref = f"{bq_project}.{bq_dataset}.{history_table}"

    ## 2. Write Record Data
    record = {
        "pipeline_name": score_result.pipeline_name,
        "scored_at": score_result.scored_at.isoformat(),
        "total_score": score_result.total_score,
        "status": score_result.status,
        "hard_block_triggered": score_result.hard_block_triggered or "",
        **{f"score_{k}": v for k, v in score_result.dimension_scores.items()},
    }
    history_df = pd.DataFrame([record])

    ## 3. Define Job Configuration
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
    )

    ## 4. Load Data to BigQuery
    job = client.load_table_from_dataframe(history_df, table_ref, job_config=job_config)
    job.result()

    print(f"Gate 5 [PASS] — Quality history saved -> {table_ref}")

    ## 5. return
    return {
        "passed": True,
        "table": table_ref,
    }
