# FILE      : gates/gate1_validator.py
# LIBRARY   : great_expectations (GE 1.20)
# CONFIG    : required_columns, expected_types, non_nullable_columns,
#             pipeline_name, datasource_name, asset_name,
#             batch_name, validation_name — all via parameter
# __main__  : none
# CALLER    : run_pipeline.py


import pandas as pd
import great_expectations as gx
from google.cloud import storage
from io import StringIO
from typing import Optional



### 1. Read CSV from GCS
def read_csv_from_gcs(bucket_name: str, blob_path: str) -> pd.DataFrame:
    """Download a CSV from GCS and return as DataFrame."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    content = blob.download_as_text()
    return pd.read_csv(StringIO(content))



### 2. Build GE Expectation Suite
# check schema, column types, and nulls
def build_expectation_suite(
        required_columns: list,
        expected_types: dict,
        non_nullable_columns: list,
        suite_name: Optional[str] = "gate1_suite",
) -> gx.ExpectationSuite:
    """
    Build a GE ExpectationSuite from config parameters.

    Args:
        required_columns: Columns that must exist in the dataset.
        expected_types: Dict mapping column names to 'numeric' or 'string'.
        non_nullable_columns: Columns that must not contain nulls.
        suite_name: GE suite name.

    Returns:
        Configured GE ExpectationSuite.
    """

    suite = gx.ExpectationSuite(name=suite_name)

    ## Schema Check
    for col in required_columns:
        suite.add_expectation(
            gx.expectations.ExpectColumnToExist(column=col)
        )

    ## Column Type Check
    for col, expected_type in expected_types.items():
        # numeric columns
        if expected_type == "numeric":
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToBeInTypeList(
                    column=col,
                    type_list=["int64", "float64", "int32", "float32"],
                )
            )
        # string column
        elif expected_type == "string":
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToBeInTypeList(
                    column=col,
                    type_list=["object", "string", "str"],
                )
            )


    ## Null Check
    for col in non_nullable_columns:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column=col)
        )


    return suite



### 3. Run GE Suite
def run_gate1(
    bucket_name: str,
    blob_path: str,
    required_columns: list,
    expected_types: dict,
    non_nullable_columns: list,
    suite_name: Optional[str] = "gate1_suite",
    datasource_name: Optional[str] = "datasource",
    asset_name: Optional[str] = "dataframe_asset",
    batch_name: Optional[str] = "whole_dataframe_batch",
    validation_name: Optional[str] = "gate1_validation",
) -> dict:
    """
    Run Gate 1 validation using Great Expectations on a CSV from GCS.

    Args:
        bucket_name: GCS bucket name.
        blob_path: Path to the CSV file inside the bucket.
        required_columns: Columns that must exist.
        expected_types: Dict mapping column names to expected types.
        non_nullable_columns: Columns that must not contain nulls.

        suite_name: GE suite name.
        datasource_name: GE datasource name.
        asset_name: GE asset name.
        batch_name: GE batch definition name.
        validation_name: GE validation definition name.

    Returns:
        Validation result with overall passed status and GE results detail.
    """

    ## 1. Read CSV (bronze layer) from GCS
    df = read_csv_from_gcs(bucket_name, blob_path)


    ## 2. Define Source Data
    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas(name=datasource_name)
    data_asset = data_source.add_dataframe_asset(name=asset_name)
    batch_definition = data_asset.add_batch_definition_whole_dataframe(name=batch_name)


    ## 3. Create Suite
    suite = build_expectation_suite(
        required_columns,
        expected_types,
        non_nullable_columns,
        suite_name
    )
    suite = context.suites.add(suite)


    ## 4. Validate Data
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


    ## 5. Result
    overall_passed = result.success

    status = "PASS" if overall_passed else "FAIL"
    print(f"Gate 1 [{status}] — {len(df)} records checked")

    failed_expectations = [
        r for r in result.results if not r.success
    ]
    for r in failed_expectations:
        print(f"  FAIL: {r.expectation_config.type} — {r.expectation_config.kwargs}")


    ## 6. Return
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
        "details": [...],
    }
