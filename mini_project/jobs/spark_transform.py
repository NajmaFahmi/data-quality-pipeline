# FILE      : jpbs/spark_transform.py
# LIBRARY   : pyspark
# CONFIG    : input_df, output_gcs_path, deduplicate_columns — via function parameters
# __main__  : none
# CALLER    : run_pipeline.py


import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F



## 1. Start Spark Session
def get_spark_session(app_name: str, gcs_connector_jar: str, gcs_key_path: str) -> SparkSession:
    """Initialize or retrieve existing Spark session using JARV & Google Credentials."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars", gcs_connector_jar)
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopAbstractFileSystem")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcs_key_path)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )



## 2. Cleaning Data
def clean(df: DataFrame) -> DataFrame:
    """
    Clean string columns — trim whitespace and normalize empty strings to null.
    """
    for col_name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(
                col_name,
                F.when(F.trim(F.col(col_name)) == "", None)
                .otherwise(F.trim(F.col(col_name)))
            )

    return df 


## 3. Normalize Data Format
def normalize(df: DataFrame, date_columns: list) -> DataFrame:
    """
    Normalize data formats — cast date columns from string to DateType.
    """
    for col_name in date_columns:
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.to_date(F.col(col_name))
            )

    return df 


## 4. Drop Duplicates Data
def deduplicate(df: DataFrame, subset: list) -> DataFrame:
    """
    Remove duplicate rows based on subset columns.
    Keeps first occurrence, drops subsequent duplicates.
    """
    return df.dropDuplicates(subset)


## 5. Run Spark Transformation
def run_spark_transform(
    input_df: pd.DataFrame,
    output_gcs_path: str,
    deduplicate_columns: list,
    date_columns: list,
    app_name: str,
    gcs_connector_jar: str,
    gcs_key_path: str,
) -> dict:
    """
    Run Spark transform: clean, normalize, deduplicate, save to GCS Silver.

    Args:
        input_df: Pandas DataFrame from Gate 3.
        output_gcs_path: GCS path for Silver layer output (Parquet).
        dedup_columns: Columns to use for deduplication.
        date_columns: Columns to normalize as DateType.
        app_name: Spark application name.

    Returns:
        result_dict with record counts and output path.
    """

    # 1. create spark session
    spark = get_spark_session(app_name, gcs_connector_jar, gcs_key_path)

    # 2. turn pandas dataframe into spark dataframe
    spark_df = spark.createDataFrame(input_df)
    # before transformation
    input_count = spark_df.count()

    # 3. run transformation
    spark_df = clean(spark_df)
    spark_df = normalize(spark_df, date_columns)
    spark_df = deduplicate(spark_df, deduplicate_columns)
    # after transformation
    output_count = spark_df.count()

    # 4. write spark dataframe into parquet
    # and save to GCS silver layer
    spark_df.write.mode("overwrite").parquet(output_gcs_path)

    # 5. print result
    print(f"Spark transform complete — {input_count} in, {output_count} out")
    print(f"  Duplicates removed : {input_count - output_count}")
    print(f"  Output             : {output_gcs_path}")

    return {
        "passed": True,
        "input_records": input_count,
        "output_records": output_count,
        "duplicates_removed": input_count - output_count,
        "output_gcs_path": output_gcs_path,
    }
