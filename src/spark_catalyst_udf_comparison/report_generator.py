"""
Report Generator Module

Generates statistical reports from collected Spark metrics and job data.
Includes outlier removal and duration formatting utilities.
"""

from typing import List, Optional
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from spark_catalyst_udf_comparison.spark_history_client import load_dataframe_spark_jobs_from_history_server


def format_duration(col):
    """
    Formats a duration column into human-readable time units.

    Args:
        col (Column): A numeric column representing duration in milliseconds.

    Returns:
        Column: Formatted string with appropriate time unit.
    """
    return (
        F.when(col < 1000, F.concat(F.round(col, 1), F.lit(" ms")))  # Milliseconds 
        .when((col >= 1000) & (col < 60000), F.concat(F.round(col / 1000, 1), F.lit(" s")))  # Seconds
        .when((col >= 60000) & (col < 3600000), F.concat(F.round(col / 60000, 1), F.lit(" min")))  # Minutes
        .otherwise(F.concat(F.round(col / 3600000, 1), F.lit(" h")))  # Hours
    )


def _select_columns_for_metrics(df: DataFrame) -> DataFrame:
    """
    Selects the columns for metrics from the given DataFrame.

    Args:
        df (DataFrame): Input Spark DataFrame containing execution data.

    Returns:
        DataFrame: Filtered DataFrame with only relevant columns.
    """
    return df.select("execution_type", "duration")


def generate_comparison_report(
    spark: SparkSession,
    app_ids: List[str],
    report_path: str,
    spark_history_url: Optional[str] = None,
    warm_up_number: int = 5
) -> None:
    """
    Generates a comparison report between different execution types.

    Args:
        spark (SparkSession): Active SparkSession instance.
        app_ids (List[str]): Application IDs to compare.
        report_path (str): Base path for saving reports.
        spark_history_url (Optional[str]): URL of Spark History Server. If None, job metadata will be skipped.
        warm_up_number (int): Number of initial runs to ignore as warm-up.
    """
    df_report = spark.read.parquet(f"{report_path}/task/full_report*") \
        .filter(F.col("app_id").isin(app_ids)) \
        .filter(F.col("duration") >= 0) \
        .filter(F.col("execution_number") > warm_up_number)

    if spark_history_url:
        from spark_catalyst_udf_comparison.spark_history_client import load_dataframe_spark_jobs_from_history_server
        df_jobs = load_dataframe_spark_jobs_from_history_server(spark, spark_history_url, app_ids) \
            .filter(F.col("status") == "SUCCEEDED") \
            .filter(F.col("name").startswith("showString at"))
        df_final = df_report.join(df_jobs, on=["jobId", "app_id"])
    else:
        # Uses only the sparkmeasure generated data without additional information from the History Server
        df_final = df_report

    df_selected = _select_columns_for_metrics(df_final)        

    df_summary = df_selected.groupBy("execution_type") \
        .agg(
            F.mean("duration").cast("long").alias("avg_duration_ms"),
            F.expr("percentile_approx(duration, 0.5)").cast("long").alias("median_duration_ms"),
            F.stddev("duration").cast("long").alias("stddev_duration_ms")
        ) \
        .withColumn("avg_duration", format_duration(F.col("avg_duration_ms"))) \
        .withColumn("median_duration", format_duration(F.col("median_duration_ms"))) \
        .withColumn("stddev_duration", format_duration(F.col("stddev_duration_ms"))) \
        .select(
            "execution_type",
            "avg_duration",
            "median_duration",
            "stddev_duration"
        )

    df_summary.show(truncate=False)