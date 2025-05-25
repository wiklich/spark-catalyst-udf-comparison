"""
Spark Utility Module

This module contains general purpose utilities for working with PySpark, including:
- Dataframe manipulation
- Metrics collection using sparkmeasure
"""

from typing import Union
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import pyspark.sql.functions as F
from sparkmeasure import StageMetrics, TaskMetrics
from spark_catalyst_udf_comparison.crypto_utils import encrypt_with_key, decrypt_with_key


def create_or_get_spark_session(app_name: str) -> SparkSession:
    """
    Creates or retrieves an existing SparkSession with the given application name.

    Args:
        app_name (str): Name of the Spark application.

    Returns:
        SparkSession: An active SparkSession instance.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()


def stop_spark_session(spark: SparkSession) -> None:
    """
    Stops the given SparkSession gracefully.

    Args:
        spark (SparkSession): The SparkSession to stop.
    """
    if spark.sparkContext._jsc is not None:
        spark.stop()


def register_python_udfs(spark: SparkSession):
    """
    Registers Python UDFs for encryption and decryption.

    Args:
        spark (SparkSession): Active SparkSession.
    """
    spark.udf.register("encrypt_with_key", encrypt_with_key, StringType())
    spark.udf.register("decrypt_with_key", decrypt_with_key, StringType())


def save_metrics(
    spark: SparkSession,
    execution_type: str,
    metrics: Union[StageMetrics, TaskMetrics],
    report_full_path: str,
    execution_number: int
) -> None:
    """
    Saves Spark stage or task metrics to Parquet format for later analysis.

    Args:
        spark (SparkSession): Active SparkSession.
        execution_type (str): Identifier for the type of execution.
        metrics (Union[StageMetrics, TaskMetrics]): Metrics object to save.
        report_full_path (str): Path where reports will be saved.
        execution_number (int): Sequential number for this execution.

    Raises:
        TypeError: If an unsupported metrics type is passed.
    """
    if not isinstance(metrics, (StageMetrics, TaskMetrics)):
        raise TypeError("metrics must be an instance of StageMetrics or TaskMetrics")

    if isinstance(metrics, StageMetrics):
        df_metrics = metrics.create_stagemetrics_DF("PerfStageMetrics")
        df_aggregated = metrics.aggregate_stagemetrics_DF("PerfStageMetrics")
    else:
        df_metrics = metrics.create_taskmetrics_DF("PerfTaskMetrics")
        df_aggregated = metrics.aggregate_taskmetrics_DF("PerfTaskMetrics")

    df_metrics = df_metrics.withColumn("app_name", F.lit(spark.sparkContext.appName)) \
        .withColumn("app_id", F.lit(spark.sparkContext.applicationId)) \
        .withColumn("execution_type", F.lit(execution_type)) \
        .withColumn("execution_number", F.lit(execution_number))

    df_aggregated = df_aggregated.withColumn("app_name", F.lit(spark.sparkContext.appName)) \
        .withColumn("app_id", F.lit(spark.sparkContext.applicationId)) \
        .withColumn("execution_type", F.lit(execution_type)) \
        .withColumn("execution_number", F.lit(execution_number))

    df_metrics.write.mode("append").parquet(f"{report_full_path}/full_report")
    df_aggregated.write.mode("append").parquet(f"{report_full_path}/aggregated_report")


def save_metrics_reports(
    spark: SparkSession,
    execution_type: str,
    reports_full_path: str,
    run_number: int,
    stagemetrics: StageMetrics,
    taskmetrics: TaskMetrics
) -> None:
    """
    Saves both stage and task metrics to their respective paths.

    Args:
        spark (SparkSession): Active SparkSession.
        execution_type (str): Identifier for the execution type.
        reports_full_path (str): Base path for saving reports.
        run_number (int): Execution iteration number.
        stagemetrics (StageMetrics): Stage-level metrics.
        taskmetrics (TaskMetrics): Task-level metrics.
    """
    save_metrics(spark, execution_type, stagemetrics, f"{reports_full_path}/stage/", run_number)
    save_metrics(spark, execution_type, taskmetrics, f"{reports_full_path}/task/", run_number)