"""
Metrics Collector Module

Handles the collection and persistence of Spark execution metrics using sparkmeasure.
Supports both StageMetrics and TaskMetrics for performance analysis.
"""

from typing import Union
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from sparkmeasure import StageMetrics, TaskMetrics


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
        spark (SparkSession): Active SparkSession instance.
        execution_type (str): Type of execution (e.g., CATALYST, PYTHON_UDF).
        metrics (Union[StageMetrics, TaskMetrics]): Metrics object to be saved.
        report_full_path (str): Base path where metrics will be stored.
        execution_number (int): Sequential run number for this benchmark iteration.

    Raises:
        TypeError: If an unsupported metrics type is provided.
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
    Saves both stage and task metrics to their respective directories.

    Args:
        spark (SparkSession): Active SparkSession instance.
        execution_type (str): Type of execution (e.g., CATALYST, PYTHON_UDF).
        reports_full_path (str): Base path where reports will be stored.
        run_number (int): Execution iteration number.
        stagemetrics (StageMetrics): Collected stage-level metrics.
        taskmetrics (TaskMetrics): Collected task-level metrics.
    """
    save_metrics(spark, execution_type, stagemetrics, f"{reports_full_path}/stage/", run_number)
    save_metrics(spark, execution_type, taskmetrics, f"{reports_full_path}/task/", run_number)