"""
Execution Logic Module

Contains core logic for running benchmarks and collecting performance metrics.
"""

from typing import List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from sparkmeasure import StageMetrics, TaskMetrics
from spark_catalyst_udf_comparison.config import ExecutionType


def display_result(result_df: DataFrame, explain_codegen: bool = False) -> None:
    """
    Displays the result of a Spark operation and optionally shows the codegen plan.

    Args:
        result_df (DataFrame): The resulting DataFrame to display.
        explain_codegen (bool): If True, prints the code generation plan.

    Returns:
        None
    """
    result_df.show(truncate=False)
    if explain_codegen:
        result_df.explain("codegen")

def execute_with_catalyst_custom_function(df: DataFrame, explain_codegen: bool = False) -> None:
    """
    Executes Catalyst-based custom function for encryption and decryption.

    Args:
        df (DataFrame): Input DataFrame containing data to process.
        explain_codegen (bool): Whether to show codegen explanation.
    """
    result = df.withColumn("encrypted", F.expr("encrypt_with_key(name)")) \
        .withColumn("decrypted", F.expr("decrypt_with_key(encrypted)"))
    display_result(result, explain_codegen)


def execute_with_python_udf(df: DataFrame, explain_codegen: bool = False) -> None:
    """
    Executes Python UDF for encryption and decryption.

    Args:
        df (DataFrame): Input DataFrame containing data to process.
        explain_codegen (bool): Whether to show codegen explanation.
    """
    result = df.withColumn("encrypted", F.expr("encrypt_with_key(name)")) \
        .withColumn("decrypted", F.expr("decrypt_with_key(encrypted)"))
    display_result(result, explain_codegen)


def execute_with_pandas_udf(df: DataFrame, explain_codegen: bool = False) -> None:
    """
    Executes Pandas UDF for encryption and decryption.

    Args:
        df (DataFrame): Input DataFrame containing data to process.
        explain_codegen (bool): Whether to show codegen explanation.
    """
    from spark_catalyst_udf_comparison.crypto_pandas_utils import encrypt_with_key_udf, decrypt_with_key_udf
        
    result = df.withColumn("encrypted", encrypt_with_key_udf(F.col("name"))) \
        .withColumn("decrypted", decrypt_with_key_udf(F.col("encrypted")))
    display_result(result, explain_codegen)


def execute_read_reports(
    spark: SparkSession,
    app_ids: List[str],
    spark_history_url: Optional[str],
    report_path: str,
    warm_up_number: int = 5
) -> None:
    """
    Generates and displays a comparison report from previous executions.

    Args:
        spark (SparkSession): Active SparkSession instance.
        app_ids (List[str]): List of application IDs to compare.
        spark_history_url (Optional[str]): URL of the Spark History Server. If None, job metadata will be skipped.
        report_path (str): Path where reports will be saved/generated.
        warm_up_number (int): Number of initial runs to ignore as warm-up.
    """
    from spark_catalyst_udf_comparison.report_generator import generate_comparison_report    
    generate_comparison_report(
        spark=spark,
        app_ids=app_ids,
        report_path=report_path,
        spark_history_url=spark_history_url,        
        warm_up_number=warm_up_number
    )


def run_benchmark(
    spark: SparkSession,
    execution_type: ExecutionType,
    data_path: str,
    reports_full_path: str,
    num_runs: int = 5,
    explain_codegen: bool = False
) -> None:
    """
    Runs a benchmark for the given execution type and collects performance metrics.

    Args:
        spark (SparkSession): Active SparkSession instance.
        execution_type (ExecutionType): Type of execution to perform.
        data_path (str): Path to the input CSV file.
        reports_full_path (str): Path to store collected metrics.
        num_runs (int): Number of times to run the benchmark.
        explain_codegen (bool): Whether to show codegen explanation.
    """
    from spark_catalyst_udf_comparison.metrics_collector import save_metrics_reports
    
    df = create_data(spark, data_path)

    executor_map = {
        ExecutionType.CATALYST: execute_with_catalyst_custom_function,
        ExecutionType.PYTHON_UDF: execute_with_python_udf,
        ExecutionType.PANDAS_UDF: execute_with_pandas_udf
    }

    executor = executor_map.get(execution_type)
    if not executor:
        raise ValueError(f"Unsupported execution type: {execution_type}")

    for i in range(num_runs):
        run_number = i + 1
        print(f"Execution {run_number}/{num_runs}")
        stagemetrics = StageMetrics(spark)
        taskmetrics = TaskMetrics(spark)

        stagemetrics.begin()
        taskmetrics.begin()

        try:
            executor(df, explain_codegen)
            stagemetrics.end()
            taskmetrics.end()
            stagemetrics.print_report()
            taskmetrics.print_report()
            save_metrics_reports(spark, execution_type.name, reports_full_path, run_number, stagemetrics, taskmetrics)
        except Exception as e:
            print(f"Error during execution {i+1}: {str(e)}")


def create_data(spark, data_path: str) -> DataFrame:
    """
    Creates a DataFrame by reading from a CSV file.

    Args:
        spark (SparkSession): Active SparkSession instance.
        data_path (str): Path to the input CSV file.

    Returns:
        DataFrame: Loaded dataset.
    """
    return spark.read.csv(data_path, header=True)