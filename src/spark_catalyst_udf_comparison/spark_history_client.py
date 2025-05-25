"""
Spark History Server Client Module

This module provides functions to fetch job data from the Spark History Server API.
"""

from typing import List
import requests
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from pyspark.sql import DataFrame, SparkSession


def get_job_schema() -> StructType:
    """
    Returns the schema for parsing job data from the Spark History Server.

    Returns:
        StructType: Schema definition for job data.
    """
    return StructType([
        StructField("jobId", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("submissionTime", StringType(), True),
        StructField("completionTime", StringType(), True),
        StructField("stageIds", ArrayType(IntegerType()), True),
        StructField("jobTags", ArrayType(StringType()), True),
        StructField("status", StringType(), True),
        StructField("numTasks", IntegerType(), True),
        StructField("numCompletedTasks", IntegerType(), True),
        StructField("numFailedTasks", IntegerType(), True),
        StructField("numKilledTasks", IntegerType(), True),
        StructField("numActiveTasks", IntegerType(), True),
        StructField("numCompletedIndices", IntegerType(), True),
        StructField("numActiveStages", IntegerType(), True),
        StructField("numCompletedStages", IntegerType(), True),
        StructField("numSkippedStages", IntegerType(), True),
        StructField("numFailedStages", IntegerType(), True),
        StructField("app_id", IntegerType(), True),
    ])


def load_dataframe_spark_jobs_from_history_server(
    spark: SparkSession,
    spark_history_server_url: str,
    apps_id: List[str]
) -> DataFrame:
    """
    Loads job data from the Spark History Server and returns it as a Spark DataFrame.

    Args:
        spark (SparkSession): Active SparkSession.
        spark_history_server_url (str): Base URL of the Spark History Server.
        apps_id (List[str]): List of application IDs to fetch job data for.

    Returns:
        DataFrame: Spark DataFrame containing job data.

    Raises:
        Exception: If a request fails or no data is returned.
    """
    job_schema = get_job_schema()
    APPS_ENDPOINT = "/api/v1/applications"

    df_final = spark.createDataFrame([], job_schema)
    for app_id in apps_id:
        jobs_url = f"{spark_history_server_url}{APPS_ENDPOINT}/{app_id}/jobs"

        response = requests.get(jobs_url)
        if response.status_code != 200:
            raise Exception(f"API error {response.status_code}: {response.text}")

        jobs_data = response.json()
        if not jobs_data:
            continue

        df = spark.createDataFrame(jobs_data, schema=job_schema).withColumn("app_id", F.lit(app_id))
        if df is not None and not df.rdd.isEmpty():
            df_final = df_final.union(df)

    return df_final