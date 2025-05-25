import pytest
from unittest.mock import MagicMock, patch


@pytest.fixture
def mock_spark_session():
    with patch("spark_catalyst_udf_comparison.spark_utils.create_or_get_spark_session") as mock:
        yield mock        


@pytest.fixture
def mock_spark_session():
    spark = MagicMock()
    spark.read.csv.return_value = MagicMock()
    spark.sparkContext.appName = "test_app"
    spark.sparkContext.applicationId = "app-1234567890"
        
    spark.udf = MagicMock()
    spark.udf.register = MagicMock()
    spark.udf.pandas_udf = MagicMock(return_value=lambda x: x)

    return spark


@pytest.fixture
def mock_dataframe():
    df = MagicMock()
    df.withColumn.return_value = df
    df.show = MagicMock()
    df.explain = MagicMock()
    return df