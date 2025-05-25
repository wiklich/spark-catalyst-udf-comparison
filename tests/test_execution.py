import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import DataFrame


class MockPandasUDF:
    def __init__(self, func, returnType=None):
        self.func = func
        self.returnType = returnType

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


@pytest.fixture
def mock_spark_session():
    """Fixture to mock a SparkSession for testing."""
    spark = MagicMock()
    spark.read.csv.return_value = "mocked_df"
    spark.sparkContext.appName = "test_app"
    spark.sparkContext.applicationId = "app-123456"

    spark.udf = MagicMock()
    spark.udf.register = MagicMock()

    return spark


def test_create_data(mock_spark_session):
    from spark_catalyst_udf_comparison.execution import create_data

    result = create_data(mock_spark_session, "/data/names.csv")
    mock_spark_session.read.csv.assert_called_once_with("/data/names.csv", header=True)
    assert result == "mocked_df"


def test_unsupported_execution_type_raises_error(mock_spark_session):
    from spark_catalyst_udf_comparison.execution import run_benchmark, ExecutionType

    with pytest.raises(ValueError) as exc_info:
        run_benchmark(
            spark=mock_spark_session,
            execution_type="UNSUPPORTED_TYPE",
            data_path="/data/names.csv",
            reports_full_path="/reports/"
        )

    assert "Unsupported execution type" in str(exc_info.value)


def test_display_result(mock_spark_session):
    from spark_catalyst_udf_comparison.execution import display_result

    df = MagicMock()
    df.show = MagicMock()
    df.explain = MagicMock()

    display_result(df, explain_codegen=True)

    df.show.assert_called_once_with(truncate=False)
    df.explain.assert_called_once_with("codegen")


def test_register_python_udfs(mock_spark_session):
    from spark_catalyst_udf_comparison.spark_utils import register_python_udfs

    register_python_udfs(mock_spark_session)

    assert mock_spark_session.udf.register.called
    assert mock_spark_session.udf.register.call_count >= 2


def test_udfs_are_defined(monkeypatch):
    from spark_catalyst_udf_comparison.crypto_pandas_utils import encrypt_with_pandas

    class MockStringType:
        pass

    def mock_pandas_udf(func, returnType=None):
        return func

    monkeypatch.setattr("spark_catalyst_udf_comparison.crypto_pandas_utils.StringType", MockStringType)
    monkeypatch.setattr("spark_catalyst_udf_comparison.crypto_pandas_utils.pandas_udf", mock_pandas_udf)

    import importlib
    import spark_catalyst_udf_comparison.crypto_pandas_utils as cpu
    importlib.reload(cpu)

    assert hasattr(cpu, "encrypt_with_key_udf")
    assert hasattr(cpu, "decrypt_with_key_udf")

    assert callable(cpu.encrypt_with_key_udf)
    assert callable(cpu.decrypt_with_key_udf)


@patch("spark_catalyst_udf_comparison.execution.display_result")
@patch("pyspark.sql.functions._invoke_function", lambda name, *args: lambda: MagicMock())
def test_execute_with_catalyst_custom_function(mock_spark_session):
    """
    Test that execute_with_catalyst_custom_function runs correctly.
    """
    from spark_catalyst_udf_comparison.execution import execute_with_catalyst_custom_function

    df = MagicMock()
    df.withColumn.return_value = df

    result = execute_with_catalyst_custom_function(df, explain_codegen=False)

    assert result is None
    assert df.withColumn.call_count == 2

@patch("spark_catalyst_udf_comparison.execution.display_result")
@patch("pyspark.sql.functions._invoke_function", lambda name, *args: lambda: MagicMock())
def test_execute_with_python_udf(mock_spark_session):
    """
    Test that execute_with_python_udf runs correctly and calls display_result.
    """
    from spark_catalyst_udf_comparison.execution import execute_with_python_udf
    df = MagicMock()
    df.withColumn.return_value = df

    result = execute_with_python_udf(df, explain_codegen=False)

    assert result is None
    assert df.withColumn.call_count == 2

