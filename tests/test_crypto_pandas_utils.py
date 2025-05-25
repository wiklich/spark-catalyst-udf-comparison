"""
Unit tests for crypto_pandas_utils.py

This module validates encryption and decryption functions with Pandas Series,
and ensures UDFs can be created without actual Spark dependency.
"""

import pytest
import pandas as pd
import importlib
import sys


class MockStringType:
    def __init__(self):
        pass


def mock_pandas_udf(func, returnType=None):
    """Minimal mock of pandas_udf."""
    return func


@pytest.fixture
def mock_spark_dependencies(monkeypatch):
    """Mock Spark dependencies to avoid real Spark installation."""
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions.pandas_udf", mock_pandas_udf)
    monkeypatch.setitem(sys.modules, "pyspark.sql.types.StringType", MockStringType)


def test_udfs_are_defined(mock_spark_dependencies):
    """Test that UDFs are defined without triggering real Spark."""
    import spark_catalyst_udf_comparison.crypto_pandas_utils
    importlib.reload(spark_catalyst_udf_comparison.crypto_pandas_utils)

    from spark_catalyst_udf_comparison.crypto_pandas_utils import encrypt_with_key_udf, decrypt_with_key_udf

    assert encrypt_with_key_udf is not None
    assert decrypt_with_key_udf is not None


def test_encrypt_with_pandas_valid_input():
    from spark_catalyst_udf_comparison.crypto_pandas_utils import encrypt_with_pandas, decrypt_with_pandas

    input_series = pd.Series(["Alice", "Bob", "Charlie"])
    encrypted = encrypt_with_pandas(input_series)
    decrypted = decrypt_with_pandas(encrypted)

    assert all(decrypted.notna())
    assert all(decrypted == input_series)


def test_encrypt_with_pandas_handles_none():
    from spark_catalyst_udf_comparison.crypto_pandas_utils import encrypt_with_pandas

    input_series = pd.Series([None, "Bob", None])
    result = encrypt_with_pandas(input_series)

    assert result[0] is None
    assert result[2] is None
    assert result[1] is not None


def test_decrypt_with_pandas_handles_invalid_data():
    from spark_catalyst_udf_comparison.crypto_pandas_utils import decrypt_with_pandas

    input_series = pd.Series([None, "invalid_base64", "garbage_data"])
    result = decrypt_with_pandas(input_series)

    assert result[0] is None
    assert result[1] is None
    assert result[2] is None


def test_decrypt_with_pandas_invalid_input_types():
    from spark_catalyst_udf_comparison.crypto_pandas_utils import decrypt_with_pandas

    input_series = pd.Series([123, True, 3.14])
    result = decrypt_with_pandas(input_series)

    assert result[0] is None
    assert result[1] is None
    assert result[2] is None


def test_encrypt_decrypt_roundtrip():
    from spark_catalyst_udf_comparison.crypto_pandas_utils import encrypt_with_pandas, decrypt_with_pandas

    original = pd.Series(["Message A", "Message B", "Top Secret"])
    encrypted = encrypt_with_pandas(original)
    decrypted = decrypt_with_pandas(encrypted)

    assert all(decrypted.notna())
    assert (original == decrypted).all()