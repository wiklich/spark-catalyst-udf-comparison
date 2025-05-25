"""
Unit tests for config.py

This file validates the functionality of the ExecutionType enum and get_app_name function.
"""

import pytest
from spark_catalyst_udf_comparison.config import ExecutionType, get_app_name


def test_execution_type_enum_values():
    """
    Test that each ExecutionType has the expected string value.
    """
    assert ExecutionType.CATALYST.value == "CATALYST"
    assert ExecutionType.PYTHON_UDF.value == "PYTHON_UDF"
    assert ExecutionType.PANDAS_UDF.value == "PANDAS_UDF"


def test_execution_type_from_string_valid():
    """
    Test that valid strings are correctly converted to ExecutionType enum.
    """
    assert ExecutionType.from_string("CATALYST") == ExecutionType.CATALYST        
    assert ExecutionType.from_string("PYTHON_UDF") == ExecutionType.PYTHON_UDF    
    assert ExecutionType.from_string("PANDAS_UDF") == ExecutionType.PANDAS_UDF


def test_execution_type_from_string_invalid():
    """
    Test that invalid strings raise a ValueError with correct message.
    """
    with pytest.raises(ValueError) as exc_info:
        ExecutionType.from_string("invalid_type")

    assert "Invalid execution type: invalid_type" in str(exc_info.value)
    assert "Valid options are: CATALYST, PYTHON_UDF, PANDAS_UDF" in str(exc_info.value)


def test_execution_type_from_string_case_insensitive():
    """
    Test that from_string() is case-insensitive.
    """
    assert ExecutionType.from_string("catalyst") == ExecutionType.CATALYST
    assert ExecutionType.from_string("Catalyst") == ExecutionType.CATALYST
    assert ExecutionType.from_string("pandas_Udf") == ExecutionType.PANDAS_UDF    


def test_get_app_name():
    """
    Test that get_app_name returns the expected formatted application name.
    """
    result = get_app_name("CATALYST")
    assert result == "Catalyst Custom Function x UDF functions HSM GCM [CATALYST]"

    result = get_app_name("PYTHON_UDF")
    assert result == "Catalyst Custom Function x UDF functions HSM GCM [PYTHON_UDF]"

    result = get_app_name("PANDAS_UDF")
    assert result == "Catalyst Custom Function x UDF functions HSM GCM [PANDAS_UDF]"


def test_get_app_name_with_special_characters():
    """
    Test that get_app_name handles special characters or spaces.
    """
    result = get_app_name("CustomType")
    assert result == "Catalyst Custom Function x UDF functions HSM GCM [CustomType]"