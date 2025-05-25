"""
Global Configuration Module

This module contains global constants and configuration values used throughout the project.
"""

from enum import Enum


class ExecutionType(str, Enum):
    """
    Enumeration of supported execution types for benchmarking.

    Attributes:
        CATALYST: Catalyst-based custom UDFs (Scala).
        PYTHON_UDF: Regular Python UDFs.
        PANDAS_UDF: Vectorized Pandas UDFs.
    """

    CATALYST = "CATALYST"
    PYTHON_UDF = "PYTHON_UDF"
    PANDAS_UDF = "PANDAS_UDF"

    @classmethod
    def from_string(cls, value: str) -> "ExecutionType":
        """
        Converts a string to an ExecutionType enum.

        Args:
            value (str): The name of the execution type.

        Returns:
            ExecutionType: Corresponding enum value.

        Raises:
            ValueError: If the input is not a valid execution type.
        """
        try:
            return cls[value.upper()]
        except KeyError:
            valid_types = ", ".join(e.name for e in cls)
            raise ValueError(f"Invalid execution type: {value}. Valid options are: {valid_types}")


DEFAULT_NUM_RUNS = 20

def get_app_name(execution_type: str) -> str:
    """
    Generates a Spark application name based on the execution type.

    Args:
        execution_type (str): The execution type.

    Returns:
        str: Formatted application name.
    """
    return f"Catalyst Custom Function x UDF functions HSM GCM [{execution_type}]"