"""
Unit tests for the CLI module.

These tests validate that the Typer CLI commands work as expected:
- The 'run' command parses arguments correctly and shows proper errors.
- The 'compare' command handles required parameters.
- Invalid input is handled with appropriate error messages.
"""

import pytest
from typer.testing import CliRunner
from spark_catalyst_udf_comparison.cli import app
from unittest.mock import patch


runner = CliRunner(mix_stderr=False)


def test_cli_run_help():
    """
    Test that running 'spark-catalyst run --help' shows the help message.
    """
    result = runner.invoke(app, ["run", "--help"])
    assert result.exit_code == 0
    assert "Usage:" in result.stdout
    assert "Run a benchmark with the specified execution type." in result.stdout

@pytest.mark.parametrize("execution_type", ["CATALYST", "PYTHON_UDF", "PANDAS_UDF"])
def test_cli_run_missing_data_path_parameter(execution_type):
    """
    Test that missing required params like data-path and report-path raise errors.
    """
    result = runner.invoke(app, ["run", execution_type])
    assert result.exit_code == 2
    assert "Missing option '--data-path'" in result.stderr


@pytest.mark.parametrize("execution_type", ["CATALYST", "PYTHON_UDF", "PANDAS_UDF"])
def test_cli_run_missing_report_path_parameter(execution_type):
    """
    Test that missing required params like data-path and report-path raise errors.
    """
    result = runner.invoke(
        app,
        ["run", "INVALID_TYPE", "--data-path", "/data/names.csv"]
    )
    assert result.exit_code == 2
    assert "Missing option '--report-path'" in result.stderr    


def test_cli_run_invalid_execution_type():
    """
    Test that an invalid execution_type raises an error and displays valid options.
    """
    result = runner.invoke(
        app,
        ["run", "INVALID_TYPE", "--data-path", "/data/names.csv", "--report-path", "/reports"]
    )
    assert result.exit_code == 1
    assert "Invalid execution type" in result.stdout or "Invalid execution type" in result.stderr
    assert "Valid options are: CATALYST, PYTHON_UDF, PANDAS_UDF" in result.stdout or result.stderr


def test_cli_run_valid_execution_types():
    """
    Test that valid execution types are accepted and show correct error about missing files.
    """
    for execution_type in ["CATALYST", "PYTHON_UDF", "PANDAS_UDF"]:
        result = runner.invoke(
            app,
            [
                "run", execution_type,
                "--data-path", "/data/names.csv",
                "--report-path", "/reports"
            ],
        )
        
        assert result.exit_code != 0
        assert "Running benchmark..." not in result.stdout


def test_cli_compare_help():
    """
    Test that 'spark-catalyst compare --help' shows usage instructions.
    """
    result = runner.invoke(app, ["compare", "--help"])
    assert result.exit_code == 0
    assert "Usage:" in result.stdout
    assert "Generate comparison report from multiple executions." in result.stdout


def test_cli_compare_missing_app_ids():
    """
    Test that 'compare' fails when no app IDs are provided.
    """
    result = runner.invoke(app, ["compare"])
    assert result.exit_code == 2
    assert "Missing argument 'APP_IDS...'" in result.stderr or "Missing argument 'APP_IDS'" in result.stderr


def test_cli_compare_missing_report_path_parameter():
    """
    Test that 'compare' fails when no app IDs are provided.
    """
    result = runner.invoke(app, ["compare", "app-XXX-0105"])
    assert result.exit_code == 2
    assert "Missing option '--report-path'" in result.stderr    


@patch("spark_catalyst_udf_comparison.cli.execute_read_reports")
def test_cli_compare_with_spark_history_url(mock_execute_read_reports):
    """
    Test that 'compare' accepts '--spark-history-url'.
    """
    result = runner.invoke(
        app,
        [
            "compare",
            "app-XXX-0105",
            "app-YYY-0106",
            "--spark-history-url", "http://history-server:18080",
            "--report-path", "/reports"
        ],
    )
    assert result.exit_code == 0
    assert mock_execute_read_reports.called


def test_cli_compare_warm_up_number_default():
    """
    Test that warm-up-number defaults to 5 when not provided.
    """
    result = runner.invoke(
        app,
        [
            "compare",
            "app-20250522230757-0105",
            "app-20250522230853-0106",
            "--report-path", "/reports"
        ],
    )
    result = runner.invoke(app, ["compare", "--help"])
    assert result.exit_code == 0
    assert "--warm-up-number" in result.stdout
    assert "default: 5" in result.stdout


def test_cli_main_function():
    """
    Test that the main() function calls the Typer app.
    """
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "Commands" in result.stdout
    assert "run       Run a benchmark" in result.stdout
    assert "compare   Generate comparison report" in result.stdout


def test_cli_run_benchmark_with_all_params():
    """
    Test that all parameters are parsed correctly in 'run' command.
    """
    result = runner.invoke(
        app,
        [
            "run", "CATALYST",
            "--data-path", "/data/names.csv",
            "--report-path", "/reports",
            "--num-runs", "3",
            "--explain-codegen"
        ],
    )
    assert result.exit_code != 0
    assert "Running benchmark..." not in result.stdout


def test_cli_run_invalid_data_path():
    """
    Test that passing an invalid data path doesn't break parsing.
    """
    result = runner.invoke(
        app,
        [
            "run", "CATALYST",
            "--data-path", "invalid/path/to/file.csv",
            "--report-path", "/reports"
        ],
    )
    assert result.exit_code != 0
    assert "Running benchmark..." not in result.stdout

