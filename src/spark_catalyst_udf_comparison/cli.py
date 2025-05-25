"""
Command Line Interface Module

This module provides a CLI interface using Typer for running benchmarks and generating reports.
"""

import typer
from typing import List
from spark_catalyst_udf_comparison.config import ExecutionType, DEFAULT_NUM_RUNS, get_app_name
from spark_catalyst_udf_comparison.execution import run_benchmark, execute_read_reports
from spark_catalyst_udf_comparison.spark_utils import create_or_get_spark_session, stop_spark_session, register_python_udfs


app = typer.Typer(
    rich_markup_mode="rich",
    help="""
        [b][#006400]Script to demonstrate benchmark using:[/][/b]\n
        - [dim][#006400]Catalyst Custom Functions (Scala)[/][/dim]
        - [dim][#006400]Python UDF[/][/dim]
        - [dim][#006400]Pandas UDF[/][/dim]

        [b][#006400]It will generate reports for each execution using sparkmeasure features.[/][/b]\n
        [#ffcb00]Thanks to LucaCanali for sparkmeasure![/]
        [#ffcb00]Visit: [underline blue]https://github.com/LucaCanali/sparkMeasure[/][/]
        Please [blink][b]star[/b][/] it if you like it :)

        [#00ffff]Listenin' to Mega Drive - Converter ♪♬♩♫♭♩[/]
        [#14b8b8]Check the track: [underline]https://www.youtube.com/watch?v=EeJv3aSVJoM[/][/]
        [#ffd700]More from Mega Drive: [underline]https://www.youtube.com/channel/UC4Q6jc2ZeHmcg2Cv4kYsyEg[/][/]
        [#fefe00]Thanks for this banger, man!!![/]

        [b][#1c0031]Author:[/][/b] ☠ ☠ ☠  [#00ff00]RafHellRaiser from Wik-Lich ha-K-Nitzotzot Maradot[/] ☠ ☠ ☠
    """,
)


@app.command(name="run")
def cli_run(
    execution_type: str = typer.Argument(
        ..., help=f"Type of execution. Valid options: {', '.join(e.value for e in ExecutionType)}."
    ),
    data_path: str = typer.Option(
        ..., "--data-path", "-d",
        help="Path to input CSV file containing names (e.g., /opt/bitnami/spark/jobs/app/names.csv)."
    ),
    report_path: str = typer.Option(..., "--report-path", "-r", help="Path where metrics will be saved."),
    num_runs: int = typer.Option(DEFAULT_NUM_RUNS, "--num-runs", "-n", help="Number of runs per execution type."),
    explain_codegen: bool = typer.Option(False, "--explain-codegen", help="Show codegen plan for execution.")
):
    """
    Run a benchmark with the specified execution type.
    """
    try:
        execution_enum = ExecutionType.from_string(execution_type)
    except ValueError as e:
        typer.secho(str(e), fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    spark = create_or_get_spark_session(get_app_name(execution_enum.name))

    try:
        if execution_enum == ExecutionType.PYTHON_UDF:
            register_python_udfs(spark)
        run_benchmark(spark, execution_enum, data_path, report_path, num_runs, explain_codegen)
    finally:
        stop_spark_session(spark)


@app.command(name="compare")
def cli_compare(
    app_ids: List[str] = typer.Argument(
        ..., help="List of Spark application IDs to compare. Example: app-20250000000000-0105 app-20250000000000-0106"
    ),
    spark_history_url: str = typer.Option(
        None, "--spark-history-url", "-u",
        help="URL of Spark History Server (optional). If not provided, job metadata will not be loaded."
    ),
    report_path: str = typer.Option(
        ..., "--report-path", "-r", help="Path where final reports will be saved."
    ),
    warm_up_number: int = typer.Option(
        5, "--warm-up-number", "-w",
        help="Number of initial runs to ignore as warm-up (default: 5)."
    ),
):
    """
    Generate comparison report from multiple executions.

    The user must provide a list of Spark application IDs to compare.
    """
    spark = create_or_get_spark_session(get_app_name("Report Comparison"))

    try:
        execute_read_reports(spark, app_ids, spark_history_url, report_path, warm_up_number)
    finally:
        stop_spark_session(spark)

def main():
    """
    Entry point for direct execution via Python, CLI or PySpark (e.g., spark-submit).
    """
    app()

if __name__ == "__main__":
    main()        