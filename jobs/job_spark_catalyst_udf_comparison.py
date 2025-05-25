"""
Entry point script for Spark submission.

This script is the main entry point when submitting the job via spark-submit.
It delegates control to the Typer-based CLI in spark_catalyst_udf_comparison.cli.
"""


import sys

from spark_catalyst_udf_comparison.cli import main as cli_main

if __name__ == "__main__":
    sys.argv = ["job_spark_catalyst_udf_comparison.py"] + sys.argv[1:] 
    cli_main()