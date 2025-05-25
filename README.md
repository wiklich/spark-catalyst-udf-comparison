# Spark Catalyst vs UDF Benchmark

This project compares the performance of different encryption execution strategies in PySpark:
- **Catalyst Custom Functions**
- **Python UDFs**
- **Pandas UDFs**

It uses `sparkmeasure` to collect stage and task metrics and generate reports for benchmark comparison. The goal is to evaluate which approach offers better performance when applying cryptographic transformations in Spark jobs.

## Features

- AES-GCM encryption and decryption with preloaded key
- Pandas UDF
- Python UDF
- Catalyst custom function integration via Scala extension
- CLI interface for running benchmarks or generating reports
- Unit tests without real Spark dependency (fully mocked)
- Metrics collection using [sparkMeasure](https://github.com/LucaCanali/sparkMeasure)
- Packaging script (`build_zip.py`) to prepare distribution for Docker or Spark submission

---

## Requirements

To run locally:

- Python 3.8+
- PySpark 3.5.x
- Poetry or pip
- Optional: Java 8 or 11 (for local Spark runs)

For Docker environment:

- Docker
- Docker Compose

---

## Installation

### Using Pip

```bash
git clone https://github.com/wiklich/spark-catalyst-udf-comparison.git
cd spark-catalyst-udf-comparison
pip install -r requirements.txt
```

### Using Poetry

```bash
git clone https://github.com/wiklich/spark-catalyst-udf-comparison.git
cd spark-catalyst-udf-comparison
poetry install
```

---

## Running Unit Tests

All unit tests mock Spark dependencies and avoid requiring Spark or PyArrow during execution.
Some unit tests have not yet been created, but they will be created in the future.

```bash
poetry run pytest tests/ -v
```

With coverage:

```bash
poetry run pytest tests/ -v --cov=spark_catalyst_udf_comparison --cov-report term-missing
```

Open `htmlcov/index.html` for detailed coverage report.

---

## Project Structure

```
spark-catalyst-udf-comparison/
├── dist/
│   ├── crypto_udfs.zip
│   └── job_spark_catalyst_udf_comparison.py
│       # Copied here by build_zip.py
│
├── jobs/
│   └── job_spark_catalyst_udf_comparison.py
│       # Entry point for Spark jobs (used during spark-submit)
│
├── scripts/
│   └── build_zip.py
│       # Builds the .zip package containing all UDFs and utilities
|
├── src/
│   └── spark_catalyst_udf_comparison/
│       ├── __init__.py
│       ├── cli.py
│       ├── config.py
│       ├── crypto_config.py
│       ├── crypto_pandas_utils.py
│       ├── crypto_utils.py
│       ├── execution.py
│       ├── hsm_simulator.py
│       ├── metrics_collector.py
│       ├── report_generator.py
│       ├── spark_history_client.py
│       └── spark_utils.py
|
├── tests/
│   ├── conftest.py
│   ├── test_cli.py
│   ├── test_config.py
│   ├── test_crypto_config.py
│   ├── test_crypto_pandas_utils.py
│   ├── test_crypto_utils.py
│   ├── test_execution.py
│   └── test_hsm_simulator.py
├── pyproject.toml
├── README.md
└── requirements.txt    

```

---

## Purpose of `build_zip.py`

The `scripts/build_zip.py` script prepares your code for deployment, especially when submitting jobs to a Spark cluster (e.g., inside Docker).

### What it does:

1. **Packages all Python UDFs and utility modules** from `src/spark_catalyst_udf_comparison` into a `.zip` file.
2. **Copies the main entrypoint script** (`jobs/job_spark_catalyst_udf_comparison.py`) to the `dist/` directory.

### Resulting structure after running:

```
dist/
├── crypto_udfs.zip          # Contains all Python UDFs and utils
└── job_spark_catalyst_udf_comparison.py  # Main script used by spark-submit
```

This allows you to submit jobs like this:

```bash
spark-submit \
  --py-files dist/crypto_udfs.zip \
  dist/job_spark_catalyst_udf_comparison.py \
  run CATALYST \
  --data-path /opt/bitnami/spark/jobs/app/names.csv \
  --report-path /opt/bitnami/spark/jobs/app/sparkmeasure_reports/
```

---

## Spark Catalyst Extension JAR

This project depends on a **Spark Catalyst extension** implemented in Scala, available in the following repository:

[wiklich/spark-catalyst-hsm-example](https://github.com/wiklich/spark-catalyst-hsm-example)

This extension provides optimized cryptographic functions via **Catalyst Custom Expressions** (e.g., `encrypt_with_key(name)`) and must be compiled into a `.jar` file before execution.  

---

## Docker Setup

Use your own `docker-compose.yml` to simulate a full Spark cluster.

### Build and Start the Environment

```bash
docker-compose up -d
```

Wait until all services are up and running.

### Prepare Your Code for Submission

Run the build script to create the `.zip` package:

```bash
python scripts/build_zip.py
```

### Copy Files to Container

```bash
docker cp dist/crypto_udfs.zip spark-master:/opt/bitnami/spark/jobs/app/crypto_udfs.zip
docker cp dist/job_spark_catalyst_udf_comparison.py spark-master:/opt/bitnami/spark/jobs/app/
```

### Submit Job Inside Docker Container

You can run benchmarks for Catalyst, Pandas UDF and Python UDF. Following is an example how to run benchmark for Custom Function (Catalyst):

```bash
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.extensions=br.com.wiklich.security.crypto.CryptoExtension \
  /opt/bitnami/spark/jobs/app/job_spark_catalyst_udf_comparison.py \
  run CATALYST \
  --data-path /opt/bitnami/spark/jobs/app/names.csv \
  --report-path /opt/bitnami/spark/jobs/app/sparkmeasure_reports/
```

---

## Generating Reports

You can compare multiple executions from the Spark History Server API:

```bash
poetry run python -m spark_catalyst_udf_comparison.cli compare app-XXX-0105 app-YYY-0106 \
    --spark-history-url http://localhost:18080 \
    --report-path ./sparkmeasure_reports/
```

---

## Using Typer CLI Help

This project uses [Typer](https://typer.tiangolo.com/) to build a powerful and auto-documented CLI interface. You can use the built-in `--help` option to explore all available commands and parameters.

### Show Help for All Commands

```bash
poetry run python -m spark_catalyst_udf_comparison.cli --help
```

Or without Poetry:

```bash
python -m spark_catalyst_udf_comparison.cli --help
```

---

## Show Help for Specific Command

### `run` command help

```bash
python -m spark_catalyst_udf_comparison.cli run --help
```

Example of usage:

```bash
python -m spark_catalyst_udf_comparison.cli run CATALYST \
    -d ./data/names.csv \
    -r ./sparkmeasure_reports/ \
    -n 3 \
    -e
```

---

### `compare` command help

```bash
python -m spark_catalyst_udf_comparison.cli compare --help
```

Example:

```bash
python -m spark_catalyst_udf_comparison.cli compare app-20250522230757-0105 app-20250522230853-0106 \
    --spark-history-url http://localhost:18080 \
    --report-path ./reports/
```

---

### View Execution Help Inside Docker

You can also view the help directly inside the container:

```bash
docker exec -it spark-master python /opt/bitnami/spark/jobs/app/job_spark_catalyst_udf_comparison.py --help
```

And for subcommands:

```bash
docker exec -it spark-master python /opt/bitnami/spark/jobs/app/job_spark_catalyst_udf_comparison.py run --help
docker exec -it spark-master python /opt/bitnami/spark/jobs/app/job_spark_catalyst_udf_comparison.py compare --help
```

---

## Credits

- RafHellRaiser – original author
- Luca Canali – for [sparkMeasure](https://github.com/LucaCanali/sparkMeasure)