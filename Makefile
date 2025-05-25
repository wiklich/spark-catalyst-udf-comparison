# Makefile
#
# Automates builds, tests and deployment of the project.
# Use 'make [target]' to execute commands.

.DEFAULT_GOAL := help

# === Settings ===
PROJECT_NAME = spark-catalyst-udf-comparison
SRC_DIR = src/spark_catalyst_udf_comparison
OUTPUT_ZIP = dist/crypto_udfs.zip
MAIN_SCRIPT = jobs/job_spark_catalyst_udf_comparison.py
DOCKER_CONTAINER = spark-master
SPARK_MASTER = spark://$(DOCKER_CONTAINER):7077

# === Commands ===

help:
    @echo "Available commands:"
    @echo "  make install        - Install dependencies in virtual environment"
    @echo "  make build-zip      - Build .zip with Python UDFs for Spark"
    @echo "  make run-local      - Run benchmark locally (ex: CATALYST)"
    @echo "  make copy-to-docker - Copy files into Docker container"
    @echo "  make submit-spark   - Submit job to Spark inside the container"
    @echo "  make clean          - Clean temporary files"

install: ## Install the project's dependencies
    poetry install

build-zip: clean ## Build .zip file with Python modules for use with Spark
    mkdir -p dist/
    python scripts/build_zip.py

copy-to-docker: build-zip ## Copy the zip package to Docker container
    docker exec -it $(DOCKER_CONTAINER) mkdir -p /app
    docker cp $(OUTPUT_ZIP) $(DOCKER_CONTAINER):/app/
    docker cp $(MAIN_SCRIPT) $(DOCKER_CONTAINER):/app/

submit-spark: copy-to-docker ## Submit job to PySpark in Docker container
    docker exec -it $(DOCKER_CONTAINER) /opt/bitnami/spark/bin/spark-submit \
        --master $(SPARK_MASTER) \
        --deploy-mode client \
        --conf spark.sql.extensions=br.com.wiklich.security.crypto.CryptoExtension \
        --py-files /app/crypto_udfs.zip \
        /app/$(notdir $(MAIN_SCRIPT)) \
        --execution_type=CATALYST \
        --report_path=/app/reports \
        --num_runs=5 \
        --explain-codegen

run-local: ## Run benchmark locally using Poetry
    poetry run spark-catalyst run CATALYST \
        --data-path jobs/names.csv \
        --report-path dist/reports \
        --num_runs 3 \
        --explain-codegen

clean: ## Clean dist folder
    rm -rf dist/*
    echo "[SUCCESS] dist/ folder cleaned"