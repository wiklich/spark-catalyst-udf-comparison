"""
Build Script for spark-catalyst-udf-comparison

This script creates a .zip package containing all necessary Python UDFs and utilities,
but keeps the main entrypoint (`encrypt_name_example.py`) outside of it.

The final structure is:
dist/
├── crypto_udfs.zip        # src/spark_catalyst_udf_comparison/
└── encrypt_name_example.py # jobs/encrypt_name_example.py (main entrypoint)
"""

import os
from zipfile import ZipFile
from pathlib import Path


SRC_DIR = "src/spark_catalyst_udf_comparison"
OUTPUT_ZIP = "dist/crypto_udfs_lib.zip"
MAIN_SCRIPT = "jobs/job_spark_catalyst_udf_comparison.py"


def create_zip(src_dir: str, output_zip: str):
    """
    Creates a zip file with Python modules for use in PySpark.

    Args:
        src_dir (str): Source directory to include.
        output_zip (str): Output zip file path.
    """
    src_path = Path(src_dir).resolve()
    output_path = Path(output_zip).resolve()

    print(f"[INFO] Source directory: {src_path}")
    print(f"[INFO] Output zip file: {output_path}")

    if not src_path.exists():
        raise FileNotFoundError(f"Source directory not found: {src_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with ZipFile(output_path, 'w') as zipf:
        for root, _, files in os.walk(src_path):
            for file in files:
                if file.endswith(".py"):
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, os.path.dirname(src_path))
                    zipf.write(file_path, arcname)

    print(f"[SUCCESS] Successfully created {output_path}")


def copy_main_script(script_path: str, dest_dir: str):
    """
    Copies the main script to the output directory.

    Args:
        script_path (str): Path to the main entrypoint script.
        dest_dir (str): Directory where the script should be copied to.
    """
    import shutil
    script_file = Path(script_path).resolve()
    dest_folder = Path(dest_dir).resolve()

    print(f"[INFO] Copying main script {script_file} → {dest_folder}/")
    dest_folder.mkdir(exist_ok=True)
    shutil.copy(script_file, dest_folder / script_file.name)

    print(f"[SUCCESS] Main script copied to {dest_folder}/{script_file.name}")


if __name__ == "__main__":
    create_zip(SRC_DIR, OUTPUT_ZIP)
    copy_main_script(MAIN_SCRIPT, "dist")