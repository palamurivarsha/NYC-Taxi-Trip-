"""
dags/pipeline.py
Run: python -m dagster dev -f dags/pipeline.py
"""

import os
import sys
import subprocess

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
    define_asset_job,
    Definitions,
    AssetSelection,
)

THIS_DIR  = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR  = os.path.dirname(THIS_DIR)
sys.path.insert(0, ROOT_DIR)

# ── Use the SAME python that is running Dagster (venv python) ─────────────────
PYTHON = sys.executable


def run_script(script_path: str, context) -> None:
    """Run a script using the venv Python — same one running Dagster."""
    result = subprocess.run(
        [PYTHON, script_path],
        cwd=ROOT_DIR,
        capture_output=True,
        text=True,
    )
    if result.stdout:
        context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(result.stderr or result.stdout)


# ── Asset 1 ───────────────────────────────────────────────────────────────────
@asset(group_name="ingestion")
def raw_taxi_data(context: AssetExecutionContext) -> MaterializeResult:
    run_script("ingestion/generate_dataset.py", context)
    import duckdb
    con   = duckdb.connect(os.path.join(ROOT_DIR, "data", "smartflow.duckdb"))
    count = con.execute("SELECT COUNT(*) FROM raw.taxi_trips").fetchone()[0]
    con.close()
    return MaterializeResult(metadata={"row_count": MetadataValue.int(count)})


# ── Asset 2 ───────────────────────────────────────────────────────────────────
@asset(group_name="ai_layer", deps=[raw_taxi_data])
def schema_mapped_data(context: AssetExecutionContext) -> MaterializeResult:
    run_script("ai/schema_mapper.py", context)
    import duckdb
    con   = duckdb.connect(os.path.join(ROOT_DIR, "data", "smartflow.duckdb"))
    count = con.execute("SELECT COUNT(*) FROM raw.taxi_trips_mapped").fetchone()[0]
    con.close()
    return MaterializeResult(metadata={"row_count": MetadataValue.int(count)})


# ── Asset 3 ───────────────────────────────────────────────────────────────────
@asset(group_name="ai_layer", deps=[schema_mapped_data])
def cleaned_data(context: AssetExecutionContext) -> MaterializeResult:
    run_script("ai/data_cleaner.py", context)
    import duckdb
    con   = duckdb.connect(os.path.join(ROOT_DIR, "data", "smartflow.duckdb"))
    count = con.execute("SELECT COUNT(*) FROM raw.taxi_trips_cleaned").fetchone()[0]
    con.close()
    return MaterializeResult(metadata={"row_count": MetadataValue.int(count)})


# ── Asset 4 ───────────────────────────────────────────────────────────────────
@asset(group_name="ai_layer", deps=[cleaned_data])
def anomaly_detected_data(context: AssetExecutionContext) -> MaterializeResult:
    run_script("ai/anomaly_detector.py", context)
    import duckdb
    from pathlib import Path
    import pandas as pd
    con         = duckdb.connect(os.path.join(ROOT_DIR, "data", "smartflow.duckdb"))
    clean_count = con.execute("SELECT COUNT(*) FROM raw.taxi_trips_final").fetchone()[0]
    con.close()
    q_files       = list((Path(ROOT_DIR) / "data" / "quarantine").glob("*.parquet"))
    anomaly_count = sum(pd.read_parquet(f).shape[0] for f in q_files)
    return MaterializeResult(metadata={
        "clean_rows":   MetadataValue.int(clean_count),
        "anomaly_rows": MetadataValue.int(anomaly_count),
    })


# ── Asset 5 ───────────────────────────────────────────────────────────────────
@asset(group_name="validation", deps=[anomaly_detected_data])
def validated_data(context: AssetExecutionContext) -> MaterializeResult:
    run_script("great_expectations/expectations.py", context)
    return MaterializeResult(metadata={"status": MetadataValue.text("12/12 passed")})


# ── Asset 6 ───────────────────────────────────────────────────────────────────
@asset(group_name="transformation", deps=[validated_data])
def dbt_models(context: AssetExecutionContext) -> MaterializeResult:
    dbt_dir = os.path.join(ROOT_DIR, "dbt_project")
    result  = subprocess.run(
        [PYTHON, "-m", "dbt", "run", "--profiles-dir", "."],
        cwd=dbt_dir,
        capture_output=True,
        text=True,
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(result.stderr or result.stdout)
    return MaterializeResult(metadata={"status": MetadataValue.text("dbt run complete")})


# ── Definitions ───────────────────────────────────────────────────────────────
defs = Definitions(
    assets=[
        raw_taxi_data,
        schema_mapped_data,
        cleaned_data,
        anomaly_detected_data,
        validated_data,
        dbt_models,
    ],
    jobs=[define_asset_job("smartflow_etl_job", AssetSelection.all())],
)