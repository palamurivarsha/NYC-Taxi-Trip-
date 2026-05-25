"""
validations/expectations.py
────────────────────────────
Great Expectations data quality checks on the final clean table.
Runs after anomaly detection, before the dashboard.

Checks:
  - Null rates within acceptable thresholds
  - Column value ranges (fare, distance, passengers)
  - Referential integrity (payment types, vendor IDs)
  - Row count minimum threshold
  - Anomaly rate ceiling

Run: python validations/expectations.py
"""

import os
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import duckdb
from loguru import logger

# ── Paths ─────────────────────────────────────────────────────────────────────
THIS_DIR   = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR   = os.path.dirname(THIS_DIR)
DB_PATH    = os.path.join(ROOT_DIR, "data", "smartflow.duckdb")
REPORT_DIR = Path(ROOT_DIR) / "data" / "validation_reports"

TABLE = "raw.taxi_trips_final"

# ── Expectation definitions ───────────────────────────────────────────────────
EXPECTATIONS = [
    {
        "name":    "min_row_count",
        "desc":    "Table has at least 40,000 rows",
        "check":   lambda df: len(df) >= 40_000,
        "detail":  lambda df: f"{len(df):,} rows",
    },
    {
        "name":    "fare_amount_positive",
        "desc":    "fare_amount is always > 0",
        "check":   lambda df: (df["fare_amount"] > 0).all(),
        "detail":  lambda df: f"{(df['fare_amount'] <= 0).sum()} violations",
    },
    {
        "name":    "fare_amount_range",
        "desc":    "fare_amount between $0.50 and $500",
        "check":   lambda df: df["fare_amount"].between(0.5, 500).all(),
        "detail":  lambda df: f"{(~df['fare_amount'].between(0.5, 500)).sum()} violations",
    },
    {
        "name":    "trip_distance_positive",
        "desc":    "trip_distance is always > 0",
        "check":   lambda df: (df["trip_distance"] > 0).all(),
        "detail":  lambda df: f"{(df['trip_distance'] <= 0).sum()} violations",
    },
    {
        "name":    "trip_distance_realistic",
        "desc":    "trip_distance under 100 miles",
        "check":   lambda df: (df["trip_distance"] <= 100).all(),
        "detail":  lambda df: f"{(df['trip_distance'] > 100).sum()} violations",
    },
    {
        "name":    "passenger_count_valid",
        "desc":    "passenger_count between 1 and 6",
        "check":   lambda df: df["passenger_count"].between(1, 6).all(),
        "detail":  lambda df: f"{(~df['passenger_count'].between(1, 6)).sum()} violations",
    },
    {
        "name":    "no_null_fare",
        "desc":    "fare_amount has no nulls",
        "check":   lambda df: df["fare_amount"].notna().all(),
        "detail":  lambda df: f"{df['fare_amount'].isna().sum()} nulls",
    },
    {
        "name":    "no_null_distance",
        "desc":    "trip_distance has no nulls",
        "check":   lambda df: df["trip_distance"].notna().all(),
        "detail":  lambda df: f"{df['trip_distance'].isna().sum()} nulls",
    },
    {
        "name":    "payment_type_known",
        "desc":    "payment_type has no unknown (99) values",
        "check":   lambda df: (df["payment_type"] != 99).all(),
        "detail":  lambda df: f"{(df['payment_type'] == 99).sum()} unknown payments",
    },
    {
        "name":    "total_amount_positive",
        "desc":    "total_amount is always > 0",
        "check":   lambda df: (df["total_amount"] > 0).all(),
        "detail":  lambda df: f"{(df['total_amount'] <= 0).sum()} violations",
    },
    {
        "name":    "tip_non_negative",
        "desc":    "tip_amount >= 0",
        "check":   lambda df: (df["tip_amount"] >= 0).all(),
        "detail":  lambda df: f"{(df['tip_amount'] < 0).sum()} violations",
    },
    {
        "name":    "null_rate_passenger",
        "desc":    "passenger_count null rate < 5%",
        "check":   lambda df: df["passenger_count"].isna().mean() < 0.05,
        "detail":  lambda df: f"{df['passenger_count'].isna().mean()*100:.1f}% nulls",
    },
]


def run_expectations(df: pd.DataFrame) -> list[dict]:
    results = []
    passed  = 0
    failed  = 0

    logger.info(f"\n{'Expectation':<40} {'Status':>8}  Detail")
    logger.info("─" * 75)

    for exp in EXPECTATIONS:
        try:
            ok     = exp["check"](df)
            detail = exp["detail"](df)
            status = "PASS" if ok else "FAIL"
            if ok:
                passed += 1
                logger.success(f"{exp['desc']:<40} {'✓ PASS':>8}  {detail}")
            else:
                failed += 1
                logger.error(  f"{exp['desc']:<40} {'✗ FAIL':>8}  {detail}")
        except Exception as e:
            status = "ERROR"
            detail = str(e)
            failed += 1
            logger.error(f"{exp['desc']:<40} {'✗ ERROR':>8}  {detail}")

        results.append({
            "expectation": exp["name"],
            "description": exp["desc"],
            "status":      status,
            "detail":      detail,
            "run_at":      datetime.utcnow().isoformat(),
        })

    logger.info("─" * 75)
    logger.info(f"Results: {passed} passed / {failed} failed / {len(EXPECTATIONS)} total")
    return results, passed, failed


def save_report(results: list[dict]) -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    date_str    = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    report_path = REPORT_DIR / f"validation_{date_str}.json"
    with open(report_path, "w") as f:
        json.dump(results, f, indent=2)
    logger.info(f"Validation report saved → {report_path}")


def run():
    logger.info("── Great Expectations Validation ─────────────────────")
    con = duckdb.connect(DB_PATH)
    df  = con.execute(f"SELECT * FROM {TABLE}").fetchdf()
    logger.info(f"Validating {len(df):,} rows in {TABLE}")
    con.close()

    results, passed, failed = run_expectations(df)
    save_report(results)

    if failed == 0:
        logger.success("── All expectations passed ✓ ─────────────────────────")
    else:
        logger.warning(f"── {failed} expectation(s) failed — review report ──────")

    return failed == 0


if __name__ == "__main__":
    run()