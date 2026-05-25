"""
ai/anomaly_detector.py
──────────────────────
Mid-pipeline anomaly detection using two complementary methods:
  1. Z-score   — flags statistical outliers per numeric column
  2. IsolationForest — flags multivariate anomalies (rows that are
                       unusual across multiple columns at once)

Anomalous rows → data/quarantine/quarantine_YYYYMMDD.parquet
Clean rows     → raw.taxi_trips_final (ready for dbt)

Run: python ai/anomaly_detector.py
"""

import os
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import duckdb
from loguru import logger
from scipy import stats
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

# ── Paths ─────────────────────────────────────────────────────────────────────
THIS_DIR      = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR      = os.path.dirname(THIS_DIR)
DB_PATH       = os.path.join(ROOT_DIR, "data", "smartflow.duckdb")
QUARANTINE_DIR = Path(ROOT_DIR) / "data" / "quarantine"

SRC_TABLE   = "raw.taxi_trips_cleaned"
FINAL_TABLE = "raw.taxi_trips_final"

# ── Config ────────────────────────────────────────────────────────────────────
ZSCORE_THRESHOLD     = 3.5    # flag if |z| > this
ISOLATION_CONTAMINATION = 0.05  # expect ~5% anomalies

NUMERIC_COLS = [
    "fare_amount", "trip_distance", "total_amount",
    "tip_amount", "passenger_count", "congestion_surcharge",
]


def load_data(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    df = con.execute(f"SELECT * FROM {SRC_TABLE}").fetchdf()
    logger.info(f"Loaded {len(df):,} rows from {SRC_TABLE}")
    return df


def zscore_detection(df: pd.DataFrame) -> pd.Series:
    """
    Flag rows where any numeric column has |z-score| > threshold.
    Good at catching single-column extremes (e.g. fare = -99).
    """
    numeric = df[NUMERIC_COLS].fillna(0)
    z_scores = np.abs(stats.zscore(numeric))
    flagged  = (z_scores > ZSCORE_THRESHOLD).any(axis=1)
    logger.info(f"Z-score detection: {flagged.sum():,} rows flagged (threshold={ZSCORE_THRESHOLD})")
    return flagged


def isolation_forest_detection(df: pd.DataFrame) -> pd.Series:
    """
    Flag multivariate anomalies — rows that are unusual across
    multiple columns simultaneously.
    IsolationForest returns -1 for anomalies, 1 for normal.
    """
    numeric = df[NUMERIC_COLS].fillna(0)
    scaler  = StandardScaler()
    scaled  = scaler.fit_transform(numeric)

    model   = IsolationForest(
        contamination=ISOLATION_CONTAMINATION,
        random_state=42,
        n_estimators=100,
    )
    preds   = model.fit_predict(scaled)
    flagged = pd.Series(preds == -1, index=df.index)
    logger.info(f"IsolationForest detection: {flagged.sum():,} rows flagged (contamination={ISOLATION_CONTAMINATION})")
    return flagged


def combine_flags(
    df: pd.DataFrame,
    zscore_flags: pd.Series,
    iso_flags: pd.Series,
) -> pd.DataFrame:
    """
    Combine both detectors. A row is anomalous if EITHER flags it.
    Also attaches reason column for audit trail.
    """
    df = df.copy()
    df["_flag_zscore"]          = zscore_flags.astype(int)
    df["_flag_isolation_forest"] = iso_flags.astype(int)
    df["_is_anomaly"]           = (zscore_flags | iso_flags).astype(int)

    df["_anomaly_reason"] = "clean"
    df.loc[zscore_flags & ~iso_flags,  "_anomaly_reason"] = "zscore_only"
    df.loc[~zscore_flags & iso_flags,  "_anomaly_reason"] = "isolation_forest_only"
    df.loc[zscore_flags & iso_flags,   "_anomaly_reason"] = "both_detectors"

    df["_detected_at"] = datetime.utcnow().isoformat()

    total = df["_is_anomaly"].sum()
    pct   = total / len(df) * 100
    logger.info(f"Combined: {total:,} anomalous rows ({pct:.1f}%)")
    logger.info(f"  z-score only      : {(zscore_flags & ~iso_flags).sum():,}")
    logger.info(f"  isolation only    : {(~zscore_flags & iso_flags).sum():,}")
    logger.info(f"  both detectors    : {(zscore_flags & iso_flags).sum():,}")
    return df


def route_rows(df: pd.DataFrame, con: duckdb.DuckDBPyConnection) -> None:
    """
    Split into clean and anomalous.
    Clean  → raw.taxi_trips_final  (DuckDB — feeds dbt)
    Anomaly → data/quarantine/     (Parquet — for review)
    """
    clean    = df[df["_is_anomaly"] == 0].drop(
        columns=["_flag_zscore", "_flag_isolation_forest",
                 "_is_anomaly", "_anomaly_reason", "_detected_at"]
    )
    anomalous = df[df["_is_anomaly"] == 1]

    # Save clean to DuckDB
    con.execute(f"CREATE OR REPLACE TABLE {FINAL_TABLE} AS SELECT * FROM clean")
    logger.success(f"Clean rows → {FINAL_TABLE}: {len(clean):,} rows")

    # Save anomalous to quarantine parquet
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    date_str  = datetime.utcnow().strftime("%Y%m%d")
    q_path    = QUARANTINE_DIR / f"quarantine_{date_str}.parquet"
    anomalous.to_parquet(q_path, index=False)
    logger.success(f"Anomalous rows → {q_path}: {len(anomalous):,} rows")

    # Summary by reason
    logger.info("\nQuarantine breakdown:")
    summary = anomalous["_anomaly_reason"].value_counts()
    for reason, count in summary.items():
        logger.info(f"  {reason:<30} {count:,} rows")


def run():
    logger.info("── Anomaly Detector ──────────────────────────────────")
    con = duckdb.connect(DB_PATH)
    df  = load_data(con)

    logger.info("Step 1: Z-score detection")
    zscore_flags = zscore_detection(df)

    logger.info("Step 2: IsolationForest detection")
    iso_flags    = isolation_forest_detection(df)

    logger.info("Step 3: Combine flags + add audit columns")
    df           = combine_flags(df, zscore_flags, iso_flags)

    logger.info("Step 4: Route clean → DuckDB, anomalous → quarantine")
    route_rows(df, con)

    con.close()
    logger.success("── Anomaly detection complete ────────────────────────")
    logger.info("Next: run dbt run  (or python orchestrations/pipeline.py)")


if __name__ == "__main__":
    run()