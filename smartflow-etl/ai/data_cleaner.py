"""
ai/data_cleaner.py
──────────────────
ML-based data cleaning on raw.taxi_trips_mapped.
Replaces brittle if/else rules with scikit-learn models.

Steps:
  1. KNNImputer  — fills nulls using nearest-neighbour values
  2. Clip        — caps extreme outliers to valid business ranges
  3. Unknown fix — maps payment_type=99 to mode of known values
  4. Writes      — saves cleaned table as raw.taxi_trips_cleaned

Run: python ai/data_cleaner.py
"""

import os
import numpy as np
import pandas as pd
import duckdb
from loguru import logger
from sklearn.impute import KNNImputer
from sklearn.preprocessing import StandardScaler

# ── Paths ─────────────────────────────────────────────────────────────────────
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(THIS_DIR)
DB_PATH  = os.path.join(ROOT_DIR, "data", "smartflow.duckdb")

SRC_TABLE = "raw.taxi_trips_mapped"
OUT_TABLE = "raw.taxi_trips_cleaned"

# ── Business rules for clipping ───────────────────────────────────────────────
CLIP_RULES = {
    "fare_amount":          (0.5,  500.0),
    "trip_distance":        (0.01, 100.0),
    "total_amount":         (0.5,  500.0),
    "tip_amount":           (0.0,  200.0),
    "passenger_count":      (1.0,  6.0),
    "congestion_surcharge": (0.0,  10.0),
    "airport_fee":          (0.0,  10.0),
}

# ── Numeric columns to impute ─────────────────────────────────────────────────
NUMERIC_COLS = [
    "passenger_count", "trip_distance", "fare_amount",
    "tip_amount", "total_amount", "congestion_surcharge", "airport_fee",
]


def load_data(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    df = con.execute(f"SELECT * FROM {SRC_TABLE}").fetchdf()
    logger.info(f"Loaded {len(df):,} rows from {SRC_TABLE}")
    logger.info(f"Nulls before cleaning:\n{df[NUMERIC_COLS].isnull().sum().to_string()}")
    return df


def fix_unknown_payment(df: pd.DataFrame) -> pd.DataFrame:
    """Replace payment_type=99 (unknown) with the mode of known values."""
    known_mode = df.loc[df["payment_type"] != 99, "payment_type"].mode()[0]
    unknown_count = (df["payment_type"] == 99).sum()
    df.loc[df["payment_type"] == 99, "payment_type"] = known_mode
    logger.info(f"Fixed {unknown_count:,} unknown payment types → {known_mode}")
    return df


def knn_impute(df: pd.DataFrame) -> pd.DataFrame:
    """Fill nulls in numeric columns using KNN (k=5 neighbours)."""
    numeric_df = df[NUMERIC_COLS].copy()

    # Scale before KNN so distance calculation is fair
    scaler    = StandardScaler()
    scaled    = scaler.fit_transform(numeric_df)

    imputer   = KNNImputer(n_neighbors=5, weights="distance")
    imputed   = imputer.fit_transform(scaled)

    # Inverse scale back to original range
    restored  = scaler.inverse_transform(imputed)
    imputed_df = pd.DataFrame(restored, columns=NUMERIC_COLS, index=df.index)

    nulls_filled = numeric_df.isnull().sum().sum()
    df[NUMERIC_COLS] = imputed_df
    logger.info(f"KNNImputer filled {nulls_filled:,} null values across {len(NUMERIC_COLS)} columns")
    return df


def clip_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """Clip extreme values to valid business ranges."""
    total_clipped = 0
    for col, (lo, hi) in CLIP_RULES.items():
        if col not in df.columns:
            continue
        before = ((df[col] < lo) | (df[col] > hi)).sum()
        df[col] = df[col].clip(lower=lo, upper=hi)
        total_clipped += before
        if before > 0:
            logger.info(f"  Clipped {before:,} values in {col} → [{lo}, {hi}]")
    logger.info(f"Total clipped: {total_clipped:,} values")
    return df


def round_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Round financials to 2dp, counts to integers."""
    money_cols = ["fare_amount", "tip_amount", "total_amount",
                  "congestion_surcharge", "airport_fee"]
    for col in money_cols:
        if col in df.columns:
            df[col] = df[col].round(2)
    for col in ["passenger_count"]:
        if col in df.columns:
            df[col] = df[col].round(0).astype(int)
    return df


def save_to_duckdb(df: pd.DataFrame, con: duckdb.DuckDBPyConnection) -> None:
    con.execute(f"CREATE OR REPLACE TABLE {OUT_TABLE} AS SELECT * FROM df")
    count = con.execute(f"SELECT COUNT(*) FROM {OUT_TABLE}").fetchone()[0]
    nulls = con.execute(f"""
        SELECT {' + '.join([f'COUNT(*) FILTER (WHERE "{c}" IS NULL)' for c in NUMERIC_COLS])}
        FROM {OUT_TABLE}
    """).fetchone()[0]
    logger.success(f"Saved {count:,} rows → {OUT_TABLE}  |  Remaining nulls: {nulls}")


def run():
    logger.info("── AI Data Cleaner ───────────────────────────────────")
    con = duckdb.connect(DB_PATH)
    df  = load_data(con)

    logger.info("Step 1: Fix unknown payment types")
    df  = fix_unknown_payment(df)

    logger.info("Step 2: KNN imputation for nulls")
    df  = knn_impute(df)

    logger.info("Step 3: Clip outliers to valid ranges")
    df  = clip_outliers(df)

    logger.info("Step 4: Round columns")
    df  = round_columns(df)

    logger.info("Step 5: Save cleaned table")
    save_to_duckdb(df, con)

    con.close()
    logger.success("── Data cleaning complete ────────────────────────────")
    logger.info("Next: run python ai/anomaly_detector.py")


if __name__ == "__main__":
    run()