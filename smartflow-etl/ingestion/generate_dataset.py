"""
ingestion/generate_dataset.py
─────────────────────────────
Generates a realistic NYC Taxi-like dataset with intentional messiness
for the SmartFlow ETL pipeline to clean, map, and detect anomalies on.

Place this file at:
    smartflow-etl/
    └── ingestion/
        └── generate_dataset.py   ← here

Run from anywhere:
    python ingestion/generate_dataset.py
"""

import os
import pandas as pd
import numpy as np
import duckdb
from pathlib import Path

# ── Paths (works regardless of where you run the script from) ─────────────────
THIS_DIR  = Path(os.path.dirname(os.path.abspath(__file__)))  # ingestion/
ROOT_DIR  = THIS_DIR.parent                                    # smartflow-etl/
DATA_DIR  = ROOT_DIR / "data" / "raw"
DB_PATH   = ROOT_DIR / "data" / "smartflow.duckdb"
OUT_FILE  = DATA_DIR / "yellow_tripdata_2024-01.parquet"

# ── Dataset config ────────────────────────────────────────────────────────────
np.random.seed(42)
N = 50000

zones         = list(range(1, 264))
payment_types = [1, 2, 3, 4, 99]       # 99 = unknown (dirty)
vendors       = [1, 2, 4, None]

# ── Generate messy data ───────────────────────────────────────────────────────
# Columns have inconsistent naming on purpose — AI schema mapper fixes this
df = pd.DataFrame({
    "tpep_pickup_datetime":  pd.date_range("2024-01-01", periods=N, freq="1min"),
    "tpep_dropoff_datetime": pd.date_range("2024-01-01 00:05", periods=N, freq="1min"),
    "Passenger_Count":       np.random.choice([1,2,3,4,5,6,None], N, p=[.45,.2,.1,.08,.07,.05,.05]),
    "TRIP_DISTANCE":         np.abs(np.random.exponential(3, N)),
    "PULocationID":          np.random.choice(zones, N),
    "DOLocationID":          np.random.choice(zones, N),
    "payment_type":          np.random.choice(payment_types, N, p=[.6,.25,.05,.05,.05]),
    "fare_amount":           np.random.exponential(12, N),
    "tip_amount":            np.abs(np.random.normal(2, 1.5, N)),
    "total_amount":          np.random.exponential(15, N),
    "VendorID":              np.random.choice(vendors, N, p=[.45,.45,.05,.05]),
    "RatecodeID":            np.random.choice([1,2,3,4,5,6,None], N, p=[.85,.05,.03,.02,.02,.02,.01]),
    "store_and_fwd_flag":    np.random.choice(["Y","N",None], N, p=[.05,.9,.05]),
    "congestion_surcharge":  np.random.choice([0, 2.5, None], N, p=[.3,.65,.05]),
    "Airport_fee":           np.random.choice([0, 1.25, None], N, p=[.85,.1,.05]),
})

# ── Inject anomalies (~5% of rows) ───────────────────────────────────────────
anomaly_idx = np.random.choice(N, int(N * 0.05), replace=False)
df.loc[anomaly_idx[:200],  "fare_amount"]    = -99.99   # negative fares
df.loc[anomaly_idx[200:400], "TRIP_DISTANCE"] = 9999.0  # impossible distance
df.loc[anomaly_idx[400:600], "total_amount"]  = 0.0     # zero total
df.loc[anomaly_idx[600:],  "Passenger_Count"] = 0       # zero passengers

# ── Save Parquet ──────────────────────────────────────────────────────────────
DATA_DIR.mkdir(parents=True, exist_ok=True)
df.to_parquet(OUT_FILE, index=False)
print(f"Generated {N:,} rows  →  {OUT_FILE}")
print(f"  Columns   : {list(df.columns)}")
print(f"  Anomalies : {len(anomaly_idx)} rows (~5%)")
print(f"  Nulls     : {df.isnull().sum().sum()} total")

# ── Load into DuckDB ──────────────────────────────────────────────────────────
con = duckdb.connect(str(DB_PATH))
con.execute("CREATE SCHEMA IF NOT EXISTS raw")
con.execute(f"""
    CREATE OR REPLACE TABLE raw.taxi_trips AS
    SELECT * FROM read_parquet('{OUT_FILE.as_posix()}')
""")
count = con.execute("SELECT COUNT(*) FROM raw.taxi_trips").fetchone()[0]
print(f"\nLoaded {count:,} rows into DuckDB  →  raw.taxi_trips")
print(f"DB location: {DB_PATH}")
con.close()
print("\nDone. Now run:  python ai/schema_mapper.py")