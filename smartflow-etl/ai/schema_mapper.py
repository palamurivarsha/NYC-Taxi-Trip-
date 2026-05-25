"""
ai/schema_mapper.py
───────────────────
AI Schema Mapper — maps messy source columns to canonical schema
using TF-IDF cosine similarity (no extra installs needed).
"""

import json
import os
import duckdb
import numpy as np
from loguru import logger
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer

# ── Always use TF-IDF (no sentence-transformers needed) ───────────────────────
USE_SENTENCE_TRANSFORMERS = False

# ── Paths ─────────────────────────────────────────────────────────────────────
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(THIS_DIR)
DB_PATH  = os.path.join(ROOT_DIR, "data", "smartflow.duckdb")

RAW_TABLE = "raw.taxi_trips"
OUT_TABLE = "raw.taxi_trips_mapped"

# ── Canonical target schema ───────────────────────────────────────────────────
CANONICAL_SCHEMA = {
    "vendor_id":           "identifier for the taxi vendor or provider",
    "pickup_datetime":     "timestamp when passenger was picked up",
    "dropoff_datetime":    "timestamp when passenger was dropped off",
    "passenger_count":     "number of passengers in the vehicle",
    "trip_distance":       "distance of the trip in miles",
    "pickup_location_id":  "taxi zone id for pickup location",
    "dropoff_location_id": "taxi zone id for dropoff location",
    "rate_code_id":        "rate code for the trip pricing",
    "store_and_fwd_flag":  "whether trip was stored before sending to server",
    "payment_type":        "payment method used by passenger",
    "fare_amount":         "base fare amount charged",
    "tip_amount":          "tip amount given by passenger",
    "total_amount":        "total amount charged including all fees",
    "congestion_surcharge":"surcharge for congestion pricing zone",
    "airport_fee":         "fee for airport pickup or dropoff",
}


def get_embeddings(texts: list) -> np.ndarray:
    """TF-IDF character n-gram embeddings — runs anywhere, no extra install."""
    vectorizer = TfidfVectorizer(analyzer="char_wb", ngram_range=(2, 4))
    return vectorizer.fit_transform(texts).toarray()


def map_columns(source_cols: list) -> dict:
    canonical_names = list(CANONICAL_SCHEMA.keys())
    canonical_descs = list(CANONICAL_SCHEMA.values())

    source_texts = [
        f"{c.lower().replace('_', ' ').replace('tpep ', '')} {c.lower()}"
        for c in source_cols
    ]
    target_texts = [
        f"{name.replace('_', ' ')} {desc}"
        for name, desc in zip(canonical_names, canonical_descs)
    ]

    logger.info("Using TF-IDF embeddings")
    embeddings = get_embeddings(source_texts + target_texts)
    src_emb    = embeddings[:len(source_cols)]
    tgt_emb    = embeddings[len(source_cols):]
    sim_matrix = cosine_similarity(src_emb, tgt_emb)

    mapping = {}
    for i, src_col in enumerate(source_cols):
        best_idx   = int(np.argmax(sim_matrix[i]))
        best_score = float(sim_matrix[i][best_idx])
        mapping[src_col] = {
            "canonical": canonical_names[best_idx],
            "score":     round(best_score, 4),
        }
    return mapping


def apply_mapping(mapping: dict, con: duckdb.DuckDBPyConnection) -> None:
    select_parts = []
    for src_col, info in mapping.items():
        canonical = info["canonical"]
        score     = info["score"]
        if score < 0.15:
            select_parts.append(f'"{src_col}" AS "{src_col}_unmapped"')
        else:
            select_parts.append(f'"{src_col}" AS "{canonical}"')

    sql = f"""
        CREATE OR REPLACE TABLE {OUT_TABLE} AS
        SELECT {', '.join(select_parts)}
        FROM {RAW_TABLE}
    """
    con.execute(sql)
    logger.success(f"Created {OUT_TABLE} with canonical column names")


def run():
    logger.info("── AI Schema Mapper ──────────────────────────────────")
    con = duckdb.connect(DB_PATH)

    source_cols = [row[0] for row in con.execute(f"DESCRIBE {RAW_TABLE}").fetchall()]
    logger.info(f"Source columns ({len(source_cols)}): {source_cols}")

    mapping = map_columns(source_cols)

    logger.info(f"{'Source column':<30} {'→ Canonical':<30} {'Score':>6}")
    logger.info("─" * 70)
    for src, info in mapping.items():
        logger.info(f"{src:<30} → {info['canonical']:<30} {info['score']:>6}")

    os.makedirs(os.path.join(ROOT_DIR, "data"), exist_ok=True)
    with open(os.path.join(ROOT_DIR, "data", "schema_mapping.json"), "w") as f:
        json.dump(mapping, f, indent=2)
    logger.info("Mapping saved → data/schema_mapping.json")

    apply_mapping(mapping, con)

    count = con.execute(f"SELECT COUNT(*) FROM {OUT_TABLE}").fetchone()[0]
    logger.success(f"Output: {count:,} rows in {OUT_TABLE}")
    con.close()
    logger.success("── Schema mapping complete ───────────────────────────")


if __name__ == "__main__":
    run()