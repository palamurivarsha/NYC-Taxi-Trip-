# NYC-Taxi-Trip-
AI-Powered Modern Data Engineering Pipeline

<img width="1472" height="1480" alt="image" src="https://github.com/user-attachments/assets/70b80823-5aae-4274-ad1d-e0c9631dc06c" />


1. pip install duckdb pandas pyarrow
working principle:
1. PATHS       — finds smartflow.duckdb automatically, no matter
                 which directory you run the script from

2. GENERATE    — creates 50,000 rows with intentionally messy
                 column names: Passenger_Count, TRIP_DISTANCE,
                 Airport_fee, PULocationID etc.

3. INJECT      — deliberately breaks ~5% of rows:
                   200 rows → fare_amount = -99.99
                   200 rows → TRIP_DISTANCE = 9999
                   200 rows → total_amount = 0
                 1900 rows → Passenger_Count = 0
                 These are what the AI layer detects later

4. SAVE        — writes data/raw/yellow_tripdata_2024-01.parquet

5. LOAD        — creates raw schema in smartflow.duckdb
                 and loads raw.taxi_trips table into it

1. Data ingestion layer (ingestion/generate_dataset.py)
Generated a realistic 50,000-row NYC taxi dataset with intentionally injected messiness — inconsistent column names (Passenger_Count, TRIP_DISTANCE, Airport_fee), ~13,000 null values, and ~2,500 anomalous rows including negative fares, impossible distances, and zero totals. Loaded raw data into DuckDB as raw.taxi_trips using path-safe absolute references so the script runs from any working directory.

3. AI schema mapping (ai/schema_mapper.py)
Built a column-name resolver using cosine similarity on TF-IDF character n-gram embeddings (with a drop-in upgrade path to paraphrase-MiniLM-L3-v2 for semantic embeddings). The mapper automatically resolved all 15 inconsistent source columns — tpep_pickup_datetime → pickup_datetime, PULocationID → pickup_location_id, Airport_fee → airport_fee — with no hardcoded rules. Outputs a canonical raw.taxi_trips_mapped table and an auditable schema_mapping.json for every run.

5. dbt transformation layer (dbt_project/)
Configured dbt Core with the DuckDB adapter (dbt-duckdb) using a local .duckdb file as the warehouse — no cloud account needed. Built the full staging model stg_taxi_trips as a view on top of the AI-mapped source with:

Correct type casting for all 15 columns
Derived fields: trip_duration_mins, pickup_hour, pickup_day_name, time_of_day (Morning/Afternoon/Evening/Night)
Human-readable label columns: vendor_name, payment_type_label, rate_code_label
6 individual data quality flags: flag_negative_fare, flag_zero_total, flag_impossible_distance, flag_bad_passenger_count, flag_unknown_payment, flag_invalid_timestamps
Composite is_anomaly flag — 7,188 rows (14.4%) flagged across the dataset
tip_pct derived metric
_loaded_at and _source_table metadata columns for lineage tracking

Wrote sources.yml declaring the raw DuckDB source, and schema.yml with column-level documentation and 8 dbt tests — all 8 passing on first run.

<img width="489" height="242" alt="image" src="https://github.com/user-attachments/assets/4298fa9a-cf66-475e-ae9c-b25ba30083cb" />
