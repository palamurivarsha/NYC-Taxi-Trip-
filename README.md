# NYC-Taxi-Trip-
AI-Powered Modern Data Engineering Pipeline

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
<img width="1907" height="934" alt="image" src="https://github.com/user-attachments/assets/daf9a39b-e124-4beb-8a00-3a04822c2b12" />
<img width="1907" height="891" alt="image" src="https://github.com/user-attachments/assets/c0144a7a-c7e4-40d0-9bec-8d057b31d181" />
Traditional ETL vs SmartFlow ETL — what actually changed

Everyone talks about "modern data pipelines." Here's what that actually means in practice, step by step 👇
Traditional ETL — how most pipelines still work:
❌ Schema mapping = a human manually writes source.TRIP_DISTANCE → target.trip_distance for every field. When the source changes, it breaks. Someone gets paged at 2am.
❌ Data cleaning = hundreds of if/else rules. IF fare < 0 THEN drop. IF passenger_count IS NULL THEN set to 1. Brittle. Misses edge cases. Never complete.
❌ Bad data = gets silently dropped or worse, loads into the warehouse and corrupts dashboards. Nobody notices until a VP asks a question.
❌ Transformations = someone runs a SQL script manually. Or a cron job that fails silently. No lineage, no tests, no documentation.
❌ Orchestration = a spreadsheet saying "run script A before script B." Or pray.
❌ Dashboard = refreshes once a day. Built by a different team. Takes 3 weeks to add a new metric.
SmartFlow ETL — what we built instead:
✅ Schema mapping → ML model reads column names and maps them automatically using cosine similarity. TRIP_DISTANCE, trip_dist, TripDistance — it figures them out. Zero hardcoded rules.
✅ Data cleaning → KNN imputer fills nulls by looking at similar rows. IsolationForest catches outliers a human would never think to rule for. Catches what rules miss.
✅ Bad data → never deleted. Flagged with a reason (zscore_only, isolation_forest_only, both_detectors) and routed to a quarantine folder. Fully auditable.
✅ Transformations → dbt with 3 tested layers. Staging → Facts → Metrics. 26 automated tests. Full lineage. If a test fails, the pipeline stops.
✅ Orchestration → Dagster. Every step is an asset with dependencies. One fails → downstream skips → you see exactly why in a UI.
✅ Dashboard → Streamlit reading directly from DuckDB. KPIs, trends, anomaly waterfall, payment mix — all live.
