
  
    
    

    create  table
      "smartflow"."main_marts"."fct_trips__dbt_tmp"
  
    as (
      

/*
  fct_trips
  ─────────
  Source : staging.stg_taxi_trips
  Purpose: Analytics-ready fact table. Filters out anomalous rows,
           adds business metrics, and structures data for BI queries
           and the Streamlit dashboard.

  What this model does:
    1. Excludes anomalous rows (is_anomaly = 1) — clean data only
    2. Adds revenue breakdown and speed metrics
    3. Buckets trips by distance and fare for segment analysis
    4. Adds surrogate trip_id for unique row identification
    5. Retains all dimension keys for slicing (time, location, vendor)
*/

with

staging as (
    select * from "smartflow"."main_staging"."stg_taxi_trips"
    where is_anomaly = 0           -- only clean rows into the mart
      and trip_duration_mins > 0   -- valid trips only
      and trip_distance      > 0
      and fare_amount        > 0
),

with_metrics as (
    select
        -- ── Surrogate key ─────────────────────────────────────────────
        row_number() over (
            order by pickup_datetime, pickup_location_id
        )                                               as trip_id,

        -- ── Time dimensions ───────────────────────────────────────────
        pickup_datetime,
        dropoff_datetime,
        pickup_hour,
        pickup_day_of_week,
        pickup_day_name,
        time_of_day,
        cast(pickup_datetime as date)                   as trip_date,
        extract(month from pickup_datetime)             as trip_month,
        extract(year  from pickup_datetime)             as trip_year,

        -- ── Location dimensions ───────────────────────────────────────
        pickup_location_id,
        dropoff_location_id,
        case
            when pickup_location_id = dropoff_location_id then 'Same Zone'
            else 'Cross Zone'
        end                                             as trip_zone_type,

        -- ── Vendor & payment dimensions ───────────────────────────────
        vendor_id,
        vendor_name,
        payment_type,
        payment_type_label,
        rate_code_id,
        rate_code_label,
        passenger_count,

        -- ── Core trip metrics ─────────────────────────────────────────
        trip_distance,
        trip_duration_mins,
        round(
            trip_distance / nullif(trip_duration_mins / 60.0, 0)
        , 2)                                            as avg_speed_mph,

        -- ── Revenue breakdown ─────────────────────────────────────────
        fare_amount,
        tip_amount,
        coalesce(congestion_surcharge, 0)               as congestion_surcharge,
        coalesce(airport_fee, 0)                        as airport_fee,
        total_amount,
        tip_pct,

        round(
            fare_amount / nullif(trip_duration_mins, 0)
        , 2)                                            as revenue_per_min,

        round(
            fare_amount / nullif(trip_distance, 0)
        , 2)                                            as revenue_per_mile,

        -- ── Distance buckets ──────────────────────────────────────────
        case
            when trip_distance < 1    then 'Under 1 mile'
            when trip_distance < 3    then '1–3 miles'
            when trip_distance < 7    then '3–7 miles'
            when trip_distance < 15   then '7–15 miles'
            else                           '15+ miles'
        end                                             as distance_bucket,

        -- ── Fare buckets ──────────────────────────────────────────────
        case
            when fare_amount < 5      then 'Under $5'
            when fare_amount < 15     then '$5–$15'
            when fare_amount < 30     then '$15–$30'
            when fare_amount < 60     then '$30–$60'
            else                           '$60+'
        end                                             as fare_bucket,

        -- ── Duration buckets ──────────────────────────────────────────
        case
            when trip_duration_mins < 5   then 'Under 5 min'
            when trip_duration_mins < 15  then '5–15 min'
            when trip_duration_mins < 30  then '15–30 min'
            when trip_duration_mins < 60  then '30–60 min'
            else                               '60+ min'
        end                                             as duration_bucket,

        -- ── Tip behaviour ─────────────────────────────────────────────
        case
            when tip_amount  = 0          then 'No Tip'
            when tip_pct    < 10          then 'Low (<10%)'
            when tip_pct    < 20          then 'Standard (10–20%)'
            else                               'Generous (20%+)'
        end                                             as tip_category,

        -- ── Airport flag ──────────────────────────────────────────────
        case
            when coalesce(airport_fee, 0) > 0 then 1
            else 0
        end                                             as is_airport_trip,

        -- ── Metadata ──────────────────────────────────────────────────
        _loaded_at,
        _source_table

    from staging
)

select * from with_metrics
    );
  
  