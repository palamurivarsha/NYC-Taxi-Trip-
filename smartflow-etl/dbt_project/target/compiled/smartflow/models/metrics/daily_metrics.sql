

/*
  daily_metrics
  ─────────────
  Source : marts.fct_trips
  Purpose: Pre-aggregated daily summaries for the Streamlit dashboard.
           All heavy GROUP BY logic lives here so the dashboard
           queries a small table instead of scanning 42k rows live.

  Grain  : one row per trip_date (35 rows total for Jan 2024)
*/

with

base as (
    select * from "smartflow"."main_marts"."fct_trips"
),

-- ── Daily core metrics ────────────────────────────────────────────────────────
daily as (
    select
        trip_date,
        pickup_day_name,
        pickup_day_of_week,

        -- volume
        count(*)                                        as total_trips,
        sum(passenger_count)                            as total_passengers,
        sum(is_airport_trip)                            as airport_trips,

        -- revenue
        round(sum(total_amount), 2)                     as total_revenue,
        round(avg(total_amount), 2)                     as avg_revenue_per_trip,
        round(sum(fare_amount),  2)                     as total_fare,
        round(avg(fare_amount),  2)                     as avg_fare,
        round(sum(tip_amount),   2)                     as total_tips,
        round(avg(tip_pct),      1)                     as avg_tip_pct,
        round(sum(congestion_surcharge), 2)             as total_congestion_surcharge,
        round(sum(airport_fee), 2)                      as total_airport_fees,

        -- trip characteristics
        round(avg(trip_distance),      2)               as avg_distance_miles,
        round(avg(trip_duration_mins), 1)               as avg_duration_mins,
        round(avg(avg_speed_mph),      1)               as avg_speed_mph,
        round(avg(revenue_per_mile),   2)               as avg_revenue_per_mile,
        round(avg(revenue_per_min),    2)               as avg_revenue_per_min,

        -- payment mix
        round(
            sum(case when payment_type_label = 'Credit Card' then 1 else 0 end)
            * 100.0 / count(*), 1
        )                                               as pct_credit_card,
        round(
            sum(case when payment_type_label = 'Cash' then 1 else 0 end)
            * 100.0 / count(*), 1
        )                                               as pct_cash,

        -- tip behaviour
        round(
            sum(case when tip_category = 'Generous (20%+)' then 1 else 0 end)
            * 100.0 / count(*), 1
        )                                               as pct_generous_tippers,
        round(
            sum(case when tip_category = 'No Tip' then 1 else 0 end)
            * 100.0 / count(*), 1
        )                                               as pct_no_tip,

        -- distance mix
        round(
            sum(case when distance_bucket = 'Under 1 mile' then 1 else 0 end)
            * 100.0 / count(*), 1
        )                                               as pct_short_trips,
        round(
            sum(case when distance_bucket = '15+ miles' then 1 else 0 end)
            * 100.0 / count(*), 1
        )                                               as pct_long_trips,

        -- peak hours
        mode() within group (order by pickup_hour)      as peak_hour,
        max(pickup_hour)                                as latest_hour,
        min(pickup_hour)                                as earliest_hour,

        -- vendor split
        round(
            sum(case when vendor_name = 'VeriFone Inc' then 1 else 0 end)
            * 100.0 / count(*), 1
        )                                               as pct_verifone,
        round(
            sum(case when vendor_name = 'Creative Mobile Technologies' then 1 else 0 end)
            * 100.0 / count(*), 1
        )                                               as pct_creative_mobile

    from base
    group by 1, 2, 3
),

-- ── 7-day rolling averages (for trend lines in dashboard) ────────────────────
with_rolling as (
    select
        *,
        round(avg(total_trips) over (
            order by trip_date
            rows between 6 preceding and current row
        ), 0)                                           as trips_7day_avg,

        round(avg(total_revenue) over (
            order by trip_date
            rows between 6 preceding and current row
        ), 2)                                           as revenue_7day_avg,

        round(avg(avg_fare) over (
            order by trip_date
            rows between 6 preceding and current row
        ), 2)                                           as fare_7day_avg,

        -- day-over-day revenue change
        round(
            total_revenue - lag(total_revenue) over (order by trip_date)
        , 2)                                            as revenue_dod_change,

        round(
            (total_revenue - lag(total_revenue) over (order by trip_date))
            * 100.0 / nullif(lag(total_revenue) over (order by trip_date), 0)
        , 1)                                            as revenue_dod_pct_change,

        -- cumulative revenue
        round(sum(total_revenue) over (
            order by trip_date
        ), 2)                                           as cumulative_revenue

    from daily
)

select * from with_rolling
order by trip_date