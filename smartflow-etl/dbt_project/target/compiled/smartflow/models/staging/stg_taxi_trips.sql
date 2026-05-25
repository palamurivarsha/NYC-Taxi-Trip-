

with source as (
    select * from "smartflow"."raw"."taxi_trips_mapped"
),

casted as (
    select
        cast(vendor_id            as integer)   as vendor_id,
        cast(rate_code_id         as integer)   as rate_code_id,
        cast(payment_type         as integer)   as payment_type,
        cast(pickup_datetime      as timestamp) as pickup_datetime,
        cast(dropoff_datetime     as timestamp) as dropoff_datetime,
        cast(passenger_count      as integer)   as passenger_count,
        cast(pickup_location_id   as integer)   as pickup_location_id,
        cast(dropoff_location_id  as integer)   as dropoff_location_id,
        cast(store_and_fwd_flag   as varchar)   as store_and_fwd_flag,
        cast(fare_amount          as double)    as fare_amount,
        cast(tip_amount           as double)    as tip_amount,
        cast(total_amount         as double)    as total_amount,
        cast(congestion_surcharge as double)    as congestion_surcharge,
        cast(airport_fee          as double)    as airport_fee,
        cast(trip_distance        as double)    as trip_distance
    from source
),

enriched as (
    select
        vendor_id,
        rate_code_id,
        payment_type,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        pickup_location_id,
        dropoff_location_id,
        store_and_fwd_flag,
        fare_amount,
        tip_amount,
        total_amount,
        congestion_surcharge,
        airport_fee,
        trip_distance,

        -- derived time fields
        round(datediff('minute', pickup_datetime, dropoff_datetime), 1) as trip_duration_mins,
        extract(hour from pickup_datetime)                               as pickup_hour,
        extract(dow  from pickup_datetime)                               as pickup_day_of_week,

        case extract(dow from pickup_datetime)
            when 0 then 'Sunday'   when 1 then 'Monday'
            when 2 then 'Tuesday'  when 3 then 'Wednesday'
            when 4 then 'Thursday' when 5 then 'Friday'
            when 6 then 'Saturday'
        end as pickup_day_name,

        case
            when extract(hour from pickup_datetime) between 6  and 11 then 'Morning'
            when extract(hour from pickup_datetime) between 12 and 16 then 'Afternoon'
            when extract(hour from pickup_datetime) between 17 and 20 then 'Evening'
            else 'Night'
        end as time_of_day,

        -- labels
        case vendor_id
            when 1 then 'Creative Mobile Technologies'
            when 2 then 'VeriFone Inc'
            else        'Unknown Vendor'
        end as vendor_name,

        case payment_type
            when 1 then 'Credit Card' when 2 then 'Cash'
            when 3 then 'No Charge'   when 4 then 'Dispute'
            else        'Unknown'
        end as payment_type_label,

        case rate_code_id
            when 1 then 'Standard Rate'      when 2 then 'JFK'
            when 3 then 'Newark'             when 4 then 'Nassau/Westchester'
            when 5 then 'Negotiated Fare'    when 6 then 'Group Ride'
            else        'Unknown Rate'
        end as rate_code_label,

        -- quality flags
        case when fare_amount    < 0   then 1 else 0 end as flag_negative_fare,
        case when total_amount   = 0   then 1 else 0 end as flag_zero_total,
        case when trip_distance  > 500 then 1 else 0 end as flag_impossible_distance,
        case when passenger_count is null
              or passenger_count = 0  then 1 else 0 end as flag_bad_passenger_count,
        case when payment_type   = 99  then 1 else 0 end as flag_unknown_payment,
        case when dropoff_datetime <= pickup_datetime
                                       then 1 else 0 end as flag_invalid_timestamps,

        -- tip pct
        case when fare_amount > 0
             then round(tip_amount / fare_amount * 100, 2)
             else null
        end as tip_pct,

        current_timestamp         as _loaded_at,
        'raw.taxi_trips_mapped'   as _source_table

    from casted
),

final as (
    select
        *,
        -- composite anomaly flag
        case when (
            flag_negative_fare + flag_zero_total + flag_impossible_distance +
            flag_bad_passenger_count + flag_unknown_payment + flag_invalid_timestamps
        ) > 0 then 1 else 0 end as is_anomaly
    from enriched
    where not (
        pickup_location_id = dropoff_location_id
        and trip_duration_mins = 0
        and fare_amount = 0
    )
)

select * from final