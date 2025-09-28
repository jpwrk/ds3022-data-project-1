{{ config(materialized='table') }}

with lookup as (
    select co2_grams_per_mile
    from {{ source('taxi_co2', 'vehicle_emissions') }}
    limit 1
),

base as (
    select *
    from {{ source('taxi_co2', 'yellow_trips_cleaned') }}
),

enriched as (
    select
      b.*,

      -- CO2 in kg
      (b.trip_distance * (select co2_grams_per_mile from lookup) / 1000.0) as trip_co2_kgs,

      -- average mph
      case 
        when (epoch(b.tpep_dropoff_datetime) - epoch(b.tpep_pickup_datetime)) > 0
        then b.trip_distance * 3600.0 / (epoch(b.tpep_dropoff_datetime) - epoch(b.tpep_pickup_datetime))
        else null
      end as avg_mph,

      -- time components
      extract(hour from b.tpep_pickup_datetime)  as hour_of_day,
      extract(dow  from b.tpep_pickup_datetime)  as day_of_week,
      extract(week from b.tpep_pickup_datetime)  as week_of_year,
      extract(month from b.tpep_pickup_datetime) as month_of_year

    from base b
)

select * from enriched
