SELECT
    gt.*,

    -- calculate total co2 for each trip
    (gt.distance * ve.co2_grams_per_mile) / 1000.0 as trip_co2_kgs,

    -- calculate average mph
    gt.distance / (extract(epoch from gt.dropoff_datetime - gt.pickup_datetime) / 3600.0) as avg_mph,

    -- Extract hour of day
    extract(hour from gt.pickup_datetime) as hour_of_day, 

    -- Extract day of week
    strftime(gt.pickup_datetime, '%a') as day_of_week,

    -- Extract week number
    extract(week from gt.pickup_datetime) as week_of_year,

    -- Extract month number
    strftime(gt.pickup_datetime, '%b') as month_of_year,

    -- Extract year(extra added step for plotting)
    extract(year from gt.pickup_datetime) as specified_year

from {{ source('emissions','green_tripdata') }} gt
JOIN {{ source('emissions', 'vehicle_emissions') }} ve
  ON ve.vehicle_type = 'green_taxi'
