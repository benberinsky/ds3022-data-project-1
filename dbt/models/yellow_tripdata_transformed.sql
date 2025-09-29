SELECT
    yt.*,

    -- calculate total co2 for each trip
    (yt.distance * ve.co2_grams_per_mile) / 1000.0 as trip_co2_kgs,

    -- calculate average mph
    yt.distance / (extract(epoch from yt.dropoff_datetime - yt.pickup_datetime) / 3600.0) as avg_mph,

    -- Extract hour of day
    extract(hour from yt.pickup_datetime) as hour_of_day, 

    -- Extract day of week
    strftime(yt.pickup_datetime, '%a') as day_of_week,

    -- Extract week number
    extract(week from yt.pickup_datetime) as week_of_year,

    -- Extract month number
    strftime(yt.pickup_datetime, '%b') as month_of_year,

    -- Extract year(extra added step for plotting)
    extract(year from yt.pickup_datetime) as specified_year

from {{ source('emissions','yellow_tripdata') }} yt
join {{ source('emissions','vehicle_emissions') }} ve
  on ve.vehicle_type = 'yellow_taxi'
