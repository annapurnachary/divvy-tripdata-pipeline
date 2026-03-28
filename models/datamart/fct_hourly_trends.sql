SELECT 
    EXTRACT(HOUR FROM start_datetime) as hour_of_day,
    EXTRACT(DAYOFWEEK FROM start_datetime) as day_of_week,
    user_type,
    COUNT(*) as trip_count
FROM {{ ref('fct_divvy_trips') }}
GROUP BY 1, 2, 3
