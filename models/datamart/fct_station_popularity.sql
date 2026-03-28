SELECT 
    start_station_name,
    user_type,
    start_lat,
    start_lng,
    COUNT(*) as total_trips,
    ROUND(AVG(duration_minutes), 2) as avg_trip_duration
FROM {{ ref('fct_divvy_trips') }}
GROUP BY 1, 2, 3, 4
