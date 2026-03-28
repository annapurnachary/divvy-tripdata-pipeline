SELECT 
    start_station_name,
    end_station_name,
    COUNT(*) as total_trips
FROM {{ ref('fct_divvy_trips') }}
WHERE start_station_name != end_station_name  -- Exclude round trips for route analysis
GROUP BY 1, 2
ORDER BY total_trips DESC
