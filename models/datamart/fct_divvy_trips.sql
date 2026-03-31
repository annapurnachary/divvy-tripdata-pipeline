{{ config(
    materialized='table',
    partition_by={
      "field": "start_datetime",
      "data_type": "timestamp",
      "granularity": "month"
    },
    cluster_by=["start_station_id", "user_type"]
) }}

WITH staged_data AS (
    SELECT * FROM {{ ref('staging_divvy_trips') }}
)

SELECT 
    *,
    -- 1. Calculations from your first block
    TIMESTAMP_DIFF(end_datetime, start_datetime, MINUTE) AS duration_minutes,
    EXTRACT(HOUR FROM start_datetime) AS start_hour,
    FORMAT_TIMESTAMP('%A', start_datetime) AS day_of_week,
    
    -- 2. New logic from your second block (Round trips)
    CASE 
        WHEN start_station_id = end_station_id THEN 1 
        ELSE 0 
    END AS is_round_trip,
    
    -- 3. New logic from your second block (Commute periods)
    CASE 
        WHEN EXTRACT(HOUR FROM start_datetime) BETWEEN 7 AND 9 THEN 'Morning Rush'
        WHEN EXTRACT(HOUR FROM start_datetime) BETWEEN 16 AND 18 THEN 'Evening Rush'
        ELSE 'Off-Peak'
    END AS commute_period

FROM staged_data

