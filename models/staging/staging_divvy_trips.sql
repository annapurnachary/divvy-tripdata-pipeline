{{ config(materialized='view') }}

WITH trip_data AS 
(
  SELECT *,
    row_number() OVER(PARTITION BY ride_id, started_at) as rn
  FROM {{ source('staging','external_divvy_data') }}
  WHERE ride_id IS NOT NULL
)
SELECT
    -- 1. Identifiers
    CAST(ride_id AS STRING) AS ride_id,
    CAST(rideable_type AS STRING) AS rideable_type,

    -- 2. Timestamps (Converted from STRING to TIMESTAMP)
    CAST(started_at AS TIMESTAMP) AS start_datetime,
    CAST(ended_at AS TIMESTAMP) AS end_datetime,

    -- 3. Station Info (Handling Nulls with COALESCE)
    COALESCE(start_station_name, 'Unknown') AS start_station_name,
    COALESCE(start_station_id, 'Unknown') AS start_station_id,
    COALESCE(end_station_name, 'In-Progress/Unknown') AS end_station_name,
    COALESCE(end_station_id, 'In-Progress/Unknown') AS end_station_id,

    -- 4. Coordinates (Ensuring FLOAT64 for Mapping)
    CAST(start_lat AS FLOAT64) AS start_lat,
    CAST(start_lng AS FLOAT64) AS start_lng,
    CAST(end_lat AS FLOAT64) AS end_lat,
    CAST(end_lng AS FLOAT64) AS end_lng,

    -- 5. User Type
    CAST(member_casual AS STRING) AS user_type,

    -- 6. New Features (Feature Engineering)
    TIMESTAMP_DIFF(CAST(ended_at AS TIMESTAMP), CAST(started_at AS TIMESTAMP), MINUTE) AS duration_minutes

FROM trip_data
WHERE rn = 1 
  -- Data Quality: Filter out "ghost rides" (trips < 1 minute)
  AND TIMESTAMP_DIFF(CAST(ended_at AS TIMESTAMP), CAST(started_at AS TIMESTAMP), SECOND) > 60
  -- Data Quality: Chicago Coordinate Bounds
  AND CAST(start_lat AS FLOAT64) > 40 
  AND CAST(start_lng AS FLOAT64) < -80