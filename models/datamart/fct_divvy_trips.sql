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
    * 
FROM staged_data
