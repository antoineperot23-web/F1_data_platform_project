{{ config(materialized='table') }}

WITH laps_clean AS (
    SELECT *
    FROM {{ ref('stg_laps') }}
    WHERE lap_duration IS NOT NULL
)

SELECT
    session_key,
    driver_number,
    ROUND(AVG(lap_duration)::NUMERIC, 3) AS avg_lap_ms,
    MIN(lap_duration) AS best_lap_ms,
    MAX(lap_duration) AS worst_lap_ms,
    COUNT(*)          AS lap_count
FROM laps_clean
GROUP BY session_key, driver_number
ORDER BY session_key, best_lap_ms
