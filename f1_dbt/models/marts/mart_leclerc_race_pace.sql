{{ config(materialized='table') }}

WITH leclerc_laps AS (
    SELECT 
        s.location,
        s.country_name,
        s.session_name,
        s.date_start,
        l.lap_duration,
        l.lap_number
    FROM {{ ref('stg_laps') }} l
    LEFT JOIN {{ ref('stg_sessions') }} s 
        ON l.session_key = s.session_key
    LEFT JOIN {{ ref('stg_drivers_2025') }} d 
        ON l.driver_number = d.driver_number
    WHERE s.session_type = 'Race' and s.year = 2025
      AND l.lap_duration IS NOT NULL
      AND d.name_acronym = 'LEC'
)

SELECT
    location,
    country_name,
    session_name,
    date_start,
    COUNT(*) as total_laps,
    ROUND(AVG(lap_duration)::NUMERIC, 3) as avg_lap_ms,
    MIN(lap_duration) as best_lap_ms,
    MAX(lap_duration) as worst_lap_ms,
    STDDEV(lap_duration) as consistency_ms
FROM leclerc_laps
GROUP BY 1,2,3,4
ORDER BY date_start DESC
