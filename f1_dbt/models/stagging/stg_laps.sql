{{ config(materialized='view') }}

SELECT
    session_key,
    driver_number,
    lap_number,
    lap_duration,
    is_pit_out_lap,
    date_start::timestamp
FROM public.raw_laps
