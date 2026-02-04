{{ config(materialized='view') }}

SELECT
    session_key,
    session_name,
    session_type,
    circuit_key,
    date_start::timestamp,
    date_end::timestamp,
    country_name,
    location,
    year
FROM public.raw_sessions
