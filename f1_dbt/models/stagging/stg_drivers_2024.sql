{{ config(materialized='view') }}

SELECT DISTINCT
    driver_number,
    broadcast_name,
    full_name,
    name_acronym,
    team_name,
    first_name,
    last_name,
    country_code
FROM public.raw_drivers rd
inner join {{ ref('stg_sessions') }} s on s.session_key = rd.session_key
WHERE s.year = 2024