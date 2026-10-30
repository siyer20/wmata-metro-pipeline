-- Staging layer for raw WMATA realtime predictions.
-- Grain: one row per train prediction per fetch (fetched_at, location_code, line, destination_name).
--
-- Two fields arrive as VARCHAR and carry non-numeric sentinels from the WMATA API:
--   min        -> numeric minutes-to-arrival, but also "ARR", "BRD", "---", "-"
--   car_count  -> numeric car count, but also "---", "-"
-- Both are kept raw here and also exposed as cleaned integers (sentinels -> NULL).
-- The cleaning lives in staging so downstream marts can decide how to treat
-- missing predictions without silently re-implementing the sentinel logic.

with source as (

    select
        fetched_at,
        line,
        destination_name,
        min,
        car_count,
        location_code,
        location_name
    from {{ source('wmata', 'raw_realtime_predictions') }}

),

renamed as (

    select
        fetched_at,
        line,
        destination_name,
        location_code,
        location_name,

        -- raw string kept for auditability
        min                         as min_raw,
        -- cleaned numeric minutes; non-numeric sentinels become NULL
        try_cast(min as integer)    as min_numeric,

        car_count                   as car_count_raw,
        try_cast(car_count as integer) as car_count_numeric

    from source

)

select * from renamed