-- Ridership fact at station x snapshot grain.
--
-- stg_ridership is already one row per station per snapshot; the measure is
-- the point-in-time entry/exit count, so there is nothing to aggregate here.
-- The fact's purpose is to carry station_key so the dimension relationship is
-- explicit and downstream joins are against the dimension rather than names.
--
-- Join is inner: all three ridership stations reconcile to layer 6 through
-- normalized_name (verified), so no rows are dropped.

with ridership as (

    select
        station_name,
        entries,
        exits,
        snapshot_at,
        snapshot_date,
        regexp_replace(upper(trim(station_name)), '[^A-Z0-9 ]', '', 'g') as normalized_name
    from {{ ref('stg_ridership') }}

),

stations as (

    select
        station_key,
        normalized_name
    from {{ ref('dim_stations') }}

)

select
    r.station_name,
    s.station_key,
    r.entries,
    r.exits,
    r.snapshot_at,
    r.snapshot_date
from ridership r
inner join stations s
    on r.normalized_name = s.normalized_name