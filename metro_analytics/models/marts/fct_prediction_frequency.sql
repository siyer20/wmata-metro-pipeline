-- Prediction frequency fact at station x fetch grain.
--
-- One row per station (location_code) per fetch (fetched_at), aggregating the
-- individual train predictions observed at that moment.
--
-- Uses both cleaned and raw min columns by design:
--   avg(min_numeric)                -- sentinels are NULL, so the mean is not
--                                      corrupted by ARR/BRD/---
--   count(*) filter (min_raw='BRD') -- boarding trains, answerable only from
--                                      the preserved raw string
-- Collapsing sentinels to NULL in staging would have made the second metric
-- impossible.
--
-- The crosswalk below is duplicated from dim_stations. It is needed because
-- dim_stations stores the layer 6 normalized name for crosswalked stations,
-- while this model would otherwise derive a normalized key from the raw
-- realtime name. Duplication is acceptable at two models; if a third model
-- needs it, move it to a seed.

with crosswalk as (

    select * from (values
        ('Arlington Cemetery',                            'ARLINGTON-CEMETERY'),
        ('King St-Old Town',                              'KING STREET'),
        ('Mt Vernon Sq 7th St-Convention Center',         'MT VERNON SQ / 7TH ST-CONVENTION CENTER'),
        ('Navy Yard-Ballpark',                            'NAVY YARD'),
        ('Southern Avenue',                               'SOUTHERN AVE.'),
        ('Tysons',                                        'TYSONS CORNER'),
        ('U Street/African-Amer Civil War Memorial/Cardozo', 'U ST / AFRICAN-AMER CIVIL WAR MEMORIAL / CARDOZO'),
        ('Waterfront',                                    'WATERFRONT-SEU'),
        ('West Falls Church',                             'WEST FALLS CHURCH-VT / UVA'),
        ('Woodley Park-Zoo/Adams Morgan',                 'WOODLEY PARK-ZOO / ADAMS MORGAN')
    ) as t(realtime_name, layer6_name)

),

predictions as (

    select
        fetched_at,
        location_code,
        location_name,
        line,
        min_raw,
        min_numeric,
        car_count_numeric,
        coalesce(c.layer6_name, p.location_name) as resolved_name
    from {{ ref('stg_realtime') }} p
    left join crosswalk c
        on p.location_name = c.realtime_name

),

stations as (

    select
        station_key,
        normalized_name
    from {{ ref('dim_stations') }}

),

aggregated as (

    select
        location_code,
        location_name,
        fetched_at,
        count(*)                                          as prediction_count,
        avg(min_numeric)                                  as avg_min_to_arrival,
        min(min_numeric)                                  as min_min_to_arrival,
        count(*) filter (where min_raw = 'BRD')           as boarding_count,
        count(*) filter (where min_raw = 'ARR')           as arriving_count,
        count(distinct line)                              as lines_served,
        avg(car_count_numeric)                            as avg_car_count,
        regexp_replace(upper(trim(resolved_name)), '[^A-Z0-9 ]', '', 'g') as normalized_name
    from predictions
    group by location_code, location_name, fetched_at, resolved_name

)

select
    a.location_code,
    a.location_name,
    s.station_key,
    a.fetched_at,
    a.prediction_count,
    a.avg_min_to_arrival,
    a.min_min_to_arrival,
    a.boarding_count,
    a.arriving_count,
    a.lines_served,
    a.avg_car_count
from aggregated a
-- using left join, just in case station doesn't reconcile w dim, the fact row 
-- survives with a NULL station_key val instead of being lost
left join stations s
    on a.normalized_name = s.normalized_name