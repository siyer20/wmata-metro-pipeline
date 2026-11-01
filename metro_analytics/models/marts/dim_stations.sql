-- Station dimension.
--
-- Backbone: the 98-station reference from DC GIS layer 6 (NAME, WMATA_NAME, LINE,
-- GIS_ID). Realtime location names that don't reconcile with layer 6 after
-- normalization are resolved through an explicit crosswalk where a mapping is
-- justified, and unioned in as additional rows only when genuinely absent.
--
-- Join key: normalized_name (uppercase, punctuation and whitespace stripped).
-- Raw names differ across sources ("Navy Yard-Ballpark" vs "NAVY YARD"), so
-- exact string equality would silently drop fact rows.
--
-- Note: layer 6's LINE field is a comma-separated multi-line string
-- (e.g. "blue, yellow"), kept as a display attribute rather than split here.

with layer6 as (

    select
        NAME                                     as station_name,
        WMATA_NAME,
        LINE,
        GIS_ID,
        regexp_replace(upper(trim(WMATA_NAME)), '[^A-Z0-9 ]', '', 'g') as normalized_name,
        'layer6'                                 as source
    from {{ source('wmata', 'raw_stations') }}

),

-- Explicit mappings from realtime location_name to the layer 6 WMATA_NAME it
-- refers to. Each pair was verified against raw_stations; a mapping exists only
-- where a specific layer 6 row is unambiguously the same station.
crosswalk as (

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

realtime_resolved as (

    select distinct
        r.location_name                          as realtime_name,
        coalesce(c.layer6_name, r.location_name) as resolved_name
    from {{ source('wmata', 'raw_realtime_predictions') }} r
    left join crosswalk c
        on r.location_name = c.realtime_name

),

realtime_only as (

    select distinct
        resolved_name                            as station_name,
        null                                     as WMATA_NAME,
        null                                     as LINE,
        null                                     as GIS_ID,
        regexp_replace(upper(trim(resolved_name)), '[^A-Z0-9 ]', '', 'g') as normalized_name,
        'realtime_only'                          as source
    from realtime_resolved
    where regexp_replace(upper(trim(resolved_name)), '[^A-Z0-9 ]', '', 'g')
          not in (select normalized_name from layer6)

),

unioned as (

    select * from layer6
    union all
    select * from realtime_only

),

keyed as (

    select
        row_number() over (order by normalized_name) as station_key,
        *
    from unioned

)

select * from keyed