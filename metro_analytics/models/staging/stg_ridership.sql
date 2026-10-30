

with source as (
    select
        STATION,
        CURRENT_IN_TOTAL,
        CURRENT_OUT_TOTAL,
        SNAPSHOT_DT
    from {{ source('wmata', 'raw_ridership') }}
),

renamed as (
    select
        STATION                                    as station_name,
        CURRENT_IN_TOTAL                           as entries,
        CURRENT_OUT_TOTAL                          as exits,
        -- SNAPSHOT_DT is an epoch in milliseconds from the ArcGIS archive;
        -- divide by 1000 to get seconds, then convert to a real timestamp.
        to_timestamp(SNAPSHOT_DT / 1000.0)         as snapshot_at,
        cast(to_timestamp(SNAPSHOT_DT / 1000.0) as date) as snapshot_date
    from source
)

select * from renamed