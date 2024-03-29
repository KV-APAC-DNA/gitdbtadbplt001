{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['cycle', 'channel', 'territory_dist', 'warehouse', 'franchise', 'brand', 'variant'],
        pre_hook= "delete from {{this}} where (cycle, channel, territory_dist, warehouse, franchise, brand, variant) in ( select cycle, channel, territory_dist, warehouse, franchise, brand, variant from {{ source('vnmsdl_raw', 'sdl_vn_dms_forecast') }} );"
    )
}}


with source as(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_forecast') }}
),
transformed as(
    SELECT
        cycle::number(10,0) as cycle,
        channel::varchar(30) as channel,
        territory_dist::varchar(100) as territory_dist,
        warehouse::varchar(100) as warehouse,
        franchise::varchar(100) as franchise,
        brand::varchar(100) as brand,
        variant::varchar(100) as variant,
        forecastso_mil::varchar(100) as forecastso_mil,
        curr_date::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) AS updt_dttm,
        run_id::number(14,0) as run_id
    FROM source
)
select * from transformed