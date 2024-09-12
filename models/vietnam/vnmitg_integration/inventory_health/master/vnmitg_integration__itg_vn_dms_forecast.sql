{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['cycle', 'channel', 'territory_dist', 'warehouse', 'franchise', 'brand', 'variant'],
        pre_hook= "{% if is_incremental() %}
        delete from {{this}} where (cycle, channel, territory_dist, warehouse, franchise, brand, variant) in ( select cycle, channel, territory_dist, warehouse, franchise, brand, variant from {{ source('vnmsdl_raw', 'sdl_vn_dms_forecast') }} 
        where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_forecast__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_forecast__duplicate_test')}}
        )
        );{% endif %}"
    )
}}


with source as(
    select *, dense_rank() over (partition by cycle, channel, territory_dist, warehouse, franchise, brand, variant order by file_name desc) rnk 
    from {{ source('vnmsdl_raw', 'sdl_vn_dms_forecast') }}
    where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_forecast__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_forecast__duplicate_test')}}
    ) qualify rnk =1
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
        run_id::number(14,0) as run_id,
        file_name::varchar(255) as file_name
    FROM source
)
select * from transformed