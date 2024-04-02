{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['year','kpi','category'],
        pre_hook= " delete from {{this}} where (year, kpi, category) in ( select year, kpi, trim(category) from {{ source('vnmsdl_raw', 'sdl_vn_dms_yearly_target') }} );"
    )
}}

with source as(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_yearly_target') }}
),
final as(
    select
        year::number(18,0) as year,
        kpi::varchar(100) as kpi,
        TRIM(category)::varchar(200) as category,
        target::number(38,4) as target,
        curr_date::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        run_id::number(14,0) as run_id
    from source
)
select * from final