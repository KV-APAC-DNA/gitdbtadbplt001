{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['year','kpi','category'],
        pre_hook= " {%if is_incremental()%}
        delete from {{this}} where (year, kpi, category) in ( select year, kpi, trim(category) from {{ source('vnmsdl_raw', 'sdl_vn_dms_yearly_target') }} where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_yearly_target__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_yearly_target__duplicate_test')}}));
        {% endif %}"
    )
}}

with source as(
    select *, dense_rank() over (partition by year, kpi, trim(category) order by file_name desc) rnk
     from {{ source('vnmsdl_raw', 'sdl_vn_dms_yearly_target') }}
    where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_yearly_target__null_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_yearly_target__duplicate_test')}}
    ) qualify rnk = 1
), 
final as(
    select
        year::number(18,0) as year,
        kpi::varchar(100) as kpi,
        TRIM(category)::varchar(200) as category,
        target::number(38,4) as target,
        curr_date::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        run_id::number(14,0) as run_id,
        file_name::varchar(255) as file_name
    from source
)
select * from final