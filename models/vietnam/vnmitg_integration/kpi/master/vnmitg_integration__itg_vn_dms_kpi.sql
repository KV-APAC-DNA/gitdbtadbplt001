{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['dstrbtr_id','cycle','kpi_type','saleman_code']
    )
}}

with source as (
    select *,dense_rank() over (partition by dstrbtr_id, cycle, kpi_type,saleman_code order by file_name desc) rnk 
    from {{ source('vnmsdl_raw','sdl_vn_dms_kpi') }}
    where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_kpi__duplicate_test')}}
        union all
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dms_kpi__null_test')}}
    ) qualify rnk = 1
),

final as
(
    select
    dstrbtr_id::varchar(30) as dstrbtr_id,
    saleman_code::varchar(50) as saleman_code,
    saleman_name::varchar(50) as saleman_name,
    cycle::number(18,0) as cycle,
    to_date(to_timestamp_ntz(export_date,'mm/dd/yyyy hh12:mi:ss pm')) as export_date,
    kpi_type::varchar(20) as kpi_type,
    target_value::number(15,4) as target_value,
    actual_value::number(15,4) as actual_value,
    current_timestamp()::timestamp(9) as crtd_dttm,
    current_timestamp()::timestamp(9) as updt_dttm,
    run_id::number(14,0) as run_id,
    file_name::varchar(255) as file_name
    from source
)
select * from final