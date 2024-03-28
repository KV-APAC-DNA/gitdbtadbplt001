{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['from_cycle','to_cycle','customer_code']
    )
}}

with source as (
    select * from {{ source('vnmsdl_raw','sdl_vn_dms_kpi') }}
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
    run_id::number(14,0) as run_id
    from source
)
select * from final