{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ["dstrbtr_id", "cycle", "ordertype"]
    )
}}
with source as
(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_kpi_sellin_sellthrgh') }}
),
final as
(
    select 
        dstrbtr_id::varchar(30) as dstrbtr_id,
        dstrbtr_type::varchar(10) as dstrbtr_type,
        dstrbtr_name::varchar(100) as dstrbtr_name,
        cycle::number(18,0) as cycle,
        ordertype::varchar(30) as ordertype,
        sellin_tg::number(15,4) as sellin_tg,
        sellin_ac::number(15,4) as sellin_ac,
        curr_date::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        run_id::number(14,0) as run_id
    from source
)
select * from final