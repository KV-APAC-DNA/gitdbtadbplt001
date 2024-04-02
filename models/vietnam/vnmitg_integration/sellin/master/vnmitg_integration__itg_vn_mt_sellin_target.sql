{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['filename']
    )
}}
with source as
(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_mt_sellin_target') }}
),
final as
(
    SELECT 
        mtd_code::varchar(20) as mtd_code,
        mti_code::varchar(20) as mti_code,
        target::number(20,5) as target,
        sellin_cycle::number(18,0) as sellin_cycle,
        sellin_year::varchar(10) as sellin_year,
        visit::varchar(10) as visit,
        filename::varchar(100) as filename,
        run_id::number(14,0) as run_id,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        current_timestamp::timestamp_ntz(9) as updt_dttm
    FROM source
)
select * from final