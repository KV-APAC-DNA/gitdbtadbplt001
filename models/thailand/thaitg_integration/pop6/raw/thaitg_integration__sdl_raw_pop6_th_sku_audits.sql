{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_pop6_th_sku_audits') }}
    where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_pop6_th_sku_audits__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_pop6_th_sku_audits__duplicate_test') }}
    )
    
),
final as(
    select * from source
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
)
select * from final