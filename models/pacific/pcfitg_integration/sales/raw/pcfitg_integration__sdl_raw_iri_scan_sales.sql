{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}

with source as (
    select * from {{ source('pcfsdl_raw','sdl_iri_scan_sales') }}
    where filename not in (
        select distinct file_name from {{source('pcfwks_integration','TRATBL_sdl_iri_scan_sales__duplicate_test')}}
    )
),
final as (
    select * from source
 {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
 {% endif %}
)

select * from final