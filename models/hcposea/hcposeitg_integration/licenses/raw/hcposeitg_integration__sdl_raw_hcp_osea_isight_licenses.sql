{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}
with source as (
    select * from {{ source('hcposesdl_raw','sdl_hcp_osea_isight_licenses') }}
),
final as (
    select * from source
 {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }})
 {% endif %}
)
select * from final