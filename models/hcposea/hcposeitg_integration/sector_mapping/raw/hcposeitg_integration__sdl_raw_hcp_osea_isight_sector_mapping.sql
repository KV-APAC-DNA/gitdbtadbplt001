{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}
with source as (
    select * from {{ source('hcposesdl_raw','sdl_hcp_osea_isight_sector_mapping') }}
),
final as (
    select * from source
 {% if is_incremental() %}
    where source.crt_dttm > (select max(crt_dttm) from {{ this }})
 {% endif %}
)
select * from final