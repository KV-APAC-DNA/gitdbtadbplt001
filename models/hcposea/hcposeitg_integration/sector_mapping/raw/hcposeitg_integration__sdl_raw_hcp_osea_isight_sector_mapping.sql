{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}
with source as (
    select * from {{ source('hcposesdl_raw','sdl_hcp_osea_isight_sector_mapping') }}
    where filename not in (
            select distinct file_name from {{ source('hcposewks_integration', 'TRATBL_sdl_hcp_osea_isight_sector_mapping__null_test') }}
            union all
            select distinct file_name from {{ source('hcposewks_integration', 'TRATBL_sdl_hcp_osea_isight_sector_mapping__duplicate_test') }}
    )
),
final as (
    select * from source
 {% if is_incremental() %}
    where source.crt_dttm > (select max(crt_dttm) from {{ this }})
 {% endif %}
)
select * from final