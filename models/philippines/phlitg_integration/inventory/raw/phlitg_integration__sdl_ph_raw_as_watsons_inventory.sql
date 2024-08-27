{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('phlsdl_raw', 'sdl_ph_as_watsons_inventory') }}
    where file_name not in (
        select distinct file_name from {{SOURCE('phlwks_integration','TRATBL_sdl_ph_as_watsons_inventory__test_null__ff')}}
        union all
        select distinct file_name from {{SOURCE('phlwks_integration','TRATBL_sdl_ph_as_watsons_inventory__test_duplicate__ff')}}
        union all
        select distinct file_name from {{SOURCE('phlwks_integration','TRATBL_sdl_ph_as_watsons_inventory__test_lookup__ff')}}
    )
),
final as
(
    select * from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
)
select * from final
