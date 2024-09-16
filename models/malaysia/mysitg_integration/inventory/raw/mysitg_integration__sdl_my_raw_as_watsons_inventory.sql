{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('myssdl_raw','sdl_my_as_watsons_inventory') }} where filename not in
    ( 
      select distinct file_name from {{ source('myswks_integration', 'TRATBL_sdl_my_as_watsons_inventory__duplicate_test') }}
      union all
      select distinct file_name from {{ source('myswks_integration', 'TRATBL_sdl_my_as_watsons_inventory__null_test') }} )
),
final as(
    select * from source
 {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
 {% endif %}
)

select * from final