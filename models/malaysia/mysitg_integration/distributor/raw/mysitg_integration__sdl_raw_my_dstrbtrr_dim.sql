{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as (
     select * from {{ source('myssdl_raw','sdl_my_dstrbtrr_dim') }} where file_name not in
    ( select distinct file_name from {{ source('myswks_integration', 'TRATBL_sdl_my_dstrbtrr_dim__null_test') }}
      union all 
      select distinct file_name from {{ source('myswks_integration', 'TRATBL_sdl_my_dstrbtrr_dim__duplicate_test') }}
    ) ),
     
final as (
    select * from source
 {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.curr_dt > (select max(curr_dt) from {{ this }}) 
 {% endif %}
)

select * from final