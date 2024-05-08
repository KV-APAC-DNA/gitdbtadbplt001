{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as (
     select * from {{ source('ntasdl_raw','sdl_tsi_target_data') }} ),
     
final as (
    select * from source
 {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.curr_dt > (select max(curr_dt) from {{ this }}) 
 {% endif %}
)

select * from final