{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('ntasdl_raw','sdl_tw_pos_7eleven') }}

),
final as(
    select *,null as run_id from source
 {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
 {% endif %}
)

select * from final