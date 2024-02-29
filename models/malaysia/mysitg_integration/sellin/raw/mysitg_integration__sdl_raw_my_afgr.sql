{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ ref('myswks_integration__wks_sdl_my_afgr') }}
),
final as(
    select * from source
 {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.curr_dt > (select max(curr_dt) from {{ this }}) 
 {% endif %}
)

select * from final