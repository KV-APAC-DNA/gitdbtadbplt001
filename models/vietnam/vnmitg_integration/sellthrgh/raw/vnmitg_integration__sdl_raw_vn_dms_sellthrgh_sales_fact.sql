{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_sellthrgh_sales_fact') }}
),
final as(
    select * from source
 {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.curr_date > (select max(curr_date) from {{ this }}) 
 {% endif %}
)

select * from final