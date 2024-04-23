{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_fsni_sales') }}
),
final as(
    select * from source
 {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
 {% endif %}
)

select * from final