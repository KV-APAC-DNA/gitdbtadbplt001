{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_fssi_sales') }}
    where file_name not in (
        select distinct file_name from {{source('pcfwks_integration','TRATBL_sdl_perenso_fssi_sales__null_test')}}
    )
),
final as(
    select * from source
 {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
 {% endif %}
)

select * from final