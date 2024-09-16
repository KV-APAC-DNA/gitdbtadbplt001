{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('ntasdl_raw','sdl_kr_pos_lotte_mart') }}
    where filename not in (
<<<<<<< HEAD
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_pos_lotte_mart__null_test') }}
=======
    select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_pos_lotte_mart__null_test') }}
>>>>>>> c06f259266c6ae7a914ab047517ccfc0167f0cab
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