{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('ntasdl_raw','sdl_kr_pos_olive_young') }}
<<<<<<< HEAD
     where filename not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_pos_olive_young__null_test') }}
=======
    where filename not in (
            select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_pos_olive_young__null_test') }}
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