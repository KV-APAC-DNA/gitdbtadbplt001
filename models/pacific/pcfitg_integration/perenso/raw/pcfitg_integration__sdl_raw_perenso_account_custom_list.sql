{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_account_custom_list') }}
),
final as(
    select * from source
 {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.create_dt > (select max(create_dt) from {{ this }}) 
 {% endif %}
)

select * from final