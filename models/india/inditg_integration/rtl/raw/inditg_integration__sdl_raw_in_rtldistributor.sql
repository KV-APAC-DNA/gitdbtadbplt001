{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as(
    select * from {{ source('indsdl_raw', 'sdl_in_rtldistributor') }} 
),
final as(
    select * from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
)

select * from final