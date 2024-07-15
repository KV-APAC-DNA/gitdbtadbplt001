with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_ecommerce_unitoa_sellout') }}
),
final as (
    select * from source
)
select * from final