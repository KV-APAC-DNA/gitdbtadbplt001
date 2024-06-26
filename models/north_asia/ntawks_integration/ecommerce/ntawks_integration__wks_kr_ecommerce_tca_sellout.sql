with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_ecommerce_tca_sellout') }}
),
final as (
    select * from source
)
select * from final