with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_ecommerce_offtake_coupang_transaction') }}
),
final as (
    select * from source
)
select * from final