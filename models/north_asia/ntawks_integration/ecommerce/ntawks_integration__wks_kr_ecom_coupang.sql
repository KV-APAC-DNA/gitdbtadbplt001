with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_ecom_coupang') }}
),
final as (
    select * from source
)
select * from final