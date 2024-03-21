with source as(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_product_dim') }}
),
final as(
    select * from source
)
select * from final