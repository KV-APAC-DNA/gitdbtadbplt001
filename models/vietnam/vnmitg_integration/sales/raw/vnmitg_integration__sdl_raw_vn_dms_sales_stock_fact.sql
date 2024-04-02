with source as(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_sales_stock_fact') }}
),
final as(
    select * from source
)

select * from final