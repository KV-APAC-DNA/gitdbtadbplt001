with source as (
    select * from {{ source('thasdl_raw', 'sdl_la_gt_sales_order_fact') }}
)

select * from source