with source as (
    select * from {{ source('thasdl_raw', 'sdl_th_gt_sales_order') }}
)

select * from source