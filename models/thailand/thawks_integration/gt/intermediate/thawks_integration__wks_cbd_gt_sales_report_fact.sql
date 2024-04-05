with source as (
    select * from {{ source('thasdl_raw', 'sdl_cbd_gt_sales_report_fact') }}
)

select * from source