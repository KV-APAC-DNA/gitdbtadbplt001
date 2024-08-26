with source as (
    select * from {{ source('thasdl_raw', 'sdl_th_gt_sales_order') }}
    where filename not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_sales_order__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_sales_order__duplicate_test') }}
            union all 
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_sales_order__test_format') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_sales_order__test_date_format_odd_eve_leap') }}
            )
)

select * from source