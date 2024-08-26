with source as (
    select * from {{ source('thasdl_raw', 'sdl_cbd_gt_sales_report_fact') }}
    where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_cbd_gt_sales_report_fact__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_cbd_gt_sales_report_fact__duplicate_test') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_cbd_gt_sales_report_fact__test_format') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_cbd_gt_sales_report_fact__test_format_2') }}
        )
)

select * from source