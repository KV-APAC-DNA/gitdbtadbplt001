with source as (
    select * from {{ source('thasdl_raw', 'sdl_la_gt_route_detail') }}
        where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_route_detail__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_route_detail__duplicate_test') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_route_detail__test_format_created_date') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_route_detail__test_format_sale_unit') }}
        )
)

select * from source