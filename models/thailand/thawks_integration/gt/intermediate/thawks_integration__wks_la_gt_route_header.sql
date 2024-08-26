with source as (
    select * from {{ source('thasdl_raw', 'sdl_la_gt_route_header') }}
    where filename not in (
                select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_route_header__null_test') }}
                union all
                select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_route_header__duplicate_test') }}
                union all
                select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_route_header__test_format') }}
                union all
                select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_route_header__test_format_2') }}
                )
)

select * from source