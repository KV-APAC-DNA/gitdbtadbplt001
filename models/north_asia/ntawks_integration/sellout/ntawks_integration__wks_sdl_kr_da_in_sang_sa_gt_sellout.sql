with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_da_in_sang_sa_gt_sellout') }}
    where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_gt_sellout_da_in_sang_sa__null_test') }}
    )
),
final as (
    select * from source
)
select * from final