with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_du_bae_ro_yu_tong_gt_sellout') }}
    where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_du_bae_ro_yu_tong_gt_sellout__null_test') }}
    )
),
final as (
    select * from source
)
select * from final