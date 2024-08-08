with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_hyundai_gt_sellout') }}
),
final as (
    select * from source
)
select * from final