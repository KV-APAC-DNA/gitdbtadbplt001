with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_ju_hj_life_gt_sellout') }}
),
final as (
    select * from source
)
select * from final