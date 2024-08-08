with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_lotte_logistics_yang_ju_gt_sellout') }}
),
final as (
    select * from source
)
select * from final