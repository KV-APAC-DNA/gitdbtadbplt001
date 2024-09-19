with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_da_in_sang_sa_gt_sellout') }}
),
final as (
    select * from source
)
select * from final