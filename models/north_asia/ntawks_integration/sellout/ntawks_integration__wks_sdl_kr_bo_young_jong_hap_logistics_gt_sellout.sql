with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_bo_young_jong_hap_logistics_gt_sellout') }}
),
final as (
    select * from source
)
select * from final