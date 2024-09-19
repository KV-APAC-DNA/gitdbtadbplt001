with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_du_bae_ro_yu_tong_gt_sellout') }}
),
final as (
    select * from source
)
select * from final