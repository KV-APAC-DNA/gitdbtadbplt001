with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_il_dong_hu_di_s_deok_seong_sang_sa_gt_sellout') }}
),
final as (
    select * from source
)
select * from final