with source as(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_dms_promotion_list') }}
),
final as(
    select * from source
)
select * from final