with source as(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_dksh_daily_sales') }}
),
final as(
    select * from source
)

select * from final