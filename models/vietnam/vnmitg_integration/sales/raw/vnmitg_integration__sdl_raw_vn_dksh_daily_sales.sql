with source as(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_dksh_daily_sales') }}
    where file_name not in (
        select distinct file_name from {{source('vnmwks_integration','TRATBL_sdl_vn_dksh_daily_sales__null_test')}}
    )
),
final as(
    select * from source
)

select * from final