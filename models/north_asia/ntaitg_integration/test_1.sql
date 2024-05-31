with source as
(
    select * from {{ source('ntasdl_raw', 'sdl_kr_sfmc_naver_data_additional') }}
),
final as
(
    select * from source
)
select * from final