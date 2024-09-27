with sdl_pop6_kr_users as 
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_kr_users') }}
    where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_kr_users__null_test') }}
        union all
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_kr_users__duplicate_test') }}
    )
),
final as (
    select * from sdl_pop6_kr_users
)
select * from final