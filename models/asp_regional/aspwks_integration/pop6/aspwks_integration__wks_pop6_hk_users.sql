with sdl_pop6_hk_users as 
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_hk_users') }}
),
final as (
    select * from sdl_pop6_hk_users
)
select * from final