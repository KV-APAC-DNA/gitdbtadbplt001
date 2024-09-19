with sdl_pop6_tw_users as 
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_tw_users') }}
),
final as (
    select * from sdl_pop6_tw_users
)
select * from final