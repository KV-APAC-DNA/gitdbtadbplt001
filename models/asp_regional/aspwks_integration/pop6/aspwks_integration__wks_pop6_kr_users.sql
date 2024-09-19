with sdl_pop6_kr_users as 
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_kr_users') }}
),
final as (
    select * from sdl_pop6_kr_users
)
select * from final