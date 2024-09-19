with sdl_pop6_jp_users as 
(
    select * from {{ source('jpnsdl_raw', 'sdl_pop6_jp_users') }}
),
final as (
    select * from sdl_pop6_jp_users
)
select * from final