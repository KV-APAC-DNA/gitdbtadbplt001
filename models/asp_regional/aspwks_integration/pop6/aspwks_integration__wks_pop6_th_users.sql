with sdl_pop6_th_users as 
(
    select * from {{ source('thasdl_raw', 'sdl_pop6_th_users_test') }}
),
final as (
    select * from sdl_pop6_th_users
)
select * from final