with sdl_pop6_sg_users as 
(
    select * from {{ source('sgpsdl_raw', 'sdl_pop6_sg_users') }}
),
final as (
    select * from sdl_pop6_sg_users
)
select * from final