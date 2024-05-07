with sdl_ph_pos_south_star as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_pos_south_star') }}
),
final as (
    select * from sdl_ph_pos_south_star
)
select * from final