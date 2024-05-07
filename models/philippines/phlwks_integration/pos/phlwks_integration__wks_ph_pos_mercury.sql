with sdl_ph_pos_mercury as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_pos_mercury') }}
)
final as (
    select * from sdl_ph_pos_mercury
)
select * from final