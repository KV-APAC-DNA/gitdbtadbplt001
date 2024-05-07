with sdl_ph_pos_711 as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_pos_711') }}
)
final as (
    select * from sdl_ph_pos_711
)
select * from final