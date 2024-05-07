with sdl_ph_pos_watsons as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_pos_watsons') }}
)
final as (
    select * from sdl_ph_pos_watsons
)
select * from final