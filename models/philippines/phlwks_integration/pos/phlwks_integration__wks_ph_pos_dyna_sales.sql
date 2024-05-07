with sdl_ph_pos_dyna_sales as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_pos_dyna_sales') }}
)
final as (
    select * from sdl_ph_pos_dyna_sales
)
select * from final