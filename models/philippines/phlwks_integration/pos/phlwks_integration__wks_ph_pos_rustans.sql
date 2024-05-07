with sdl_ph_pos_rustans as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_pos_rustans') }}
),
final as (
    select * from sdl_ph_pos_rustans
)
select * from final