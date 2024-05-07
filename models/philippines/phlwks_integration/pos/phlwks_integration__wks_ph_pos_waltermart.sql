with sdl_ph_pos_waltermart as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_pos_waltermart') }}
)
final as (
    select * from sdl_ph_pos_waltermart
)
select * from final