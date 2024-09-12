with sdl_ph_pos_711 as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_pos_711') }}
     where file_name not in (
    select distinct file_name from {{ source('phlwks_integration', 'TRATBL_sdl_ph_pos_711__null_test') }} 
    union all
    select distinct file_name from {{ source('phlwks_integration', 'TRATBL_sdl_ph_pos_711__lookup_test') }} )
),
final as (
    select * from sdl_ph_pos_711
)
select * from final