with sdl_ph_pos_dyna_sales as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_pos_dyna_sales') }}
     where file_name not in (
    select distinct file_name from {{ source('phlwks_integration', 'TRATBL_sdl_ph_pos_dyna_sales__null_test') }} 
    union all
    select distinct file_name from {{ source('phlwks_integration', 'TRATBL_sdl_ph_pos_dyna_sales__lookup_test') }} )
),
final as (
    select * from sdl_ph_pos_dyna_sales
)
select * from final