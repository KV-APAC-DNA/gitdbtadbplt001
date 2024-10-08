with 
source as
(
    select * from {{ source('thasdl_raw', 'sdl_pop6_th_displays') }}
    where file_name not in (
        select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_pop6_th_displays__null_test') }}
        union all
        select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_pop6_th_displays__duplicate_test') }}
    )
),
final as
(
    select * from source
)
select * from final