with 
source as
(
    select * from {{ source('sgpsdl_raw', 'sdl_pop6_sg_displays') }}
    where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_sg_displays__duplicate_test') }}
        union all
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_sg_displays__null_test') }}
    )
),
final as
(
    select * from source
)
select * from final