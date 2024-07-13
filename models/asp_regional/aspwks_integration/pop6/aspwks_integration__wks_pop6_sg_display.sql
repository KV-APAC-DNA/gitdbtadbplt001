with 
source as
(
    select * from {{ source('sgpsdl_raw', 'sdl_pop6_sg_displays') }}
),
final as
(
    select * from source
)
select * from final