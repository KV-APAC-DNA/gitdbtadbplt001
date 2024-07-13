with 
source as
(
    select * from {{ source('thasdl_raw', 'sdl_pop6_th_displays') }}
),
final as
(
    select * from source
)
select * from final