with 
source as
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_kr_displays') }}
),
final as
(
    select * from source
)
select * from final