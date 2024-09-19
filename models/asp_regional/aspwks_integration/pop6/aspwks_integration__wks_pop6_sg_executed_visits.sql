with 
source as
(
    select * from {{ source('sgpsdl_raw', 'sdl_pop6_sg_executed_visits') }}
),
final as
(
    select * from source
)
select * from final