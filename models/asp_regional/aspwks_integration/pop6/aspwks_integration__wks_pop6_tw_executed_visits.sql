with 
source as
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_tw_executed_visits') }}
),
final as
(
    select * from source
)
select * from final