with 
source as
(
    select * from {{ source('thasdl_raw', 'sdl_pop6_th_executed_visits') }}
),

final as
(
    select * from source
)
select * from final