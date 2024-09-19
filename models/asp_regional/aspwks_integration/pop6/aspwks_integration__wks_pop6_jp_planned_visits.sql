with 
source as
(
    select * from {{ source('jpnsdl_raw', 'sdl_pop6_jp_planned_visits') }}
),
final as
(
    select * from source
)
select * from final