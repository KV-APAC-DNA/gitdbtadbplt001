with 
source as
(
    select * from {{source('jpnsdl_raw','sdl_pop6_jp_pop_lists')}}
),
final as
(
    select * from source
)
select * from final