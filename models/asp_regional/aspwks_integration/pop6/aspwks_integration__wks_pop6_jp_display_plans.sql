with 
source as
(
    select * from {{source('jpnsdl_raw','sdl_pop6_jp_display_plans')  }}
),
final as
(
    select * from source
)
select * from final