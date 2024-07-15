with 
source as
(
    select * from {{source('sgpsdl_raw','sdl_pop6_sg_display_plans')  }}
),
final as
(
    select * from source
)
select * from final