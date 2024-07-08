with 
source as
(
    select * from {{ source('ntasdl_raw','sdl_pop6_hk_display_plans') }}
),
final as
(
    select * from source
)
select * from final