with 
source as
(
    select * from {{ source('sgpwks_integration', 'sdl_pop6_sg_pops') }}
),
final as
(
    select * from source
)
select * from final