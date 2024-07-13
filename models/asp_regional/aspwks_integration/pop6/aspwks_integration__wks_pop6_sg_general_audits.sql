with 
source as
(
    select * from {{ source('sgpsdl_raw', 'sdl_pop6_sg_general_audits') }}
),
final as
(
    select * from source
)
select * from final