with 
source as
(
    select * from {{ source('thasdl_raw', 'sdl_pop6_th_general_audits') }}
),
final as
(
    select * from source
)
select * from final