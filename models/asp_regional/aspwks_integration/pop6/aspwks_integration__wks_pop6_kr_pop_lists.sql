with 
source as
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_kr_pop_lists') }}
),
final as
(
    select * from source
)
select * from final