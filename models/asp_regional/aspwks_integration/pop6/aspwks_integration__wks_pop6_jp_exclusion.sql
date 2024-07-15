with 
source as
(
    select * from {{ source('jpnsdl_raw', 'sdl_pop6_jp_exclusion') }}
),
final as
(
    select * from source
)
select * from final