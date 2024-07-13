with 
source as
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_kr_product_groups_lists') }}
),
final as
(
    select * from source
)
select * from final