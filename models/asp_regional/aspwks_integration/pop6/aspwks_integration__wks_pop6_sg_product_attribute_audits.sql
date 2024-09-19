with 
source as
(
    select * from {{ source('sgpsdl_raw', 'sdl_pop6_sg_product_attribute_audits') }}
),
final as
(
    select * from source
)
select * from final