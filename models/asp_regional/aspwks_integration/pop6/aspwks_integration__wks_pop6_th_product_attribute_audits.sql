with 
source as
(
    select * from {{ source('thasdl_raw', 'sdl_pop6_th_product_attribute_audits') }}
),
final as
(
    select * from source
)
select * from final