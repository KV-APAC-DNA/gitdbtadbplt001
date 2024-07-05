with source as
(
    select * from {{ source('jpndclsdl_raw', 'sfcc_product_category') }}
),


final as
(
    select * from source
)

select * from final