with source as
(
    select * from {{ source('jpdclsdl_raw', 'sfcc_product_mst') }}
),


final as
(
    select * from source
)

select * from final