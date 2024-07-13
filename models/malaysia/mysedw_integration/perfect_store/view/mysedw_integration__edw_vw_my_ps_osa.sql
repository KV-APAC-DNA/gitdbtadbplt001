with source as 
(
    select * from {{ ref('mysitg_integration__itg_my_perfectstore_osa') }}
),

final as 
(
    SELECT 
        yearmo,
        date,
        channel,
        chain,
        region,
        outlet,
        outlet_no,
        category,
        brand,
        sub_category,
        sub_brand,
        packsize,
        product_barcode,
        sku_description,
        answer
    FROM source
)
select * from final