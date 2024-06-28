with source as 
(
    select * from snaposeitg_integration.itg_my_perfectstore_sos
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
        sku_description,
        answer
    FROM source
)
select * from final