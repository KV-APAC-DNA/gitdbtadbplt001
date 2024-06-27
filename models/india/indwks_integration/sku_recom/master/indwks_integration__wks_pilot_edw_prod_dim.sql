with edw_product_dim as
(
    select * from snapindedw_integration.edw_product_dim
    --{{ ref('indedw_integration__edw_product_dim') }}
),
final as
(
    SELECT franchise_name,
        brand_name,
        product_category_name,
        variant_name,
        mothersku_name,
        mothersku_code
    FROM edw_product_dim
    WHERE NVL(delete_flag,'XYZ') <> 'N'
    GROUP BY 1,2,3,4,5,6
)
select * from final