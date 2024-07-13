with wks_skurecom_mi_actuals_tmp4 as(
    select * from {{ ref('indwks_integration__wks_skurecom_mi_actuals_tmp4') }}
),
edw_product_dim as(
    select * from {{ ref('indedw_integration__edw_product_dim') }}
),
final as(
    SELECT sku.*,
       pd.franchise_name,
       pd.brand_name,
       pd.product_category_name,
       pd.variant_name,
       pd.mothersku_name
FROM wks_skurecom_mi_actuals_tmp4 sku
LEFT JOIN (SELECT franchise_name,
                  brand_name,
                  product_category_name,
                  variant_name,
                  mothersku_name,
                  mothersku_code
           FROM edw_product_dim
           WHERE NVL(delete_flag,'XYZ') <> 'N'
           GROUP BY 1,
                    2,
                    3,
                    4,
                    5,
                    6) pd
       ON sku.mother_sku_cd = pd.mothersku_code
)
select * from final