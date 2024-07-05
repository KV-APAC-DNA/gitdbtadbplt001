with itg_sku_recom_flag_msl_spike_mi as(
    select * from {{ ref('inditg_integration__itg_sku_recom_flag_msl_spike_mi') }}
),
final as(
    SELECT sku.year,
       sku.quarter,
       sku.cust_cd,
       sku.retailer_cd,
       sku.mother_sku_cd,
       AVG(sku.oos_flag) AS oos_flag,
       AVG(sku.ms_flag) AS ms_flag,
       AVG(sku.cs_flag) AS cs_flag,
       AVG(sku.soq) AS soq
    FROM itg_sku_recom_flag_msl_spike_mi sku
    WHERE NOT (sku.cust_cd IS NULL OR sku.retailer_cd IS NULL)
    GROUP BY sku.year,
         sku.quarter,
         sku.cust_cd,
         sku.retailer_cd,
         sku.mother_sku_cd
)
select * from final