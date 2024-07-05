with itg_sku_recom_flag_msl_spike as
(
    select * from {{ ref('inditg_integration__itg_sku_recom_flag_msl_spike') }}
),
final as
(
    SELECT sku.mnth_id,
       sku.cust_cd,
       sku.retailer_cd,
       sku.mother_sku_cd,
       AVG(sku.oos_flag) AS oos_flag,
       AVG(sku.ms_flag) AS ms_flag,
       AVG(sku.cs_flag) AS cs_flag,
       AVG(sku.soq) AS soq,
       TO_DATE((sku.mnth_id)::CHARACTER VARYING::TEXT||'01','YYYYMMDD'::CHARACTER VARYING::TEXT) AS reco_date
FROM itg_sku_recom_flag_msl_spike sku
WHERE NOT (sku.cust_cd IS NULL OR sku.retailer_cd IS NULL)
GROUP BY sku.mnth_id,
         sku.cust_cd,
         sku.retailer_cd,
         sku.mother_sku_cd,
         TO_DATE((sku.mnth_id)::CHARACTER VARYING::TEXT||'01','YYYYMMDD'::CHARACTER VARYING::TEXT)
)
select * from final
