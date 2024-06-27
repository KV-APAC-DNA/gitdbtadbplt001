with wks_pilot_sku_recom_tbl1_integ as
(
    select * from {{ ref('indwks_integration__wks_pilot_sku_recom_tbl1_integ') }}
),
wks_pilot_msku_actuals as
(
    select * from {{ ref('indwks_integration__wks_pilot_msku_actuals') }}
),
final as
(
    SELECT NVL(sku.mth_mm,msku_sales.mth_mm) AS mth_mm,
       NVL(sku.qtr,msku_sales.qtr) AS qtr,
       NVL(sku.fisc_yr,msku_sales.fisc_yr) AS fisc_yr,
       NVL(sku.month,msku_sales.month) AS month,
       NVL(sku.cust_cd,msku_sales.customer_code) AS cust_cd,
       NVL(sku.customer_name,msku_sales.customer_name) AS customer_name,
       NVL(sku.rtruniquecode,msku_sales.rtruniquecode) AS rtruniquecode,
       NVL(sku.retailer_cd,msku_sales.retailer_code) AS retailer_cd,
       NVL(sku.mother_sku_cd,msku_sales.mothersku_code) AS mother_sku_cd,
       sku.oos_flag,
       sku.ms_flag,
       sku.cs_flag,
       sku.soq,
       NVL(sku.region_name,msku_sales.region_name) AS region_name,
       NVL(sku.zone_name,msku_sales.zone_name) AS zone_name,
       NVL(sku.territory_name,msku_sales.territory_name) AS territory_name,
       NVL(sku.class_desc,msku_sales.class_desc) AS class_desc,
       NVL(sku.class_desc_a_sa_flag,msku_sales.class_desc_a_sa_flag) AS class_desc_a_sa_flag,
       NVL(sku.channel_name,msku_sales.channel_name) AS channel_name,
       NVL(sku.business_channel,msku_sales.business_channel) AS business_channel,
       NVL(sku.status_desc,msku_sales.status_desc) AS status_desc,
       sku.actv_flg,
       NVL(sku.retailer_name,msku_sales.retailer_name) AS retailer_name,
       NVL(sku.retailer_category_name,msku_sales.retailer_category_name) AS retailer_category_name,
       sku.csrtrcode,
       sku.nup_target,
       CAST(NULL AS BIGINT) AS nup_actual_ly,
       msku_sales.nup_actual_cy,
       msku_sales.achievement_nr,
       CASE WHEN sku.ms_flag = 1 AND msku_sales.achievement_nr > 0 THEN 1 ELSE 0 END AS hit_ms_flag,
       CASE WHEN sku.cs_flag = 1 AND msku_sales.achievement_nr > 0 THEN 1 ELSE 0 END AS hit_cs_flag,
       NVL(sku.franchise_name,msku_sales.franchise_name) AS franchise_name,
       NVL(sku.brand_name,msku_sales.brand_name) AS brand_name,
       NVL(sku.product_category_name,msku_sales.product_category_name) AS product_category_name,
       NVL(sku.variant_name,msku_sales.variant_name) AS variant_name,
       NVL(sku.mothersku_name,msku_sales.mothersku_name) AS mothersku_name,
       NVL(sku.salesman_code,msku_sales.latest_salesman_code) AS salesman_code,
       NVL(sku.salesman_name,msku_sales.latest_salesman_name) AS salesman_name,
       NVL(sku.unique_sales_code,msku_sales.latest_uniquesalescode) AS unique_sales_code,
       trim(sku.route_code) as route_code,
       trim(sku.route_name) as route_name,
       CASE WHEN (sku.ms_flag = 1 OR sku.cs_flag = 1) THEN 'RECO_YES' ELSE 'RECO_NO' END AS reco_flag,
       CASE WHEN msku_sales.rtruniquecode IS NOT NULL THEN 'SALES_YES_CY' ELSE 'SALES_NO_CY' END AS sales_flag
    FROM  wks_pilot_sku_recom_tbl1_integ sku
    FULL OUTER JOIN wks_pilot_msku_actuals msku_sales
        ON sku.mth_mm = msku_sales.mth_mm
        AND sku.rtruniquecode = msku_sales.rtruniquecode
        AND sku.mother_sku_cd = msku_sales.mothersku_code
)
select * from final