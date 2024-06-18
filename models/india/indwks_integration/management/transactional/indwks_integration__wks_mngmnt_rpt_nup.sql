WITH sales_cube_mgmnt_agg as
(SELECT mth_mm, region_name, zone_name, territory_name, customer_code, customer_name, retailer_code, retailer_name, rtruniquecode, channel_name,
        franchise_name, brand_name, product_category_name, variant_name,  mothersku_code, mothersku_name,
        SUM(quantity) AS quantity, SUM(achievement_nr) AS achievement_nr, retailer_code||'-'||mothersku_name AS ret_msku,
        TO_DATE((sd.mth_mm)::CHARACTER VARYING::TEXT,'YYYYMM'::CHARACTER VARYING::TEXT) AS sales_date
 FROM indedw_integration.edw_rpt_sales_details sd
 WHERE mth_mm >= 202111
 AND business_channel = 'GT'
 GROUP BY mth_mm, region_name, zone_name, territory_name, customer_code, customer_name, retailer_code, retailer_name, rtruniquecode, channel_name,
          franchise_name, brand_name, product_category_name, variant_name, mothersku_code, mothersku_name
),
sku_recom_mgmnt_agg
AS
(SELECT cust_cd, retailer_cd, mother_sku_cd
 FROM indedw_integration.edw_sku_recom_spike_msl sku
 WHERE mth_mm >= 202201
 AND business_channel = 'GT'
 GROUP BY cust_cd, retailer_cd, mother_sku_cd
),
final as 
(
    SELECT sc.*,
       CASE WHEN sku.mother_sku_cd IS NULL THEN '0' ELSE '1' END AS ms_flag
    FROM sales_cube_mgmnt_agg sc
    LEFT JOIN sku_recom_mgmnt_agg sku
       ON UPPER(TRIM(sc.customer_code)) = UPPER(TRIM(sku.cust_cd))
      AND UPPER(TRIM(sc.retailer_code)) = UPPER(TRIM(sku.retailer_cd))
      AND UPPER(TRIM(sc.mothersku_code)) = UPPER(TRIM(sku.mother_sku_cd))
)
select * from final