with wks_rev_sku_recom_tbl2 as
(
    select * from {{ ref('indwks_integration__wks_rev_sku_recom_tbl2') }}
),
wks_rev_edw_dailysales_fact as
(
    select * from {{ ref('indwks_integration__wks_rev_edw_dailysales_fact') }}
),
final as
(
SELECT sku.mnth_id,
       sku.cust_cd,
       sku.retailer_cd,
       sku.mother_sku_cd,
       sku.oos_flag,
       sku.ms_flag,
       sku.cs_flag,
       sku.soq,
       sku.reco_date,
       sku.mth_mm,
       sku.qtr,
       sku.fisc_yr,
       sku.month,
       sku.customer_name,
       sku.region_name,
       sku.zone_name,
       sku.territory_name,
       sku.rtruniquecode,
       sku.class_desc,
       sku.channel_name,
       sku.business_channel,
       sku.status_desc,
       sku.actv_flg,
       sku.retailer_name,
       sku.retailer_category_name,
       sku.csrtrcode,
       sku.retailer_channel_level_1,
       sku.retailer_channel_level_2,
       sku.retailer_channel_level_3,
       sku.salesman_code,
       sku.route_code,
       sku.salesman_name,
       sku.unique_sales_code,
       sku.route_name,
       SUM(sales.quantity) AS quantity,
       SUM(sales.achievement_nr_val) AS achievement_nr_val,
       MAX(CASE
         WHEN sku.ms_flag::TEXT = '0'::CHARACTER VARYING::TEXT AND sales.achievement_nr_val >= 0::NUMERIC::NUMERIC(18,0)::NUMERIC(38,6) THEN '0'::CHARACTER VARYING
         WHEN sku.ms_flag::TEXT = '1'::CHARACTER VARYING::TEXT AND sales.achievement_nr_val <= 0::NUMERIC::NUMERIC(18,0)::NUMERIC(38,6) THEN '0'::CHARACTER VARYING
         WHEN sku.ms_flag::TEXT = '1'::CHARACTER VARYING::TEXT AND sales.achievement_nr_val > 0::NUMERIC::NUMERIC(18,0)::NUMERIC(38,6) THEN '1'::CHARACTER VARYING
         ELSE 0::CHARACTER VARYING
       END) AS hit_ms_flag
FROM wks_rev_sku_recom_tbl2 sku
  LEFT JOIN wks_rev_edw_dailysales_fact sales
         ON DATEDIFF (month,sales.sales_date::TIMESTAMP without TIME zone,sku.reco_date::TIMESTAMP without TIME zone) < 3
        AND DATEDIFF (month,sales.sales_date::TIMESTAMP without TIME zone,sku.reco_date::TIMESTAMP without TIME zone) >= 0
        AND COALESCE (sales.customer_code,'0'::CHARACTER VARYING)::TEXT = COALESCE (sku.cust_cd,'0'::CHARACTER VARYING)::TEXT
        AND COALESCE (sales.retailer_code,'0'::CHARACTER VARYING)::TEXT = COALESCE (sku.retailer_cd,'0'::CHARACTER VARYING)::TEXT
        AND COALESCE (sales.mothersku_code,'0'::CHARACTER VARYING)::TEXT = COALESCE (sku.mother_sku_cd,'0'::CHARACTER VARYING)::TEXT
GROUP BY sku.mnth_id,
       sku.cust_cd,
       sku.retailer_cd,
       sku.mother_sku_cd,
       sku.oos_flag,
       sku.ms_flag,
       sku.cs_flag,
       sku.soq,
       sku.reco_date,
       sku.mth_mm,
       sku.qtr,
       sku.fisc_yr,
       sku.month,
       sku.customer_name,
       sku.region_name,
       sku.zone_name,
       sku.territory_name,
       sku.rtruniquecode,
       sku.class_desc,
       sku.channel_name,
       sku.business_channel,
       sku.status_desc,
       sku.actv_flg,
       sku.retailer_name,
       sku.retailer_category_name,
       sku.csrtrcode,
       sku.retailer_channel_level_1,
       sku.retailer_channel_level_2,
       sku.retailer_channel_level_3,
       sku.salesman_code,
       sku.route_code,
       sku.salesman_name,
       sku.unique_sales_code,
       sku.route_name
)
select * from final
