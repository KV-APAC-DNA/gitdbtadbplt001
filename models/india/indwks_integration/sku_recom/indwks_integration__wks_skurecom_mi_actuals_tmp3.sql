with wks_skurecom_mi_actuals_tmp2 as (
    select * from {{ ref('indwks_integration__wks_skurecom_mi_actuals_tmp2') }}
),
wks_skurecom_mi_actuals_salesfact as (
    select * from {{ ref('indwks_integration__wks_skurecom_mi_actuals_salesfact') }}
),
final as(
    SELECT sku.year,
       sku.quarter,
       sku.cust_cd,
       sku.retailer_cd,
       sku.mother_sku_cd,
       sku.oos_flag,
       sku.ms_flag,
       sku.cs_flag,
       sku.soq,
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
         WHEN sku.ms_flag::TEXT = '1'::CHARACTER VARYING::TEXT AND (sales.achievement_nr_val <= 0::NUMERIC::NUMERIC(18,0)::NUMERIC(38,6)) THEN '0'::CHARACTER VARYING
         WHEN sku.ms_flag::TEXT = '1'::CHARACTER VARYING::TEXT AND sales.achievement_nr_val > 0::NUMERIC::NUMERIC(18,0)::NUMERIC(38,6) THEN '1'::CHARACTER VARYING
         ELSE 0::CHARACTER VARYING
       END) AS hit_ms_flag
    FROM wks_skurecom_mi_actuals_tmp2 sku
  LEFT JOIN wks_skurecom_mi_actuals_salesfact sales
         ON sales.fisc_yr = sku.year
        AND sales.qtr = sku.quarter
        AND COALESCE(sales.customer_code,'0'::CHARACTER VARYING)::TEXT = COALESCE(sku.cust_cd,'0'::CHARACTER VARYING)::TEXT
        AND COALESCE(sales.retailer_code,'0'::CHARACTER VARYING)::TEXT = COALESCE(sku.retailer_cd,'0'::CHARACTER VARYING)::TEXT
        AND COALESCE(sales.mothersku_code,'0'::CHARACTER VARYING)::TEXT = COALESCE(sku.mother_sku_cd,'0'::CHARACTER VARYING)::TEXT
    GROUP BY sku.year,
       sku.quarter,
       sku.cust_cd,
       sku.retailer_cd,
       sku.mother_sku_cd,
       sku.oos_flag,
       sku.ms_flag,
       sku.cs_flag,
       sku.soq,
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