with wks_skurecom_mi_actuals_tmp3 as(
    select * from {{ ref('indwks_integration__wks_skurecom_mi_actuals_tmp3') }}
),
wks_skurecom_mi_actuals_salesfact_br as(
    select * from {{ ref('indwks_integration__wks_skurecom_mi_actuals_salesfact_br') }}
),
final as(
    SELECT tmp.year,
       tmp.quarter,
       tmp.cust_cd,
       tmp.retailer_cd,
       tmp.mother_sku_cd,
       tmp.oos_flag,
       tmp.ms_flag,
       tmp.cs_flag,
       tmp.soq,
       tmp.customer_name,
       tmp.region_name,
       tmp.zone_name,
       tmp.territory_name,
       tmp.rtruniquecode,
       tmp.class_desc,
       tmp.channel_name,
       tmp.business_channel,
       tmp.status_desc,
       tmp.actv_flg,
       tmp.retailer_name,
       tmp.retailer_category_name,
       tmp.csrtrcode,
       tmp.retailer_channel_level_1,
       tmp.retailer_channel_level_2,
       tmp.retailer_channel_level_3,
       tmp.salesman_code,
       tmp.route_code,
       tmp.salesman_name,
       tmp.unique_sales_code,
       tmp.route_name,
       tmp.quantity,
       tmp.achievement_nr_val,
       tmp.hit_ms_flag,
       MAX(CASE
         WHEN  sales.achievement_nr_val_br > 0::NUMERIC::NUMERIC(18,0)::NUMERIC(38,6) THEN '1'::CHARACTER VARYING
         ELSE 0::CHARACTER VARYING
       END) AS br_flag
FROM wks_skurecom_mi_actuals_tmp3 tmp
  LEFT JOIN wks_skurecom_mi_actuals_salesfact_br sales
         ON sales.fisc_yr = tmp.year
        AND sales.qtr = tmp.quarter
        AND COALESCE(sales.customer_code,'0'::CHARACTER VARYING)::TEXT = COALESCE(tmp.cust_cd,'0'::CHARACTER VARYING)::TEXT
        AND COALESCE(sales.retailer_code,'0'::CHARACTER VARYING)::TEXT = COALESCE(tmp.retailer_cd,'0'::CHARACTER VARYING)::TEXT
GROUP BY tmp.year,
       tmp.quarter,
       tmp.cust_cd,
       tmp.retailer_cd,
       tmp.mother_sku_cd,
       tmp.oos_flag,
       tmp.ms_flag,
       tmp.cs_flag,
       tmp.soq,
       tmp.customer_name,
       tmp.region_name,
       tmp.zone_name,
       tmp.territory_name,
       tmp.rtruniquecode,
       tmp.class_desc,
       tmp.channel_name,
       tmp.business_channel,
       tmp.status_desc,
       tmp.actv_flg,
       tmp.retailer_name,
       tmp.retailer_category_name,
       tmp.csrtrcode,
       tmp.retailer_channel_level_1,
       tmp.retailer_channel_level_2,
       tmp.retailer_channel_level_3,
       tmp.salesman_code,
       tmp.route_code,
       tmp.salesman_name,
       tmp.unique_sales_code,
       tmp.route_name,
       tmp.quantity,
       tmp.achievement_nr_val,
       tmp.hit_ms_flag
)
select * from final