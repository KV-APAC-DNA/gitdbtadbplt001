--Import CTE
with v_edw_hk_rpt_retail_excellence as (
    select * from {{ ref('ntaedw_integration__edw_hk_rpt_retail_excellence') }}
),

--Logical CTE
final as 
(
    SELECT FISC_YR,
       CAST(FISC_PER AS numeric(18,0) ) AS FISC_PER,
       "cluster",
       MARKET,
       MD5(nvl (SELL_OUT_CHANNEL,'soc') ||nvl (RETAIL_ENVIRONMENT,'re') ||nvl (REGION,'reg') ||
	   nvl (ZONE_NAME,'zn') ||nvl (CITY,'cty') ||nvl (PROD_HIER_L1,'ph1') ||nvl (PROD_HIER_L2,'ph2') || 
	   nvl (PROD_HIER_L3,'ph3') ||nvl (PROD_HIER_L4,'ph4') || nvl (GLOBAL_PRODUCT_FRANCHISE,'gpf') ||
	   nvl (GLOBAL_PRODUCT_BRAND,'gpb') || nvl (GLOBAL_PRODUCT_SUB_BRAND,'gpsb') ||nvl (GLOBAL_PRODUCT_SEGMENT,'gps') || 
	   nvl (GLOBAL_PRODUCT_SUBSEGMENT,'gpss') ||nvl (GLOBAL_PRODUCT_CATEGORY,'gpc') || 
	   nvl (GLOBAL_PRODUCT_SUBCATEGORY,'gpsc')) AS FLAG_AGG_DIM_KEY,
       DATA_SRC,
       DISTRIBUTOR_CODE,
       DISTRIBUTOR_NAME,
       SELL_OUT_CHANNEL,
       REGION,
       ZONE_NAME,
       CITY,
       RETAIL_ENVIRONMENT,
       PROD_HIER_L1,
       PROD_HIER_L2,
       PROD_HIER_L3,
       PROD_HIER_L4,
       NULL AS PROD_HIER_L5,
       NULL AS PROD_HIER_L6,
       NULL AS PROD_HIER_L7,
       NULL AS PROD_HIER_L8,
       NULL AS PROD_HIER_L9,
       GLOBAL_PRODUCT_FRANCHISE,
       GLOBAL_PRODUCT_BRAND,
       GLOBAL_PRODUCT_SUB_BRAND,
       GLOBAL_PRODUCT_SEGMENT,
       GLOBAL_PRODUCT_SUBSEGMENT,
       GLOBAL_PRODUCT_CATEGORY,
       GLOBAL_PRODUCT_SUBCATEGORY,
       STORE_CODE,
       PRODUCT_CODE,
       SUM(SALES_VALUE) AS SALES_VALUE,
       SUM(SALES_QTY) AS SALES_QTY,
       AVG(SALES_QTY) AS AVG_SALES_QTY,
       SUM(LM_SALES) AS LM_SALES,
       SUM(LM_SALES_QTY) AS LM_SALES_QTY,
       AVG(LM_SALES_QTY) AS LM_AVG_SALES_QTY,
       SUM(P3M_SALES) AS P3M_SALES,
       SUM(P3M_QTY) AS P3M_QTY,
       AVG(P3M_QTY) AS P3M_AVG_QTY,
       SUM(P6M_SALES) AS P6M_SALES,
       SUM(P6M_QTY) AS P6M_QTY,
       AVG(P6M_QTY) AS P6M_AVG_QTY,
       SUM(P12M_SALES) AS P12M_SALES,
       SUM(P12M_QTY) AS P12M_QTY,
       AVG(P12M_QTY) AS P12M_AVG_QTY,
       SUM(F3M_SALES) AS F3M_SALES,
       SUM(F3M_QTY) AS F3M_QTY,
       AVG(F3M_QTY) AS F3M_AVG_QTY,
       MAX(LIST_PRICE) AS LIST_PRICE,
       CASE
         WHEN SUM(
           CASE
             WHEN LM_SALES_FLAG = 'Y' THEN 1
             ELSE 0
           END ) > 0 THEN 1
         ELSE 0
       END AS LM_SALES_FLAG,
       CASE
         WHEN SUM(
           CASE
             WHEN P3M_SALES_FLAG = 'Y' THEN 1
             ELSE 0
           END ) > 0 THEN 1
         ELSE 0
       END AS P3M_SALES_FLAG,
       CASE
         WHEN SUM(
           CASE
             WHEN P6M_SALES_FLAG = 'Y' THEN 1
             ELSE 0
           END ) > 0 THEN 1
         ELSE 0
       END AS P6M_SALES_FLAG,
       CASE
         WHEN SUM(
           CASE
             WHEN P12M_SALES_FLAG = 'Y' THEN 1
             ELSE 0
           END ) > 0 THEN 1
         ELSE 0
       END AS P12M_SALES_FLAG,
       CASE
         WHEN MDP_FLAG = 'Y' THEN 1
         ELSE 0
       END AS MDP_FLAG,
       MAX(size_of_price_lm) AS size_of_price_lm,
       MAX(size_of_price_p3m) AS size_of_price_p3m,
       MAX(size_of_price_p6m) AS size_of_price_p6m,
       MAX(size_of_price_p12m) AS size_of_price_p12m,
       SUM(SALES_VALUE_LIST_PRICE) AS SALES_VALUE_LIST_PRICE,
       SUM(LM_SALES_LP) AS LM_SALES_LP,
       SUM(P3M_SALES_LP) AS P3M_SALES_LP,
       SUM(P6M_SALES_LP) AS P6M_SALES_LP,
       SUM(P12M_SALES_LP) AS P12M_SALES_LP,
       MAX(size_of_price_lm_lp) AS size_of_price_lm_lp,
       MAX(size_of_price_p3m_lp) AS size_of_price_p3m_lp,
       MAX(size_of_price_p6m_lp) AS size_of_price_p6m_lp,
       MAX(size_of_price_p12m_lp) AS size_of_price_p12m_lp,
       --MAX(TARGET_COMPLAINCE) OVER (PARTITION BY FISC_PER, GLOBAL_PRODUCT_BRAND, MDP_FLAG) AS TARGET_COMPLAINCE,
       TARGET_COMPLAINCE,
       SYSDATE() AS CRT_DTTM
    FROM v_edw_hk_rpt_retail_excellence FLAGS

    GROUP BY FLAGS.FISC_YR,
         FLAGS.FISC_PER,
         FLAGS."cluster",
         FLAGS.MARKET,
         MD5(nvl (SELL_OUT_CHANNEL,'soc') ||nvl (RETAIL_ENVIRONMENT,'re') ||nvl (REGION,'reg') ||
		 nvl (ZONE_NAME,'zn') ||nvl (CITY,'cty') ||nvl (PROD_HIER_L1,'ph1') ||nvl (PROD_HIER_L2,'ph2') || 
		 nvl (PROD_HIER_L3,'ph3') ||nvl (PROD_HIER_L4,'ph4') || nvl (GLOBAL_PRODUCT_FRANCHISE,'gpf') ||
		 nvl (GLOBAL_PRODUCT_BRAND,'gpb') || nvl (GLOBAL_PRODUCT_SUB_BRAND,'gpsb') ||nvl (GLOBAL_PRODUCT_SEGMENT,'gps') || 
		 nvl (GLOBAL_PRODUCT_SUBSEGMENT,'gpss') ||nvl (GLOBAL_PRODUCT_CATEGORY,'gpc') || nvl (GLOBAL_PRODUCT_SUBCATEGORY,'gpsc')),
         FLAGS.DATA_SRC,
         FLAGS.DISTRIBUTOR_CODE,
         FLAGS.DISTRIBUTOR_NAME,
         FLAGS.SELL_OUT_CHANNEL,
         FLAGS.REGION,
         FLAGS.ZONE_NAME,
         FLAGS.CITY,
         FLAGS.RETAIL_ENVIRONMENT,
         FLAGS.PROD_HIER_L1,
         FLAGS.PROD_HIER_L2,
         FLAGS.PROD_HIER_L3,
         FLAGS.PROD_HIER_L4,
         FLAGS.GLOBAL_PRODUCT_FRANCHISE,
         FLAGS.GLOBAL_PRODUCT_BRAND,
         FLAGS.GLOBAL_PRODUCT_SUB_BRAND,
         FLAGS.GLOBAL_PRODUCT_SEGMENT,
         FLAGS.GLOBAL_PRODUCT_SUBSEGMENT,
         FLAGS.GLOBAL_PRODUCT_CATEGORY,
         FLAGS.GLOBAL_PRODUCT_SUBCATEGORY,
         (CASE WHEN MDP_FLAG = 'Y' THEN 1 ELSE 0 END),
         TARGET_COMPLAINCE,
         STORE_CODE,
         PRODUCT_CODE
),

hk_rpt_retail_excellence_summary_base as (
    select
    fisc_yr::numeric(18,0) AS fisc_yr,
    fisc_per::numeric(18,0) AS fisc_per,
    "cluster"::VARCHAR(100) AS "cluster",
    market::VARCHAR(50) AS market,
    flag_agg_dim_key::VARCHAR(32) AS flag_agg_dim_key,
    data_src::VARCHAR(14) AS data_src,
    distributor_code::VARCHAR(150) AS distributor_code,
    distributor_name::VARCHAR(356) AS distributor_name,
    sell_out_channel::VARCHAR(382) AS sell_out_channel,
    region::VARCHAR(255) AS region,
    zone_name::VARCHAR(255) AS zone_name,
    city::VARCHAR(255) AS city,
    retail_environment::VARCHAR(382) AS retail_environment,
    prod_hier_l1::VARCHAR(11) AS prod_hier_l1,
    prod_hier_l2::VARCHAR(1) AS prod_hier_l2,
    prod_hier_l3::VARCHAR(255) AS prod_hier_l3,
    prod_hier_l4::VARCHAR(255) AS prod_hier_l4,
    prod_hier_l5::VARCHAR(1) AS prod_hier_l5,
    prod_hier_l6::VARCHAR(1) AS prod_hier_l6,
    prod_hier_l7::VARCHAR(1) AS prod_hier_l7,
    prod_hier_l8::VARCHAR(1) AS prod_hier_l8,
    prod_hier_l9::VARCHAR(1) AS prod_hier_l9,
    global_product_franchise::VARCHAR(30) AS global_product_franchise,
    global_product_brand::VARCHAR(30) AS global_product_brand,
    global_product_sub_brand::VARCHAR(100) AS global_product_sub_brand,
    global_product_segment::VARCHAR(50) AS global_product_segment,
    global_product_subsegment::VARCHAR(100) AS global_product_subsegment,
    global_product_category::VARCHAR(50) AS global_product_category,
    global_product_subcategory::VARCHAR(50) AS global_product_subcategory,
    store_code::VARCHAR(100) AS store_code,
    product_code::VARCHAR(150) AS product_code,
    sales_value::NUMERIC(38,6) AS sales_value,
    sales_qty::NUMERIC(38,6) AS sales_qty,
    avg_sales_qty::NUMERIC(38,6) AS avg_sales_qty,
    lm_sales::NUMERIC(38,6) AS lm_sales,
    lm_sales_qty::NUMERIC(38,6) AS lm_sales_qty,
    lm_avg_sales_qty::NUMERIC(38,6) AS lm_avg_sales_qty,
    p3m_sales::NUMERIC(38,6) AS p3m_sales,
    p3m_qty::NUMERIC(38,6) AS p3m_qty,
    p3m_avg_qty::NUMERIC(38,6) AS p3m_avg_qty,
    p6m_sales::NUMERIC(38,6) AS p6m_sales,
    p6m_qty::NUMERIC(38,6) AS p6m_qty,
    p6m_avg_qty::NUMERIC(38,6) AS p6m_avg_qty,
    p12m_sales::NUMERIC(38,6) AS p12m_sales,
    p12m_qty::NUMERIC(38,6) AS p12m_qty,
    p12m_avg_qty::NUMERIC(38,6) AS p12m_avg_qty,
    f3m_sales::NUMERIC(38,6) AS f3m_sales,
    f3m_qty::NUMERIC(38,6) AS f3m_qty,
    f3m_avg_qty::NUMERIC(38,6) AS f3m_avg_qty,
    list_price::NUMERIC(20,4) AS list_price,
    lm_sales_flag::numeric(18,0) AS lm_sales_flag,
    p3m_sales_flag::numeric(18,0) AS p3m_sales_flag,
    p6m_sales_flag::numeric(18,0) AS p6m_sales_flag,
    p12m_sales_flag::numeric(18,0) AS p12m_sales_flag,
    mdp_flag::numeric(18,0) AS mdp_flag,
    size_of_price_lm::NUMERIC(38,14) AS size_of_price_lm,
    size_of_price_p3m::NUMERIC(38,14) AS size_of_price_p3m,
    size_of_price_p6m::NUMERIC(38,14) AS size_of_price_p6m,
    size_of_price_p12m::NUMERIC(38,14) AS size_of_price_p12m,
    sales_value_list_price::NUMERIC(38,12) AS sales_value_list_price,
    lm_sales_lp::NUMERIC(38,12) AS lm_sales_lp,
    p3m_sales_lp::NUMERIC(38,12) AS p3m_sales_lp,
    p6m_sales_lp::NUMERIC(38,12) AS p6m_sales_lp,
    p12m_sales_lp::NUMERIC(38,12) AS p12m_sales_lp,
    size_of_price_lm_lp::NUMERIC(38,20) AS size_of_price_lm_lp,
    size_of_price_p3m_lp::NUMERIC(38,20) AS size_of_price_p3m_lp,
    size_of_price_p6m_lp::NUMERIC(38,20) AS size_of_price_p6m_lp,
    size_of_price_p12m_lp::NUMERIC(38,20) AS size_of_price_p12m_lp,
    target_complaince::numeric(38,6) AS target_complaince,
    crt_dttm::TIMESTAMP WITHOUT TIME ZONE  AS crt_dttm
    from final
)


--Final select
select * from hk_rpt_retail_excellence_summary_base
