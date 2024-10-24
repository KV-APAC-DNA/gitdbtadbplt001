--Import CTE
{{
    config(
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";"
    )
}}
with edw_rpt_id_re as (
    select * from {{ ref('idnedw_integration__edw_rpt_id_re') }}
),
--Logical CTE
transformation as (
    Select FISC_YR,
       CAST(FISC_PER AS numeric(18,0) ) AS FISC_PER,		--// INTEGER
       "cluster",
       MARKET,
              MD5(nvl(SELL_OUT_CHANNEL,'soc')||nvl(RETAIL_ENVIRONMENT,'re')||nvl(REGION,'reg')||nvl(ZONE_NAME,'zn')
            ||nvl(CITY,'cty')||nvl(PROD_HIER_L1,'ph1')||nvl(PROD_HIER_L2,'ph2')||
           nvl(PROD_HIER_L3,'ph3')||nvl(PROD_HIER_L4,'ph4')||
           nvl(GLOBAL_PRODUCT_FRANCHISE,'gpf')||nvl(GLOBAL_PRODUCT_BRAND,'gpb')||
           nvl(GLOBAL_PRODUCT_SUB_BRAND,'gpsb')||nvl(GLOBAL_PRODUCT_SEGMENT,'gps')||
           nvl(GLOBAL_PRODUCT_SUBSEGMENT,'gpss')||nvl(GLOBAL_PRODUCT_CATEGORY,'gpc')||
           nvl(GLOBAL_PRODUCT_SUBCATEGORY,'gpsc')) AS FLAG_AGG_DIM_KEY,
       data_src,
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
       NULL as PROD_HIER_L5,
       NULL as PROD_HIER_L6,
       NULL as PROD_HIER_L7,
       NULL as PROD_HIER_L8,
       NULL AS PROD_HIER_L9,
       GLOBAL_PRODUCT_FRANCHISE,
       GLOBAL_PRODUCT_BRAND,
       GLOBAL_PRODUCT_SUB_BRAND,
       GLOBAL_PRODUCT_SEGMENT,
       GLOBAL_PRODUCT_SUBSEGMENT,
       GLOBAL_PRODUCT_CATEGORY,
       GLOBAL_PRODUCT_SUBCATEGORY,
       store_code,
       product_code,
       SUM(SALES_VALUE)AS SALES_VALUE,
       SUM(SALES_QTY)AS SALES_QTY,
       (AVG(SALES_QTY)) AS AVG_SALES_QTY,		--// AVG
       SUM(LM_SALES)AS LM_SALES,
       SUM(LM_SALES_QTY)AS LM_SALES_QTY,
       (AVG(LM_SALES_QTY))AS LM_AVG_SALES_QTY,		--// AVG
       SUM(P3M_SALES)AS P3M_SALES,
       SUM(P3M_QTY)AS P3M_QTY,
       (AVG(P3M_QTY))AS P3M_AVG_QTY ,		--// AVG
       SUM(P6M_SALES)AS P6M_SALES,
       SUM(P6M_QTY)AS P6M_QTY,
       (AVG(P6M_QTY))AS P6M_AVG_QTY,		--// AVG
       SUM(P12M_SALES)AS P12M_SALES,
       SUM(P12M_QTY)AS P12M_QTY,
       (AVG(P12M_QTY))AS P12M_AVG_QTY,		--// AVG
       SUM(F3M_SALES)AS F3M_SALES,
       SUM(F3M_QTY)AS F3M_QTY,
       (AVG(F3M_QTY))AS F3M_AVG_QTY,		--// AVG
	    MAX(LIST_PRICE) AS LIST_PRICE,
       case when SUM (case when LM_SALES_FLAG = 'Y' then 1 else 0 end ) >0 then 1 else 0 end as LM_SALES_FLAG,
       case when SUM(case when P3M_SALES_FLAG = 'Y' then 1 else 0 end) >0 then 1 else 0 end as  P3M_SALES_FLAG,
       case when SUM(case when P6M_SALES_FLAG = 'Y' then 1 else 0 end) >0 then 1 else 0 end as  P6M_SALES_FLAG,
       case when SUM(case when P12M_SALES_FLAG= 'Y' then 1 else 0 end ) >0 then 1 else 0 end as P12M_SALES_FLAG,
       case when MDP_FLAG ='Y' then 1 else 0 end as MDP_FLAG,
       max(size_of_price_lm)  AS   size_of_price_lm,
       max(size_of_price_p3m) As   size_of_price_p3m,
       max(size_of_price_p6m) AS   size_of_price_p6m,
       max(size_of_price_p12m) AS  size_of_price_p12m,
        sum(SALES_VALUE_LIST_PRICE)AS SALES_VALUE_LIST_PRICE,
       SUM(LM_SALES_LP) AS LM_SALES_LP,
       SUM(P3M_SALES_LP) AS P3M_SALES_LP,
       SUM(P6M_SALES_LP) AS P6M_SALES_LP,
       SUM(P12M_SALES_LP) AS P12M_SALES_LP,
       max(size_of_price_lm_lp)  AS   size_of_price_lm_lp,
       max(size_of_price_p3m_lp) As   size_of_price_p3m_lp,
       max(size_of_price_p6m_lp) AS   size_of_price_p6m_lp,
       max(size_of_price_p12m_lp) AS  size_of_price_p12m_lp,
       TARGET_COMPLAINCE
 FROM edw_rpt_id_re FLAGS		--//  FROM ID_EDW.EDW_RPT_ID_RE FLAGS
 GROUP BY FLAGS.FISC_YR,		--//  GROUP BY FLAGS.FISC_YR,
       FLAGS.FISC_PER,		--//        FLAGS.FISC_PER,
       FLAGS."cluster",		--//        FLAGS."CLUSTER",
       FLAGS.MARKET,		--//        FLAGS.MARKET,
       MD5(nvl(SELL_OUT_CHANNEL,'soc')||nvl(RETAIL_ENVIRONMENT,'re')||nvl(REGION,'reg')||nvl(ZONE_NAME,'zn')
            ||nvl(CITY,'cty')||nvl(PROD_HIER_L1,'ph1')||nvl(PROD_HIER_L2,'ph2')||
           nvl(PROD_HIER_L3,'ph3')||nvl(PROD_HIER_L4,'ph4')||
           nvl(GLOBAL_PRODUCT_FRANCHISE,'gpf')||nvl(GLOBAL_PRODUCT_BRAND,'gpb')||
           nvl(GLOBAL_PRODUCT_SUB_BRAND,'gpsb')||nvl(GLOBAL_PRODUCT_SEGMENT,'gps')||
           nvl(GLOBAL_PRODUCT_SUBSEGMENT,'gpss')||nvl(GLOBAL_PRODUCT_CATEGORY,'gpc')||
           nvl(GLOBAL_PRODUCT_SUBCATEGORY,'gpsc')),
       FLAGS.DATA_SRC,		--//        FLAGS.data_src,
       FLAGS.DISTRIBUTOR_CODE,		--//        FLAGS.DISTRIBUTOR_CODE,
       FLAGS.DISTRIBUTOR_NAME,		--//        FLAGS.DISTRIBUTOR_NAME,
       FLAGS.SELL_OUT_CHANNEL,		--//        FLAGS.SELL_OUT_CHANNEL,
       FLAGS.REGION,		--//        FLAGS.REGION,
       FLAGS.ZONE_NAME,		--//        FLAGS.ZONE_NAME,
       FLAGS.CITY,		--//        FLAGS.CITY,
       FLAGS.RETAIL_ENVIRONMENT,		--//        FLAGS.RETAIL_ENVIRONMENT,
       FLAGS.PROD_HIER_L1,		--//        FLAGS.PROD_HIER_L1,
       FLAGS.PROD_HIER_L2,		--//        FLAGS.PROD_HIER_L2,
       FLAGS.PROD_HIER_L3,		--//        FLAGS.PROD_HIER_L3,
       FLAGS.PROD_HIER_L4,		--//        FLAGS.PROD_HIER_L4,
       FLAGS.GLOBAL_PRODUCT_FRANCHISE,		--//        FLAGS.GLOBAL_PRODUCT_FRANCHISE,
       FLAGS.GLOBAL_PRODUCT_BRAND,		--//        FLAGS.GLOBAL_PRODUCT_BRAND,
       FLAGS.GLOBAL_PRODUCT_SUB_BRAND,		--//        FLAGS.GLOBAL_PRODUCT_SUB_BRAND,
       FLAGS.GLOBAL_PRODUCT_SEGMENT,		--//        FLAGS.GLOBAL_PRODUCT_SEGMENT,
       FLAGS.GLOBAL_PRODUCT_SUBSEGMENT,		--//        FLAGS.GLOBAL_PRODUCT_SUBSEGMENT,
       FLAGS.GLOBAL_PRODUCT_CATEGORY,		--//        FLAGS.GLOBAL_PRODUCT_CATEGORY,
       FLAGS.GLOBAL_PRODUCT_SUBCATEGORY,		--//        FLAGS.GLOBAL_PRODUCT_SUBCATEGORY,
       (case when MDP_FLAG ='Y' then 1 else 0 end) ,
       TARGET_COMPLAINCE ,
       store_code,
       product_code
),
final as (
    select
    fisc_yr::VARCHAR(16) AS fisc_yr,
fisc_per::numeric(18,0) AS fisc_per,
"cluster"::VARCHAR(100) as "cluster",
market::VARCHAR(50) AS market,
flag_agg_dim_key::VARCHAR(32) AS flag_agg_dim_key,
data_src::VARCHAR(8) AS data_src,
distributor_code::VARCHAR(100) AS distributor_code,
distributor_name::VARCHAR(356) AS distributor_name,
sell_out_channel::VARCHAR(500) AS sell_out_channel,
region::VARCHAR(150) AS region,
zone_name::VARCHAR(150) AS zone_name,
city::VARCHAR(2) AS city,
retail_environment::VARCHAR(500) AS retail_environment,
prod_hier_l1::VARCHAR(50) AS prod_hier_l1,
prod_hier_l2::VARCHAR(1) AS prod_hier_l2,
prod_hier_l3::VARCHAR(50) AS prod_hier_l3,
prod_hier_l4::VARCHAR(50) AS prod_hier_l4,
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
product_code::VARCHAR(500) AS product_code,
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
list_price::NUMERIC(38,6) AS list_price,
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
target_complaince::numeric(38,6) AS target_complaince
from transformation
)


--Final select
select * from  final