--Import CTE
with edw_rpt_anz_pos_re as (
    select * from {{ ref('pcfedw_integration__edw_rpt_anz_pos_re') }}
),
--Logical CTE

--Final CTE
edw_rpt_anz_re_summary_pos as (
SELECT FISC_YR,
       CAST(FISC_PER AS INTEGER) AS FISC_PER,
       "cluster",
       MARKET,       
       'POS - NO STORE' as data_src,  
       null as FLAG_AGG_DIM_KEY,
       DISTRIBUTOR_CODE,
       DISTRIBUTOR_NAME,
       SELL_OUT_CHANNEL,
       REGION,
       ZONE_NAME,
       CITY,
       RETAIL_ENVIRONMENT,  --actual,msl_input, rest metrics 'NA'
       PROD_HIER_L1,
       PROD_HIER_L2,
       PROD_HIER_L3,
       PROD_HIER_L4,
       PROD_HIER_L5,
       null as PROD_HIER_L6,
       null as PROD_HIER_L7,
       null as PROD_HIER_L8,
       null as PROD_HIER_L9,
       GLOBAL_PRODUCT_FRANCHISE,
       GLOBAL_PRODUCT_BRAND,
       GLOBAL_PRODUCT_SUB_BRAND,
       GLOBAL_PRODUCT_SEGMENT,
       GLOBAL_PRODUCT_SUBSEGMENT,
       GLOBAL_PRODUCT_CATEGORY,
       GLOBAL_PRODUCT_SUBCATEGORY,
       LM_SALES_FLAG,
       P3M_SALES_FLAG,
       P6M_SALES_FLAG,
       P12M_SALES_FLAG,
       MDP_FLAG,
       SUM(TARGET_COMPLAINCE) AS TARGET_COMPLAINCE,
	   SUM(SALES_VALUE)AS SALES_VALUE,
       SUM(SALES_QTY)AS SALES_QTY,
       AVG(SALES_QTY) AS AVG_SALES_QTY,
       SUM(LM_SALES)AS LM_SALES,
       SUM(LM_SALES_QTY)AS LM_SALES_QTY,
       AVG(LM_SALES_QTY)AS LM_AVG_SALES_QTY,
       SUM(P3M_SALES)AS P3M_SALES,
       SUM(P3M_QTY)AS P3M_QTY,
       AVG(P3M_QTY)AS P3M_AVG_QTY ,
       SUM(P6M_SALES)AS P6M_SALES,
       SUM(P6M_QTY)AS P6M_QTY,
       AVG(P6M_QTY)AS P6M_AVG_QTY,
       SUM(P12M_SALES)AS P12M_SALES,
       SUM(P12M_QTY)AS P12M_QTY,
       AVG(P12M_QTY)AS P12M_AVG_QTY,
       null AS F3M_SALES,
       null AS F3M_QTY,
       null AS F3M_AVG_QTY,	   
       null AS size_of_price_lm,
       null AS size_of_price_p3m,
       null AS size_of_price_p6m,
       null AS size_of_price_p12m ,
       null as LM_SALES_FLAG_COUNT, --count(LM_SALES_FLAG) LM_SALES_FLAG_COUNT,--null
       null as P3M_SALES_FLAG_COUNT,--count(P3M_SALES_FLAG)P3M_SALES_FLAG_COUNT,--null
       null as P6M_SALES_FLAG_COUNT,--count(P6M_SALES_FLAG)P6M_SALES_FLAG_COUNT,--null
       null as P12M_SALES_FLAG_COUNT,--count(P12M_SALES_FLAG) P12M_SALES_FLAG_COUNT ,--null
       count(MDP_FLAG) as MDP_FLAG_COUNT,
       MAX(LIST_PRICE) AS LIST_PRICE,
        SUM(SALES_VALUE_LIST_PRICE)AS SALES_VALUE_LIST_PRICE,
       SUM(LM_SALES_LP) AS LM_SALES_LP,
       SUM(P3M_SALES_LP) AS P3M_SALES_LP,
       SUM(P6M_SALES_LP) AS P6M_SALES_LP,
       SUM(P12M_SALES_LP) AS P12M_SALES_LP,
       null  AS size_of_price_lm_lp,
       null As size_of_price_p3m_lp,
       null AS size_of_price_p6m_lp,
       null AS  size_of_price_p12m_lp ,
	   SYSDATE() AS CRT_DTTM	,
       sum(CM_actual_stores) as CM_actual_stores,sum(CM_universe_stores) as CM_universe_stores,avg(CM_numeric_distribution) as CM_numeric_distribution,
       sum(LM_actual_stores) as LM_actual_stores,sum(LM_universe_stores) as LM_universe_stores,avg(LM_numeric_distribution) as LM_numeric_distribution,
       sum(L3M_actual_stores) as L3M_actual_stores,sum(L3M_universe_stores) as L3M_universe_stores,avg(L3M_numeric_distribution) as L3M_numeric_distribution,
       sum(L6M_actual_stores) as L6M_actual_stores,sum(L6M_universe_stores) as L6M_universe_stores,avg(L6M_numeric_distribution) as L6M_numeric_distribution,
       sum(L12M_actual_stores) as L12M_actual_stores,sum(L12M_universe_stores) as L12M_universe_stores,avg(L12M_numeric_distribution) as L12M_numeric_distribution 	
        
 FROM edw_rpt_anz_pos_re 
    WHERE  
	  FISC_PER > TO_CHAR(ADD_MONTHS((SELECT to_date(MAX(fisc_per)::varchar,'YYYYMM') 
      FROM edw_rpt_anz_pos_re ),-15),'YYYYMM') 
  AND FISC_PER <= (select max(fisc_per) FROM edw_rpt_anz_pos_re ) 
 
GROUP BY FISC_YR,
       FISC_PER,
       "cluster",
       MARKET,
       --FLAG_AGG_DIM_KEY,       
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
       PROD_HIER_L5,
      -- null as PROD_HIER_L6,
      -- null as PROD_HIER_L7,
       --null as PROD_HIER_L8,
       --null as PROD_HIER_L9,
       GLOBAL_PRODUCT_FRANCHISE,
       GLOBAL_PRODUCT_BRAND,
       GLOBAL_PRODUCT_SUB_BRAND,
       GLOBAL_PRODUCT_SEGMENT,
       GLOBAL_PRODUCT_SUBSEGMENT,
       GLOBAL_PRODUCT_CATEGORY,
       GLOBAL_PRODUCT_SUBCATEGORY,
       LM_SALES_FLAG,
      P3M_SALES_FLAG,
      P6M_SALES_FLAG,
      P12M_SALES_FLAG,
      MDP_FLAG

)
,
final as 
(
select
    fisc_yr::VARCHAR(11) AS fisc_yr,
fisc_per::numeric(18,0) AS fisc_per,
"cluster"::VARCHAR(100) AS "cluster",
market::VARCHAR(50) AS market,
data_src::VARCHAR(14) AS data_src,
flag_agg_dim_key::VARCHAR(50) AS flag_agg_dim_key,
distributor_code::VARCHAR(500) AS distributor_code,
distributor_name::VARCHAR(500) AS distributor_name,
sell_out_channel::VARCHAR(255) AS sell_out_channel,
region::VARCHAR(255) AS region,
zone_name::VARCHAR(510) AS zone_name,
city::VARCHAR(255) AS city,
retail_environment::VARCHAR(500) AS retail_environment,
prod_hier_l1::VARCHAR(100) AS prod_hier_l1,
prod_hier_l2::VARCHAR(50) AS prod_hier_l2,
prod_hier_l3::VARCHAR(255) AS prod_hier_l3,
prod_hier_l4::VARCHAR(255) AS prod_hier_l4,
prod_hier_l5::VARCHAR(255) AS prod_hier_l5,
prod_hier_l6::VARCHAR(255) AS prod_hier_l6,
prod_hier_l7::VARCHAR(50) AS prod_hier_l7,
prod_hier_l8::VARCHAR(50) AS prod_hier_l8,
prod_hier_l9::VARCHAR(50) AS prod_hier_l9,
global_product_franchise::VARCHAR(30) AS global_product_franchise,
global_product_brand::VARCHAR(30) AS global_product_brand,
global_product_sub_brand::VARCHAR(100) AS global_product_sub_brand,
global_product_segment::VARCHAR(200) AS global_product_segment,
global_product_subsegment::VARCHAR(100) AS global_product_subsegment,
global_product_category::VARCHAR(200) AS global_product_category,
global_product_subcategory::VARCHAR(200) AS global_product_subcategory,
lm_sales_flag::VARCHAR(1) AS lm_sales_flag,
p3m_sales_flag::VARCHAR(1) AS p3m_sales_flag,
p6m_sales_flag::VARCHAR(1) AS p6m_sales_flag,
p12m_sales_flag::VARCHAR(1) AS p12m_sales_flag,
mdp_flag::VARCHAR(1) AS mdp_flag,
target_complaince::numeric(38,6) AS target_complaince,
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
size_of_price_lm::NUMERIC(38,14) AS size_of_price_lm,
size_of_price_p3m::NUMERIC(38,14) AS size_of_price_p3m,
size_of_price_p6m::NUMERIC(38,14) AS size_of_price_p6m,
size_of_price_p12m::NUMERIC(38,14) AS size_of_price_p12m,
lm_sales_flag_count::numeric(38,0) AS lm_sales_flag_count,
p3m_sales_flag_count::numeric(38,0) AS p3m_sales_flag_count,
p6m_sales_flag_count::numeric(38,0) AS p6m_sales_flag_count,
p12m_sales_flag_count::numeric(38,0) AS p12m_sales_flag_count,
mdp_flag_count::numeric(38,0) AS mdp_flag_count,
list_price::NUMERIC(20,4) AS list_price,
sales_value_list_price::NUMERIC(38,6) AS sales_value_list_price,
lm_sales_lp::NUMERIC(38,6) AS lm_sales_lp,
p3m_sales_lp::NUMERIC(38,6) AS p3m_sales_lp,
p6m_sales_lp::NUMERIC(38,6) AS p6m_sales_lp,
p12m_sales_lp::NUMERIC(38,6) AS p12m_sales_lp,
size_of_price_lm_lp::NUMERIC(38,14) AS size_of_price_lm_lp,
size_of_price_p3m_lp::NUMERIC(38,14) AS size_of_price_p3m_lp,
size_of_price_p6m_lp::NUMERIC(38,14) AS size_of_price_p6m_lp,
size_of_price_p12m_lp::NUMERIC(38,14) AS size_of_price_p12m_lp,
crt_dttm::TIMESTAMP WITHOUT TIME ZONE AS crt_dttm,
cm_actual_stores::NUMERIC(38,6) AS cm_actual_stores,
cm_universe_stores::NUMERIC(38,6) AS cm_universe_stores,
cm_numeric_distribution::NUMERIC(38,6) AS cm_numeric_distribution,
lm_actual_stores::NUMERIC(38,6) AS lm_actual_stores,
lm_universe_stores::NUMERIC(38,6) AS lm_universe_stores,
lm_numeric_distribution::NUMERIC(38,6) AS lm_numeric_distribution,
l3m_actual_stores::NUMERIC(38,6) AS l3m_actual_stores,
l3m_universe_stores::NUMERIC(38,6) AS l3m_universe_stores,
l3m_numeric_distribution::NUMERIC(38,6) AS l3m_numeric_distribution,
l6m_actual_stores::NUMERIC(38,6) AS l6m_actual_stores,
l6m_universe_stores::NUMERIC(38,6) AS l6m_universe_stores,
l6m_numeric_distribution::NUMERIC(38,6) AS l6m_numeric_distribution,
l12m_actual_stores::NUMERIC(38,6) AS l12m_actual_stores,
l12m_universe_stores::NUMERIC(38,6) AS l12m_universe_stores,
l12m_numeric_distribution::NUMERIC(38,6) AS l12m_numeric_distribution
 from edw_rpt_anz_re_summary_pos
)
--Final select
select * from final 