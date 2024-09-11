--Import CTE
with th_edw_rpt_retail_excellence_summary_base as (
    select * from {{ ref('aspedw_integration__edw_th_rpt_retail_excellence_summary_base') }}
),
--Logical CTE

--Final CTE
th_edw_rpt_retail_excellence_summary as (
SELECT FISC_YR,
       FISC_PER,
       CLUSTER,
       MARKET,
       data_src,
       FLAG_AGG_DIM_KEY,
       DISTRIBUTOR_CODE,
       DISTRIBUTOR_NAME,
       SELL_OUT_CHANNEL,
       REGION,
       ZONE_NAME,
       null as CITY,
       RETAIL_ENVIRONMENT,
       PROD_HIER_L1,
       PROD_HIER_L2,
       PROD_HIER_L3,
       PROD_HIER_L4,
       PROD_HIER_L5,
       PROD_HIER_L6,
       PROD_HIER_L7,
       PROD_HIER_L8,
       PROD_HIER_L9,
       GLOBAL_PRODUCT_FRANCHISE,
       GLOBAL_PRODUCT_BRAND,
       GLOBAL_PRODUCT_SUB_BRAND,
       GLOBAL_PRODUCT_SEGMENT,
       GLOBAL_PRODUCT_SUBSEGMENT,
       GLOBAL_PRODUCT_CATEGORY,
       GLOBAL_PRODUCT_SUBCATEGORY,
       CASE WHEN LM_SALES_FLAG = 1 THEN 'Y' ELSE 'N' END  AS LM_SALES_FLAG,
       CASE WHEN P3M_SALES_FLAG = 1 THEN 'Y' ELSE 'N' END   AS P3M_SALES_FLAG,
       CASE WHEN P6M_SALES_FLAG = 1 THEN 'Y' ELSE 'N' END  AS P6M_SALES_FLAG,
       CASE WHEN P12M_SALES_FLAG = 1 THEN 'Y' ELSE 'N' END  AS P12M_SALES_FLAG,
       CASE WHEN MDP_FLAG = 1 THEN 'Y' ELSE 'N' END  AS MDP_FLAG,
       SUM(TARGET_COMPLAINCE) AS TARGET_COMPLAINCE,
	   SUM(SALES_VALUE)AS SALES_VALUE,
       SUM(SALES_QTY)AS SALES_QTY,
       AVG(SALES_QTY) AS AVG_SALES_QTY,		--// AVG
       SUM(LM_SALES)AS LM_SALES,
       SUM(LM_SALES_QTY)AS LM_SALES_QTY,
       AVG(LM_SALES_QTY)AS LM_AVG_SALES_QTY,		--// AVG
       SUM(P3M_SALES)AS P3M_SALES,
       SUM(P3M_QTY)AS P3M_QTY,
       AVG(P3M_QTY)AS P3M_AVG_QTY ,		--// AVG
       SUM(P6M_SALES)AS P6M_SALES,
       SUM(P6M_QTY)AS P6M_QTY,
       AVG(P6M_QTY)AS P6M_AVG_QTY,		--// AVG
       SUM(P12M_SALES)AS P12M_SALES,
       SUM(P12M_QTY)AS P12M_QTY,
       AVG(P12M_QTY)AS P12M_AVG_QTY,		--// AVG
       SUM(F3M_SALES)AS F3M_SALES,
       SUM(F3M_QTY)AS F3M_QTY,
       AVG(F3M_QTY)AS F3M_AVG_QTY,		--// AVG
        SUM(size_of_price_lm)  AS size_of_price_lm,
        SUM(size_of_price_p3m) As size_of_price_p3m,
        SUM(size_of_price_p6m) AS size_of_price_p6m,
        SUM(size_of_price_p12m) AS  size_of_price_p12m ,
       count(LM_SALES_FLAG) LM_SALES_FLAG_COUNT,
        count(P3M_SALES_FLAG)P3M_SALES_FLAG_COUNT,
        count(P6M_SALES_FLAG)P6M_SALES_FLAG_COUNT,
        count(P12M_SALES_FLAG) P12M_SALES_FLAG_COUNT ,
        count(MDP_FLAG) MDP_FLAG_COUNT,
        MAX(LIST_PRICE) AS LIST_PRICE,
        SUM(SALES_VALUE_LIST_PRICE)AS SALES_VALUE_LIST_PRICE,
       SUM(LM_SALES_LP) AS LM_SALES_LP,
       SUM(P3M_SALES_LP) AS P3M_SALES_LP,
       SUM(P6M_SALES_LP) AS P6M_SALES_LP,
       SUM(P12M_SALES_LP) AS P12M_SALES_LP,
         SUM(size_of_price_lm_lp)  AS size_of_price_lm_lp,
        SUM(size_of_price_p3m_lp) As size_of_price_p3m_lp,
        SUM(size_of_price_p6m_lp) AS size_of_price_p6m_lp,
        SUM(size_of_price_p12m_lp) AS  size_of_price_p12m_lp,
        current_timestamp()::date as crt_dttm,
        null as cm_actual_stores,
        null as cm_universe_stores,
        null as cm_numeric_distribution,
        null as lm_actual_stores,
        null as lm_universe_stores,
        null as lm_numeric_distribution,
        null as l3m_actual_stores,
        null as l3m_universe_stores,
        null as l3m_numeric_distribution,
        null as l6m_actual_stores,
        null as l6m_universe_stores,
        null as l6m_numeric_distribution,
        null as l12m_actual_stores,
        null as l12m_universe_stores,
        null as l12m_numeric_distribution
 FROM th_edw_rpt_retail_excellence_summary_base
 WHERE

FISC_PER > TO_CHAR(ADD_MONTHS((SELECT to_date(MAX(fisc_per)::varchar,'YYYYMM') FROM th_edw_rpt_retail_excellence_summary_base),-15),'YYYYMM')	
  AND FISC_PER <= (select max(fisc_per) FROM th_edw_rpt_retail_excellence_summary_base)	
GROUP BY FISC_YR,
       FISC_PER,
       CLUSTER,
       MARKET,
       FLAG_AGG_DIM_KEY,
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
       PROD_HIER_L6,
       PROD_HIER_L7,
       PROD_HIER_L8,
       PROD_HIER_L9,
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
),
final as 
(
select
    fisc_yr::VARCHAR(11) as fisc_yr
    ,fisc_per::numeric(18,0) as fisc_per        
    ,cluster::VARCHAR(100) as cluster
    ,market::VARCHAR(50) as market  
    ,data_src::VARCHAR(14) as   data_src    
    ,flag_agg_dim_key::VARCHAR(50) as   flag_agg_dim_key    
    ,distributor_code::VARCHAR(500) as  distributor_code    
    ,distributor_name::VARCHAR(500) as  distributor_name    
    ,sell_out_channel::VARCHAR(255) as  sell_out_channel    
    ,region::VARCHAR(255) as    region  
    ,zone_name::VARCHAR(510) as zone_name  
    ,city::VARCHAR(255)  as city    
    ,retail_environment::VARCHAR(500) as retail_environment    
    ,prod_hier_l1::VARCHAR(100) as  prod_hier_l1    
    ,prod_hier_l2::VARCHAR(50) as   prod_hier_l2    
    ,prod_hier_l3::VARCHAR(255) as  prod_hier_l3    
    ,prod_hier_l4::VARCHAR(255) as  prod_hier_l4    
    ,prod_hier_l5::VARCHAR(255) as  prod_hier_l5    
    ,prod_hier_l6::VARCHAR(255) as  prod_hier_l6    
    ,prod_hier_l7::VARCHAR(50) as   prod_hier_l7    
    ,prod_hier_l8::VARCHAR(50) as   prod_hier_l8    
    ,prod_hier_l9::VARCHAR(50) as   prod_hier_l9    
    ,global_product_franchise::VARCHAR(30) as   global_product_franchise    
    ,global_product_brand::VARCHAR(30) as   global_product_brand    
    ,global_product_sub_brand::VARCHAR(100) as  global_product_sub_brand    
    ,global_product_segment::VARCHAR(200) as    global_product_segment  
    ,global_product_subsegment::VARCHAR(100) as global_product_subsegment  
    ,global_product_category::VARCHAR(200) as global_product_category      
    ,global_product_subcategory::VARCHAR(200) as global_product_subcategory    
    ,lm_sales_flag::VARCHAR(1) as   lm_sales_flag  
    ,p3m_sales_flag::VARCHAR(1) as  p3m_sales_flag  
    ,p6m_sales_flag::VARCHAR(1) as  p6m_sales_flag  
    ,p12m_sales_flag::VARCHAR(1) as p12m_sales_flag
    ,mdp_flag::VARCHAR(1) as    mdp_flag    
    ,target_complaince::numeric(38,6) as target_complaince      
    ,sales_value::NUMERIC(38,6) as  sales_value
    ,sales_qty::NUMERIC(38,6) as sales_qty      
    ,avg_sales_qty::NUMERIC(38,6) as avg_sales_qty      
    ,lm_sales::NUMERIC(38,6) as lm_sales    
    ,lm_sales_qty::NUMERIC(38,6) as lm_sales_qty    
    ,lm_avg_sales_qty::NUMERIC(38,6) as lm_avg_sales_qty    
    ,p3m_sales::NUMERIC(38,6) as p3m_sales      
    ,p3m_qty::NUMERIC(38,6) as  p3m_qty
    ,p3m_avg_qty::NUMERIC(38,6) as  p3m_avg_qty
    ,p6m_sales::NUMERIC(38,6) as p6m_sales      
    ,p6m_qty::NUMERIC(38,6) as  p6m_qty
    ,p6m_avg_qty::NUMERIC(38,6) as  p6m_avg_qty
    ,p12m_sales::NUMERIC(38,6) as p12m_sales        
    ,p12m_qty::NUMERIC(38,6) as p12m_qty    
    ,p12m_avg_qty::NUMERIC(38,6) as p12m_avg_qty    
    ,f3m_sales::NUMERIC(38,6) as    f3m_sales  
    ,f3m_qty::NUMERIC(38,6) as f3m_qty      
    ,f3m_avg_qty::NUMERIC(38,6) as  f3m_avg_qty
    ,size_of_price_lm::NUMERIC(38,14) as    size_of_price_lm    
    ,size_of_price_p3m::NUMERIC(38,14) as size_of_price_p3m    
    ,size_of_price_p6m::NUMERIC(38,14) as   size_of_price_p6m  
    ,size_of_price_p12m::NUMERIC(38,14) as  size_of_price_p12m  
    ,lm_sales_flag_count::numeric(38,0)  as lm_sales_flag_count
    ,p3m_sales_flag_count::numeric(38,0) as p3m_sales_flag_count        
    ,p6m_sales_flag_count::numeric(38,0) as p6m_sales_flag_count        
    ,p12m_sales_flag_count::numeric(38,0) as p12m_sales_flag_count  
    ,mdp_flag_count::numeric(38,0)   as mdp_flag_count  
    ,list_price::NUMERIC(20,4) as list_price        
    ,sales_value_list_price::NUMERIC(38,6) as   sales_value_list_price  
    ,lm_sales_lp::NUMERIC(38,6) as  lm_sales_lp
    ,p3m_sales_lp::NUMERIC(38,6) as p3m_sales_lp    
    ,p6m_sales_lp::NUMERIC(38,6) as p6m_sales_lp    
    ,p12m_sales_lp::NUMERIC(38,6) as p12m_sales_lp      
    ,size_of_price_lm_lp::NUMERIC(38,14) as size_of_price_lm_lp
    ,size_of_price_p3m_lp::NUMERIC(38,14) as size_of_price_p3m_lp      
    ,size_of_price_p6m_lp::NUMERIC(38,14) as size_of_price_p6m_lp      
    ,size_of_price_p12m_lp::NUMERIC(38,14) as size_of_price_p12m_lp    
    ,crt_dttm :: date as crt_dttm
    ,cm_actual_stores :: numeric(38,6) as cm_actual_stores
    ,cm_universe_stores :: numeric(38,6) as cm_universe_stores
    ,cm_numeric_distribution :: numeric(38,6) as cm_numeric_distribution
    ,lm_actual_stores :: numeric(38,6) as lm_actual_stores
    ,lm_universe_stores :: numeric(38,6) as lm_universe_stores
    ,lm_numeric_distribution :: numeric(38,6) as lm_numeric_distribution
    ,l3m_actual_stores :: numeric(38,6) as l3m_actual_stores
    ,l3m_universe_stores :: numeric(38,6) as l3m_universe_stores
    ,l3m_numeric_distribution :: numeric(38,6) as l3m_numeric_distribution
    ,l6m_actual_stores :: numeric(38,6) as l6m_actual_stores
    ,l6m_universe_stores :: numeric(38,6) as l6m_universe_stores
    ,l6m_numeric_distribution :: numeric(38,6) as l6m_numeric_distribution
    ,l12m_actual_stores :: numeric(38,6) as l12m_actual_stores
    ,l12m_universe_stores :: numeric(38,6) as l12m_universe_stores
    ,l12m_numeric_distribution :: numeric(38,6) as l12m_numeric_distribution
 from th_edw_rpt_retail_excellence_summary
)

--Final select
select * from final 