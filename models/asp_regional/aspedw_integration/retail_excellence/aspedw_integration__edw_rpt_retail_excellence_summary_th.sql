--Overwriding default SQL header as we dont want to change timezone to Singapore
{{
    config(
        sql_header= ""
    )
}}

--Import CTE
with th_edw_rpt_retail_excellence_summary_base as (
    select * from {{ ref('aspedw_integration__th_edw_rpt_retail_excellence_summary_base') }}
),
--Logical CTE

--Final CTE
final as (
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
       TARGET_COMPLAINCE,
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
        current_timestamp()::date  as crt_dttm
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
      MDP_FLAG,
	  TARGET_COMPLAINCE

)

--Final select
select * from final 