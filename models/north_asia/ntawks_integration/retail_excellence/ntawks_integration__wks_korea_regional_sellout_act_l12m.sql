with wks_korea_regional_sellout_allmonths as (
    select * from {{ ref('ntawks_integration__wks_korea_regional_sellout_allmonths') }}
),

--final cte
korea_regional_sellout_act_l12m  as (
SELECT SO.CNTRY_CD,		--// SELECT SO.CNTRY_CD,
       SO.DIM_KEY,		--//        SO.dim_key,
       SO.DATA_SOURCE,		--//        SO.data_source,
       MONTH,
       so_sls_value,
       SUM(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,dim_key ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS L12M_SALES_QTY,
       SUM(SO_SLS_VALUE) OVER (PARTITION BY cntry_cd,dim_key ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS L12M_SALES,
       TRUNC(AVG(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,dim_key ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)) AS L12M_AVG_SALES_QTY,		--// AVG
	   SUM(SALES_VALUE_LIST_PRICE) OVER (PARTITION BY cntry_cd,dim_key ORDER BY MONTH ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS L12M_SALES_LP
FROM WKS_KOREA_REGIONAL_SELLOUT_ALLMONTHS SO		--// FROM NA_WKS.WKS_KOREA_REGIONAL_SELLOUT_ALLMONTHS SO
ORDER BY SO.CNTRY_CD,
         SO.DIM_KEY,		--//          SO.dim_key,
         SO.DATA_SOURCE,		--//          SO.data_source,
         MONTH

),
final as
(
    select 
cntry_cd::varchar(2) AS cntry_cd,
dim_key::varchar(32) AS dim_key,
data_source::varchar(14) AS data_source,
month::varchar(23) AS month,
so_sls_value::numeric(38,6) AS so_sls_value,
l12m_sales_qty::numeric(38,6) AS l12m_sales_qty,
l12m_sales::numeric(38,6) AS l12m_sales,
l12m_avg_sales_qty::numeric(38,6) AS l12m_avg_sales_qty,
l12m_sales_lp::numeric(38,12) AS l12m_sales_lp

 from korea_regional_sellout_act_l12m
)

--final select
select * from final

