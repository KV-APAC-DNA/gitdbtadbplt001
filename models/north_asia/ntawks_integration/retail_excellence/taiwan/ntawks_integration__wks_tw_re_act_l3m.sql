--import cte     
with wks_tw_re_allmonths as (
    select * from {{ ref('ntawks_integration__wks_tw_re_allmonths') }}
),
TW_RE_ACT_L3M as (	 

SELECT SO.CNTRY_CD,
       SO.sellout_dim_key,
       MONTH,
       so_sls_value,
       SUM(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS L3M_SALES_QTY,
       SUM(SO_SLS_VALUE) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS L3M_SALES,
       AVG(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS L3M_AVG_SALES_QTY,
       SUM(SALES_VALUE_LIST_PRICE) OVER (PARTITION BY CNTRY_CD,SELLOUT_DIM_KEY ORDER BY MONTH ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS L3M_SALES_LP,
       SUM(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN CURRENT ROW AND 2 following) AS F3M_SALES_QTY,
       SUM(SO_SLS_VALUE) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN CURRENT ROW AND 2 following) AS F3M_SALES,
       AVG(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN CURRENT ROW AND 2 following) AS F3M_AVG_SALES_QTY
FROM WKS_TW_RE_ALLMONTHS SO
--WHERE sellout_dim_key in ( '105d8348b3394ef5bd2ff7986c950b91')
ORDER BY SO.CNTRY_CD,
         SO.sellout_dim_key,
         MONTH
		 ),
final as
(
    select 
cntry_cd::varchar(2) AS cntry_cd,
sellout_dim_key::varchar(32) AS sellout_dim_key,
month::varchar(23) AS month,
so_sls_value::numeric(38,6) AS so_sls_value,
l3m_sales_qty::numeric(38,6) AS l3m_sales_qty,
l3m_sales::numeric(38,6) AS l3m_sales,
l3m_avg_sales_qty::numeric(38,6) AS l3m_avg_sales_qty,
l3m_sales_lp::numeric(38,12) AS l3m_sales_lp,
f3m_sales_qty::numeric(38,6) AS f3m_sales_qty,
f3m_sales::numeric(38,6) AS f3m_sales,
f3m_avg_sales_qty::numeric(38,6) AS f3m_avg_sales_qty
from TW_RE_ACT_L3M
)
--final select
select * from final	