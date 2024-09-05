--import cte     
with wks_my_re_allmonths as (
    select * from {{ ref('myswks_integration__wks_my_re_allmonths') }}
),

--final cte
my_re_act_l3m  as (
SELECT SO.CNTRY_CD,		
       SO.SELLOUT_DIM_KEY,		
       MONTH,
       so_sls_value,
       SUM(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS L3M_SALES_QTY,
       SUM(SO_SLS_VALUE) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS L3M_SALES,
       TRUNC(AVG(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)) AS L3M_AVG_SALES_QTY,		
       SUM(SALES_VALUE_LIST_PRICE) OVER (PARTITION BY CNTRY_CD,SELLOUT_DIM_KEY ORDER BY MONTH ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS L3M_SALES_LP,
       SUM(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN CURRENT ROW AND 2 following) AS F3M_SALES_QTY,
       SUM(SO_SLS_VALUE) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN CURRENT ROW AND 2 following) AS F3M_SALES,
       TRUNC(AVG(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN CURRENT ROW AND 2 following)) AS F3M_AVG_SALES_QTY		
FROM WKS_MY_RE_ALLMONTHS SO		
ORDER BY SO.CNTRY_CD,
         SO.SELLOUT_DIM_KEY,		
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
from my_re_act_l3m
)
--final select
select * from final	