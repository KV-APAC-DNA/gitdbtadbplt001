--import cte     
with wks_my_re_allmonths as (
    select * from {{ ref('myswks_integration__wks_my_re_allmonths') }}
),

--final cte
my_re_act_l6m  as (
SELECT SO.CNTRY_CD,		--// SELECT SO.CNTRY_CD,
       SO.SELLOUT_DIM_KEY,		--//        SO.sellout_dim_key,
       MONTH,
       so_sls_value,
       SUM(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS L6M_SALES_QTY,
       SUM(SO_SLS_VALUE) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS L6M_SALES,
       TRUNC(AVG(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)) AS L6M_AVG_SALES_QTY,		--// AVG
       SUM(SALES_VALUE_LIST_PRICE) OVER (PARTITION BY CNTRY_CD,SELLOUT_DIM_KEY ORDER BY MONTH ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS L6M_SALES_LP
FROM WKS_MY_RE_ALLMONTHS SO		--// FROM os_wks.WKS_MY_RE_ALLMONTHS SO
ORDER BY SO.CNTRY_CD,
         SO.SELLOUT_DIM_KEY,		--//          SO.sellout_dim_key,
         MONTH
),
final as
(
    select 
cntry_cd::varchar(2) AS cntry_cd,
sellout_dim_key::varchar(32) AS sellout_dim_key,
month::varchar(23) AS month,
so_sls_value::numeric(38,6) AS so_sls_value,
l6m_sales_qty::numeric(38,6) AS l6m_sales_qty,
l6m_sales::numeric(38,6) AS l6m_sales,
l6m_avg_sales_qty::numeric(38,6) AS l6m_avg_sales_qty,
l6m_sales_lp::numeric(38,12) AS l6m_sales_lp
from my_re_act_l6m
)
--final select
select * from final	