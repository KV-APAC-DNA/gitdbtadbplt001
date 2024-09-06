--import cte     
with wks_tw_re_allmonths as (
    select * from {{ ref('ntawks_integration__wks_tw_re_allmonths') }}
),

TW_RE_ACT_LM as (

SELECT SO.CNTRY_CD,
       SO.sellout_dim_key,
       MONTH,
       so_sls_value,
       SUM(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS LM_SALES_QTY,
       SUM(SO_SLS_VALUE) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS LM_SALES,
       CAST(AVG(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS DECIMAL(10,2)) AS LM_AVG_SALES_QTY,
       SUM(SALES_VALUE_LIST_PRICE) OVER (PARTITION BY CNTRY_CD,SELLOUT_DIM_KEY ORDER BY MONTH ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) LM_SALES_LP
FROM WKS_TW_RE_ALLMONTHS SO
--WHERE sellout_dim_key in ( 'b526ba0ea750a03d6ea7818ca70a60f2')
ORDER BY SO.CNTRY_CD,
         SO.sellout_dim_key,
         MONTH
),
final as (
select 
cntry_cd::varchar(2) AS cntry_cd,
sellout_dim_key::varchar(32) AS sellout_dim_key,
month::varchar(23) AS month,
so_sls_value::numeric(38,6) AS so_sls_value,
lm_sales_qty::numeric(38,6) AS lm_sales_qty,
lm_sales::numeric(38,6) AS lm_sales,
lm_avg_sales_qty::numeric(38,6) AS lm_avg_sales_qty,
lm_sales_lp::numeric(38,12) AS lm_sales_lp
from TW_RE_ACT_LM
)
select * from final