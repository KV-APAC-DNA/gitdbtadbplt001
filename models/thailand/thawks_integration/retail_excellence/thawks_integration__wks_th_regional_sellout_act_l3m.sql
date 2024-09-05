with WKS_TH_REGIONAL_SELLOUT_ALLMONTHS as (
    select * from {{ ref('thawks_integration__wks_th_regional_sellout_allmonths') }}
),

transformation as (
    SELECT SO.CNTRY_CD,
       SO.sellout_dim_key,
       SO.data_src,
       MONTH,
       sls_value,
       SUM(SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS L3M_SALES_QTY,
       SUM(SLS_VALUE) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS L3M_SALES,
       AVG(SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS L3M_AVG_SALES_QTY,
       SUM(SALES_VALUE_LIST_PRICE) OVER (PARTITION BY CNTRY_CD,SELLOUT_DIM_KEY ORDER BY MONTH ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS L3M_SALES_LP,
       SUM(SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN CURRENT ROW AND 2 following) AS F3M_SALES_QTY,
       SUM(SLS_VALUE) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN CURRENT ROW AND 2 following) AS F3M_SALES,
       AVG(SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN CURRENT ROW AND 2 following) AS F3M_AVG_SALES_QTY
FROM WKS_TH_REGIONAL_SELLOUT_ALLMONTHS SO
ORDER BY SO.CNTRY_CD,
         SO.sellout_dim_key,
         SO.data_src,
         MONTH
),

final as (
select
cntry_cd::varchar(2) AS cntry_cd,
sellout_dim_key::varchar(32) AS sellout_dim_key,
data_src::varchar(14) AS data_src,
month::varchar(23) AS month,
sls_value::numeric(38,6) AS sls_value,
l3m_sales_qty::numeric(38,6) AS l3m_sales_qty,
l3m_sales::numeric(38,6) AS l3m_sales,
l3m_avg_sales_qty::numeric(38,6) AS l3m_avg_sales_qty,
l3m_sales_lp::numeric(38,12) AS l3m_sales_lp,
f3m_sales_qty::numeric(38,6) AS f3m_sales_qty,
f3m_sales::numeric(38,6) AS f3m_sales,
f3m_avg_sales_qty::numeric(38,6) AS f3m_avg_sales_qty
from transformation
)

select * from final