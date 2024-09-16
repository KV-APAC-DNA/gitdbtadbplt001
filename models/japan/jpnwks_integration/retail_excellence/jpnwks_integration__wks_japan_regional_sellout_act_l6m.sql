with WKS_JAPAN_REGIONAL_SELLOUT_ALLMONTHS as (
    select * from {{ ref('jpnwks_integration__wks_japan_regional_sellout_allmonths') }}
),

transformation as (
SELECT SO.CNTRY_CD,
       SO.SELLOUT_DIM_KEY,
       MONTH,
       SO_SLS_VALUE,
       SUM(SO_SLS_QTY) OVER (PARTITION BY CNTRY_CD,SELLOUT_DIM_KEY ORDER BY MONTH ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS L6M_SALES_QTY,
       SUM(SO_SLS_VALUE) OVER (PARTITION BY CNTRY_CD,SELLOUT_DIM_KEY ORDER BY MONTH ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS L6M_SALES,
       AVG(SO_SLS_QTY) OVER (PARTITION BY CNTRY_CD,SELLOUT_DIM_KEY ORDER BY MONTH ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS L6M_AVG_SALES_QTY,
       SUM(SALES_VALUE_LIST_PRICE) OVER (PARTITION BY CNTRY_CD,SELLOUT_DIM_KEY ORDER BY MONTH ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS L6M_SALES_LP
FROM WKS_JAPAN_REGIONAL_SELLOUT_ALLMONTHS SO
ORDER BY SO.CNTRY_CD,
         SO.SELLOUT_DIM_KEY,
         MONTH
),

final as (
select
cntry_cd::varchar(2) AS cntry_cd,
sellout_dim_key::varchar(32) AS sellout_dim_key,
month::varchar(23) AS month,
so_sls_value::numeric(38,6) AS so_sls_value,
l6m_sales_qty::numeric(38,6) AS l6m_sales_qty,
l6m_sales::numeric(38,6) AS l6m_sales,
l6m_avg_sales_qty::numeric(38,6) AS l6m_avg_sales_qty,
l6m_sales_lp::numeric(38,12) AS l6m_sales_lp
from transformation
)

select * from final