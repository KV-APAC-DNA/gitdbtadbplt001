--import cte
with wks_id_re_allmonths as (
    select * from {{ ref('idnwks_integration__wks_id_re_allmonths') }}
),

--final cte
wks_id_re_act_l12m as (
    SELECT SO.CNTRY_CD,
       SO.sellout_dim_key,
       MONTH,
       so_sls_value,
       SUM(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS L12M_SALES_QTY,
       SUM(SO_SLS_VALUE) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS L12M_SALES,
       AVG(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS L12M_AVG_SALES_QTY,
       SUM(SALES_VALUE_LIST_PRICE) OVER (PARTITION BY CNTRY_CD,SELLOUT_DIM_KEY ORDER BY MONTH ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS L12M_SALES_LP
FROM wks_id_re_allmonths SO
ORDER BY SO.CNTRY_CD,
         SO.sellout_dim_key,
         MONTH

),

final as (
    select 
    cntry_cd::varchar(2) as cntry_cd,
    sellout_dim_key::varchar(32) as sellout_dim_key,
    month::varchar(23) as month,
    so_sls_value::numeric(38,6) as so_sls_value,
    l12m_sales_qty::numeric(38,6) as l12m_sales_qty,
    l12m_sales::numeric(38,6) as l12m_sales,
    l12m_avg_sales_qty::numeric(38,6) as l12m_avg_sales_qty,
    l12m_sales_lp::numeric(38,12) as l12m_sales_lp
    from wks_id_re_act_l12m
)

--final select 
select * from final