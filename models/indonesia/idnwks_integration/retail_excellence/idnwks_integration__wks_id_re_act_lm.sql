--import cte
with wks_id_re_allmonths as (
    select * from {{ ref('idnwks_integration__wks_id_re_allmonths') }}
),

--final cte
wks_id_re_act_lm as (
    SELECT SO.CNTRY_CD,
       SO.sellout_dim_key,
       MONTH,
       so_sls_value,
       SUM(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS LM_SALES_QTY,
       SUM(SO_SLS_VALUE) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS LM_SALES,
       CAST(AVG(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS DECIMAL(10,2)) AS LM_AVG_SALES_QTY,
       SUM(SALES_VALUE_LIST_PRICE) OVER (PARTITION BY CNTRY_CD,SELLOUT_DIM_KEY ORDER BY MONTH ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) LM_SALES_LP
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
    lm_sales_qty::numeric(38,6) as lm_sales_qty,
    lm_sales::numeric(38,6) as lm_sales,
    lm_avg_sales_qty::numeric(10,2) as lm_avg_sales_qty,
    lm_sales_lp::numeric(38,12) as lm_sales_lp
    from wks_id_re_act_lm
)

--final select 
select * from final

