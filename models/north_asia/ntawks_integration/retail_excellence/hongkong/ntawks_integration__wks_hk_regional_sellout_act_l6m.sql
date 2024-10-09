--import cte

with wks_hk_regional_sellout_allmonths as (
    select * from {{ ref('ntawks_integration__wks_hk_regional_sellout_allmonths')}}
),

--final cte

wks_hk_regional_sellout_act_l6m as 
(
    SELECT SO.CNTRY_CD,
       SO.DIM_KEY,
       SO.DATA_SOURCE,
       MONTH,
       so_sls_value,
       SUM(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,dim_key ORDER BY month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS L6M_SALES_QTY,
       SUM(SO_SLS_VALUE) OVER (PARTITION BY cntry_cd,dim_key ORDER BY month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS L6M_SALES,
       AVG(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,dim_key ORDER BY month ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS L6M_AVG_SALES_QTY,
	   SUM(SALES_VALUE_LIST_PRICE) OVER (PARTITION BY cntry_cd,dim_key ORDER BY MONTH ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS L6M_SALES_LP
    FROM WKS_HK_REGIONAL_SELLOUT_ALLMONTHS SO
    ORDER BY SO.CNTRY_CD,
         SO.DIM_KEY,
         SO.DATA_SOURCE,
         MONTH
),

final as 
(
    select
    cntry_cd :: varchar(2) as cntry_cd,
    dim_key :: varchar(32) as dim_key,
    data_source :: varchar(14) as data_source,
    month :: varchar(27) as month,
    so_sls_value :: numeric(38,6) as so_sls_value,
    l6m_sales_qty :: numeric(38,6) as l6m_sales_qty,
    l6m_sales :: numeric(38,6) as l6m_sales,
    l6m_avg_sales_qty :: numeric(38,6) as l6m_avg_sales_qty,
    l6m_sales_lp :: numeric(38,12) as l6m_sales_lp
    from wks_hk_regional_sellout_act_l6m
)

--final select 

select * from final