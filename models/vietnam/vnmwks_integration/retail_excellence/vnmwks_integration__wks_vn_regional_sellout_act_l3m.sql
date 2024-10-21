--import cte

with wks_vn_regional_sellout_allmonths as (
    select * from {{ ref('vnmwks_integration__wks_vn_regional_sellout_allmonths')}}
),

--final cte

wks_vn_regional_sellout_act_l3m as 
(
    SELECT SO.CNTRY_CD,
       SO.SELLOUT_DIM_KEY,
       SO.DATA_SRC,
       MONTH,
       so_sls_value,
       SUM(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,SELLOUT_DIM_KEY ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS L3M_SALES_QTY,
       SUM(SO_SLS_VALUE) OVER (PARTITION BY cntry_cd,SELLOUT_DIM_KEY ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS L3M_SALES,
       AVG(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,SELLOUT_DIM_KEY ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS L3M_AVG_SALES_QTY,
	   SUM(SALES_VALUE_LIST_PRICE) OVER (PARTITION BY cntry_cd,SELLOUT_DIM_KEY ORDER BY MONTH ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS L3M_SALES_LP,
       SUM(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,SELLOUT_DIM_KEY ORDER BY month ROWS BETWEEN CURRENT ROW AND 2 following) AS F3M_SALES_QTY,
       SUM(SO_SLS_VALUE) OVER (PARTITION BY cntry_cd,SELLOUT_DIM_KEY ORDER BY month ROWS BETWEEN CURRENT ROW AND 2 following) AS F3M_SALES,
       AVG(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,SELLOUT_DIM_KEY ORDER BY month ROWS BETWEEN CURRENT ROW AND 2 following) AS F3M_AVG_SALES_QTY
    FROM WKS_VN_REGIONAL_SELLOUT_ALLMONTHS SO
    ORDER BY SO.CNTRY_CD,
         SO.SELLOUT_DIM_KEY,
         SO.DATA_SRC,
         MONTH
),

final as 
(
    select cntry_cd :: varchar(2) as cntry_cd,
    sellout_dim_key :: varchar(32) as sellout_dim_key,
    data_src :: varchar(14) as data_src,
    month :: varchar(27) as month,
    so_sls_value :: numeric(38,6) as so_sls_value,
    l3m_sales_qty :: numeric(38,6) as l3m_sales_qty,
    l3m_sales :: numeric(38,6) as l3m_sales,
    l3m_avg_sales_qty :: numeric(38,6) as l3m_avg_sales_qty,
    l3m_sales_lp :: numeric(38,12) as l3m_sales_lp,
    f3m_sales_qty :: numeric(38,6) as f3m_sales_qty,
    f3m_sales :: numeric(38,6) as f3m_sales,
    f3m_avg_sales_qty :: numeric(38,6) as f3m_avg_sales_qty
    from wks_vn_regional_sellout_act_l3m
)

--final select

select * from final