--import cte

with wks_philippines_regional_sellout_allmonths as (
    select * from {{ ref('phlwks_integration__wks_philippines_regional_sellout_allmonths')}}
),

--final cte

wks_philippines_regional_sellout_act_lm as 
(
    select SO.CNTRY_CD,
       SO.DIM_KEY,
       SO.DATA_SOURCE,
       MONTH,
       so_sls_value,
       SUM(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,dim_key ORDER BY month ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS LM_SALES_QTY,
       SUM(SO_SLS_VALUE) OVER (PARTITION BY cntry_cd,dim_key ORDER BY month ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS LM_SALES,
       AVG(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,dim_key ORDER BY month ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS LM_AVG_SALES_QTY,
	   SUM(SALES_VALUE_LIST_PRICE) OVER (PARTITION BY cntry_cd,dim_key ORDER BY MONTH ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS LM_SALES_LP
    FROM WKS_PHILIPPINES_REGIONAL_SELLOUT_ALLMONTHS SO
    ORDER BY SO.CNTRY_CD,
         SO.DIM_KEY,
         SO.DATA_SOURCE,
         MONTH
),

final as 
(
    select cntry_cd :: varchar(2) as cntry_cd,
    dim_key :: varchar(32) as dim_key,
    data_source :: varchar(14) as data_source,
    month :: varchar(27) as month,
    so_sls_value :: numeric(38,6) as so_sls_value,
    lm_sales_qty :: numeric(38,6) as lm_sales_qty,
    lm_sales :: numeric(38,6) as lm_sales,
    lm_avg_sales_qty :: numeric(38,6) as lm_avg_sales_qty,
    lm_sales_lp :: numeric(38,12) as lm_sales_lp
    from wks_philippines_regional_sellout_act_lm
)

--final select

select * from final