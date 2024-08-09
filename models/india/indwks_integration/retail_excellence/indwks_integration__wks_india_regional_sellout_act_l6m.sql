with WKS_INDIA_REGIONAL_SELLOUT_ALLMONTHS as 
(
select * from {{ ref('indwks_integration__wks_india_regional_sellout_allmonths') }}
),

wks_india_regional_sellout_act_l6m as(

SELECT SO.CNTRY_CD, SO.sellout_dim_key, MONTH , so_sls_value
 ,sum (SO_SLS_QTY) over ( partition by cntry_cd, sellout_dim_key order by month  rows between 5 preceding and current row ) AS  L6M_SALES_QTY
, sum(SO_SLS_VALUE) over ( partition by cntry_cd, sellout_dim_key order by month  rows between 5 preceding and current row ) AS L6M_SALES
, avg(SO_SLS_QTY) over ( partition by cntry_cd, sellout_dim_key order by month  rows between 5 preceding and current row ) AS L6M_AVG_SALES_QTY
,SUM(SALES_VALUE_LIST_PRICE) OVER (PARTITION BY CNTRY_CD,SELLOUT_DIM_KEY ORDER BY MONTH ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS L6M_SALES_LP
 FROM WKS_INDIA_REGIONAL_SELLOUT_ALLMONTHS SO
--WHERE sellout_dim_key in ( '105d8348b3394ef5bd2ff7986c950b91')
ORDER BY SO.CNTRY_CD, SO.sellout_dim_key, MONTH
),
final as(

select 
	cntry_cd::varchar(2) AS cntry_cd,
	sellout_dim_key::varchar(32) AS sellout_dim_key,
	month::varchar(23) AS month,
	so_sls_value::numeric(38,6) AS so_sls_value,
	l6m_sales_qty::numeric(38,6) AS l6m_sales_qty,
	l6m_sales::numeric(38,6) AS l6m_sales,
	l6m_avg_sales_qty::numeric(38,6) AS l6m_avg_sales_qty,
	l6m_sales_lp::numeric(38,12) AS l6m_sales_lp
from wks_india_regional_sellout_act_l6m

)
select * from final 