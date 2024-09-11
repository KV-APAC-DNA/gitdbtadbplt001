with wks_anz_pos_re_allmonths as (
    select * from {{ ref('pcfwks_integration__wks_anz_pos_re_allmonths') }}
),
--final cte
anz_pos_re_act_l12m  as (
select * from (
select  distinct A.*,B.universe_stores as L12M_universe_stores,B.numeric_distribution as L12M_numeric_distribution,row_number() over(partition by A.sellout_dim_key,A.MONTH order by B.MONTH) as rno 
 from (
SELECT SO.CNTRY_CD,
       SO.sellout_dim_key,
       MONTH,
       actual_stores,universe_stores as U1,
       max(actual_stores) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)::NUMERIC(38,6)  AS L12M_actual_stores ,
	     max(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS L12M_SALES_QTY,
       max(SO_SLS_VALUE) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS L12M_SALES,
       CAST(avg(SO_SLS_QTY) OVER (PARTITION BY cntry_cd,sellout_dim_key ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS DECIMAL(10,2)) AS L12M_AVG_SALES_QTY,
       max(SALES_VALUE_LIST_PRICE) OVER (PARTITION BY CNTRY_CD,SELLOUT_DIM_KEY ORDER BY month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) L12M_SALES_LP    
FROM wks_anz_pos_re_allmonths SO ) A LEFT JOIN  wks_anz_pos_re_allmonths B on A.sellout_dim_key=B.sellout_dim_key 
and A.CNTRY_CD=B.CNTRY_CD  and A.L12M_actual_stores=B.actual_stores and B.MONTH between to_char(ADD_MONTHS(to_date((A.month),'YYYYMM'),-11),'YYYYMM') and a.month
) where rno=1
ORDER BY CNTRY_CD,sellout_dim_key,MONTH
),
final as 
(
select 
cntry_cd::VARCHAR(2) AS cntry_cd,
sellout_dim_key::VARCHAR(32) AS sellout_dim_key,
month::VARCHAR(23) AS month,
actual_stores::NUMERIC(38,6) AS actual_stores,
u1::NUMERIC(38,14) AS u1,
l12m_actual_stores::NUMERIC(38,6) AS l12m_actual_stores,
l12m_sales_qty::NUMERIC(38,6) AS l12m_sales_qty,
l12m_sales::NUMERIC(38,6) AS l12m_sales,
l12m_avg_sales_qty::NUMERIC(10,2) AS l12m_avg_sales_qty,
l12m_sales_lp::NUMERIC(38,12) AS l12m_sales_lp,
l12m_universe_stores::NUMERIC(38,14) AS l12m_universe_stores,
l12m_numeric_distribution::NUMERIC(20,4) AS l12m_numeric_distribution,
rno::numeric(38,0) AS rno
    from anz_pos_re_act_l12m
)
--final select
select * from final