--import cte     
with wks_my_re_allmonths as (
    select * from {{ ref('myswks_integration__wks_my_re_allmonths') }}
),

--final cte
my_re_act_lm  as (
SELECT SO.CNTRY_CD,		
       SO.SELLOUT_DIM_KEY,		
       MONTH,
       so_sls_value,
       sum(so_sls_qty) over (partition by cntry_cd,sellout_dim_key order by month rows between 1 preceding and 1 preceding) as lm_sales_qty,
       sum(so_sls_value) over (partition by cntry_cd,sellout_dim_key order by month rows between 1 preceding and 1 preceding) as lm_sales,
       avg(so_sls_qty) over (partition by cntry_cd,sellout_dim_key order by month rows between 1 preceding and 1 preceding) as lm_avg_sales_qty,
	   sum(sales_value_list_price) over (partition by cntry_cd,sellout_dim_key order by month rows between 1 preceding and 1 preceding) lm_sales_lp
FROM WKS_MY_RE_ALLMONTHS SO		
ORDER BY SO.CNTRY_CD,
         SO.SELLOUT_DIM_KEY,		
         MONTH
),
final as 
(
    select 
cntry_cd::varchar(2) AS cntry_cd,
sellout_dim_key::varchar(32) AS sellout_dim_key,
month::varchar(23) AS month,
so_sls_value::numeric(38,6) AS so_sls_value,
lm_sales_qty::numeric(38,6) AS lm_sales_qty,
lm_sales::numeric(38,6) AS lm_sales,
lm_avg_sales_qty::numeric(38,6) AS lm_avg_sales_qty,
lm_sales_lp::numeric(38,12) AS lm_sales_lp
from my_re_act_lm
)


--final select
select * from final	