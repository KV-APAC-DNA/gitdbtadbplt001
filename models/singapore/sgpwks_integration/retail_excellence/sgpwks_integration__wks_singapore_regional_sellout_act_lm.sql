with wks_singapore_regional_sellout_allmonths as (
    select * from {{ ref('sgpwks_integration__wks_singapore_regional_sellout_allmonths') }}
),

--final cte
singapore_regional_sellout_act_lm  as (
select so.cntry_cd,		
       so.sellout_dim_key,		
       month,
       so_sls_value,
       sum(so_sls_qty) over (partition by cntry_cd,sellout_dim_key order by month rows between 1 preceding and 1 preceding) as lm_sales_qty,
       sum(so_sls_value) over (partition by cntry_cd,sellout_dim_key order by month rows between 1 preceding and 1 preceding) as lm_sales,
       avg(so_sls_qty) over (partition by cntry_cd,sellout_dim_key order by month rows between 1 preceding and 1 preceding) as lm_avg_sales_qty,		--// avg
       sum(sales_value_list_price) over (partition by cntry_cd,sellout_dim_key order by month rows between 1 preceding and 1 preceding) lm_sales_lp
from wks_singapore_regional_sellout_allmonths so		
order by so.cntry_cd,
         so.sellout_dim_key,		
         month

),
final as 
(
    select 
  cntry_cd::varchar(2) AS cntry_cd,
sellout_dim_key::varchar(32), AS sellout_dim_key,
month::varchar(23) AS month,
so_sls_value::numeric(38,6) AS so_sls_value,
lm_sales_qty::numeric(38,6) AS lm_sales_qty,
lm_sales::numeric(38,6) AS lm_sales,
lm_avg_sales_qty::numeric(38,6) AS lm_avg_sales_qty

    from singapore_regional_sellout_act_lm
)


--final select
select * from final

