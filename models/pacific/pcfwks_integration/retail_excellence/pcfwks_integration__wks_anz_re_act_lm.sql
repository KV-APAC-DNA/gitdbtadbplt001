with wks_anz_re_allmonths as (
    select * from {{ ref('pcfwks_integration__wks_anz_re_allmonths') }}
),
--final cte
anz_re_act_lm  as (
select so.cntry_cd,
       so.sellout_dim_key,
       month,
       so_sls_value,
       sum(so_sls_qty) over (partition by cntry_cd,sellout_dim_key order by month rows between 1 preceding and 1 preceding) as lm_sales_qty,
       sum(so_sls_value) over (partition by cntry_cd,sellout_dim_key order by month rows between 1 preceding and 1 preceding) as lm_sales,
       cast(avg(so_sls_qty) over (partition by cntry_cd,sellout_dim_key order by month rows between 1 preceding and 1 preceding) as decimal(10,2)) as lm_avg_sales_qty,
       sum(sales_value_list_price) over (partition by cntry_cd,sellout_dim_key order by month rows between 1 preceding and 1 preceding) lm_sales_lp
from wks_anz_re_allmonths so
order by so.cntry_cd,
         so.sellout_dim_key,
         month
),
final as 
(
    select 
cntry_cd::varchar(2) as cntry_cd,
sellout_dim_key::varchar(32) as sellout_dim_key,
month::varchar(23) as month,
so_sls_value::numeric(38,6) as so_sls_value,
lm_sales_qty::numeric(38,6) as lm_sales_qty,
lm_sales::numeric(38,6) as lm_sales,
lm_avg_sales_qty::numeric(10,2) as lm_avg_sales_qty,
lm_sales_lp::numeric(38,12) as lm_sales_lp
    from anz_re_act_lm
)
--final select
select * from final