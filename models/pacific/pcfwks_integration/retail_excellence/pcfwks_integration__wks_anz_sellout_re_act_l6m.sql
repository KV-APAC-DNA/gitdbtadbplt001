with wks_anz_sellout_re_allmonths as (
    select * from {{ ref('pcfwks_integration__wks_anz_sellout_re_allmonths') }}
),
--final cte
anz_sellout_re_act_l6m  as (
select so.cntry_cd,
       so.sellout_dim_key,
       month,
       so_sls_value,
       sum(so_sls_qty) over (partition by cntry_cd,sellout_dim_key order by month rows between 5 preceding and current row) as l6m_sales_qty,
       sum(so_sls_value) over (partition by cntry_cd,sellout_dim_key order by month rows between 5 preceding and current row) as l6m_sales,
       avg(so_sls_qty) over (partition by cntry_cd,sellout_dim_key order by month rows between 5 preceding and current row) as l6m_avg_sales_qty,
       sum(sales_value_list_price) over (partition by cntry_cd,sellout_dim_key order by month rows between 5 preceding and current row) as l6m_sales_lp
from wks_anz_sellout_re_allmonths so
order by so.cntry_cd,
         so.sellout_dim_key,
         month
),
final as 
(
select 
cntry_cd::VARCHAR(2) AS cntry_cd,
sellout_dim_key::VARCHAR(32) AS sellout_dim_key,
month::VARCHAR(23) AS month,
so_sls_value::NUMERIC(38,6) AS so_sls_value,
l6m_sales_qty::NUMERIC(38,6) AS l6m_sales_qty,
l6m_sales::NUMERIC(38,6) AS l6m_sales,
l6m_avg_sales_qty::NUMERIC(38,6) AS l6m_avg_sales_qty,
l6m_sales_lp::NUMERIC(38,12) AS l6m_sales_lp
    from anz_sellout_re_act_l6m
)
--final select
select * from final