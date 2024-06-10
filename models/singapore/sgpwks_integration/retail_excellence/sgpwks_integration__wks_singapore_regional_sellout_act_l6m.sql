--overwriding default sql header as we dont want to change timezone to singapore
{{
    config(
        sql_header= ""
    )
}}


with wks_singapore_regional_sellout_allmonths as (
    select * from {{ ref('sgpwks_integration__wks_singapore_regional_sellout_allmonths') }}
),

--final cte
singapore_regional_sellout_act_l6m  as (
select so.cntry_cd,		
       so.sellout_dim_key,		
       month,
       so_sls_value,
       sum(so_sls_qty) over (partition by cntry_cd,sellout_dim_key order by month rows between 5 preceding and current row) as l6m_sales_qty,
       sum(so_sls_value) over (partition by cntry_cd,sellout_dim_key order by month rows between 5 preceding and current row) as l6m_sales,
       avg(so_sls_qty) over (partition by cntry_cd,sellout_dim_key order by month rows between 5 preceding and current row) as l6m_avg_sales_qty,		
       sum(sales_value_list_price) over (partition by cntry_cd,sellout_dim_key order by month rows between 5 preceding and current row) as l6m_sales_lp
from wks_singapore_regional_sellout_allmonths so		
order by so.cntry_cd,
         so.sellout_dim_key,		
         month
)


--final select
select * from singapore_regional_sellout_act_l6m

