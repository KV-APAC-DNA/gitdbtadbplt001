with wks_singapore_regional_sellout_allmonths as (
    select * from {{ ref('sgpwks_integration__wks_singapore_regional_sellout_allmonths') }}
),

--final cte
singapore_regional_sellout_act_l12m  as (
select so.cntry_cd,		
       so.sellout_dim_key,		
       month,
       so_sls_value,
       sum(so_sls_qty) over (partition by cntry_cd,sellout_dim_key order by month rows between 11 preceding and current row) as l12m_sales_qty,
       sum(so_sls_value) over (partition by cntry_cd,sellout_dim_key order by month rows between 11 preceding and current row) as l12m_sales,
       avg(so_sls_qty) over (partition by cntry_cd,sellout_dim_key order by month rows between 11 preceding and current row) as l12m_avg_sales_qty,		
       sum(sales_value_list_price) over (partition by cntry_cd,sellout_dim_key order by month rows between 11 preceding and current row) as l12m_sales_lp
from wks_singapore_regional_sellout_allmonths so		
order by so.cntry_cd,
         so.sellout_dim_key,		
         month
),
final as
(
    select 
     cntry_cd::varchar(2),		  
    sellout_dim_key::varchar(32) ,
    month ::varchar(23),		
    so_sls_value::numeric(38,6),		
    l12m_sales_qty:: numeric(38,6),
    l12m_sales ::numeric(38,6),		
    l12m_avg_sales_qty ::numeric(38,6),		
    l12m_sales_lp ::numeric(38,12)
    from singapore_regional_sellout_act_l12m
)

--final select
select * from final

