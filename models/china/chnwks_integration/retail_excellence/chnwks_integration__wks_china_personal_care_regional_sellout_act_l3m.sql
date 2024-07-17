--import cte

with wks_china_personal_care_regional_sellout_allmonths as (
    select * from {{ ref('chnwks_integration_wks_china_personal_care_regional_sellout_allmonths')}}
),
wks_china_personal_care_regional_sellout_act_l3m 
as 
(
    select so.cntry_cd,
       so.sellout_dim_key,
       so.data_src,
       month,
       so_sls_value,
       sum(so_sls_qty) over (partition by cntry_cd,sellout_dim_key order by month rows between 2 preceding and current row) as l3m_sales_qty,
       sum(so_sls_value) over (partition by cntry_cd,sellout_dim_key order by month rows between 2 preceding and current row) as l3m_sales,
       avg(so_sls_qty) over (partition by cntry_cd,sellout_dim_key order by month rows between 2 preceding and current row) as l3m_avg_sales_qty,
       sum(sales_value_list_price) over (partition by cntry_cd,sellout_dim_key order by month rows between 2 preceding and current row) as l3m_sales_lp,
       sum(so_sls_qty) over (partition by cntry_cd,sellout_dim_key order by month rows between current row and 2 following) as f3m_sales_qty,
       sum(so_sls_value) over (partition by cntry_cd,sellout_dim_key order by month rows between current row and 2 following) as f3m_sales,
       avg(so_sls_qty) over (partition by cntry_cd,sellout_dim_key order by month rows between current row and 2 following) as f3m_avg_sales_qty
from wks_china_personal_care_regional_sellout_allmonths so
order by so.cntry_cd,
         so.sellout_dim_key,
         so.data_src,
         month
),
final as 
(
    select cntry_cd ::varchar(2) as cntry_cd,
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
    f3m_avg_sales_qty :: numeric(38,6) as f3m_avg_sales_qty,
    from wks_china_personal_care_regional_sellout_act_l3m 
)

select * from final
