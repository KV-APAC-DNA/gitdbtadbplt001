--import cte

with wks_china_personal_care_regional_sellout_allmonths as (
    select * from {{ ref('chnwks_integration_wks_china_personal_care_regional_sellout_allmonths')}}
),
wks_china_personal_care_regional_sellout_act_lm 
as 
(
    select so.cntry_cd,
       so.sellout_dim_key,
       so.data_src,
       month,
       so_sls_value,
       sum(so_sls_qty) over (partition by cntry_cd,sellout_dim_key order by month rows between 1 preceding and 1 preceding) as lm_sales_qty,
       sum(so_sls_value) over (partition by cntry_cd,sellout_dim_key order by month rows between 1 preceding and 1 preceding) as lm_sales,
       avg(so_sls_qty) over (partition by cntry_cd,sellout_dim_key order by month rows between 1 preceding and 1 preceding) as lm_avg_sales_qty,
       sum(sales_value_list_price) over (partition by cntry_cd,sellout_dim_key order by month rows between 1 preceding and 1 preceding) lm_sales_lp
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
    lm_sales_qty :: numeric(38,6) as lm_sales_qty,
    lm_sales :: numeric(38,6) as lm_sales,
    lm_avg_sales_qty :: numeric(38,6) as lm_avg_sales_qty,
    lm_sales_lp :: numeric(38,12) as lm_sales_lp 
    from wks_china_personal_care_regional_sellout_act_lm 
)

select * from final
