{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where datasource = 'DSO'
        {% endif %}"
    )
}}
with itg_mds_ap_dso as(
    select * from snapaspitg_integration.itg_mds_ap_dso
),
itg_mds_ap_sales_ops_map as(
    select * from snapaspitg_integration.itg_mds_ap_sales_ops_map
),
dso_base as 
    (select       
            market                
          , ctry_map.destination_cluster                 
          , to_date ( year||lpad(month,2,'0'),'YYYYMM' ) as year_month
          , sum(gts) as gts
          , sum(gross_account_receivable) as gross_account_receivable
          , max(jnj_days) as jnj_days
    from itg_mds_ap_dso dso
       , itg_mds_ap_sales_ops_map ctry_map 
    where ctry_map.dataset = 'DSO'
      and ctry_map.source_market  = dso.market 
    group by market                
          , ctry_map.destination_cluster                 
          , to_date ( year||lpad(month,2,'0'),'YYYYMM' )  
    ),
final as(
    Select 'DSO' as datasource   
  , base.market                
  , base.destination_cluster                 
  , cast(to_char(base.year_month,'YYYY')||'0'||to_char(base.year_month,'MM') as integer) fiscper
  , base.gts                           
  , base.gross_account_receivable
  , base.jnj_days                  
  , prev_yr.gts       as prev_yr_gts                
  , prev_yr.gross_account_receivable as prev_yr_gross_account_receivable
  , prev_yr.jnj_days  as prev_yr_jnj_days                 
from dso_base as base
  left join dso_base as prev_yr
 on  base.market                          = prev_yr.market 
 and base.destination_cluster             = prev_yr.destination_cluster
 and add_months(base.year_month, -12)     = prev_yr.year_month
)
select * from final