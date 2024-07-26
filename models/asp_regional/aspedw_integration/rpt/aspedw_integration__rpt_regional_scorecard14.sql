{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where datasource = 'GP'
        {% endif %}"
    )
}}
with v_rpt_rg_total_investment as(
    select * from aspedw_integration.v_rpt_rg_total_investment
),
itg_mds_ap_sales_ops_map as(
    select * from snapaspitg_integration.itg_mds_ap_sales_ops_map
),
gp_base as
  (select   
          ctry_map.destination_market  , 
          ctry_map.destination_cluster ,
          to_date (posting_fiscal_year||lpad(posting_fiscal_period_number,2,'0') ,'YYYYMM') as period,
          kpi,
          case 
              when kpi = 'GP'  then sum(usdf_mtd_amount)   
          end  as  gp_value,
          case
              when kpi = 'NTS' then sum(usdf_mtd_amount)       
          end as  finance_nts 
    from v_rpt_rg_total_investment  gp
       , itg_mds_ap_sales_ops_map ctry_map
  where kpi in ( 'GP' ,'NTS')
     and version_group_code = 'ACT'
     and ctry_map.dataset = 'Total Investment FDW' 
     and ctry_map.source_cluster = gp.cluster_name
     and ctry_map.source_market  = gp.country_name 
   group by ctry_map.destination_cluster , kpi,
            ctry_map.destination_market  , 
            to_date ( (posting_fiscal_year||lpad(posting_fiscal_period_number,2,'0') ), 'YYYYMM')
 ),
 final as(
    select 'GP' as datasource, 
          base.destination_market  , 
          base.destination_cluster ,
          cast(to_char(base.period,'YYYY')||'0'||to_char(base.period,'MM') as integer) fiscper ,
          base.gp_value,
          base.finance_nts,
          prev_yr.gp_value  as gp_value_prev_yr,
          prev_yr.finance_nts as finance_nts_prev_yr 
    from gp_base as base
  left join gp_base as prev_yr
 on  base.destination_market              = prev_yr.destination_market 
 and base.destination_cluster             = prev_yr.destination_cluster
 and base.kpi             = prev_yr.kpi
 and add_months(base.period, -12)     = prev_yr.period
 )
 select * from final
