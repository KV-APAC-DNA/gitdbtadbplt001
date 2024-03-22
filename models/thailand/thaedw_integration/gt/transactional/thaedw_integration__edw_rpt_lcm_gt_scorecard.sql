with edw_vw_rpt_lcm_gt_scorecard as (
select * from {{ ref('thaedw_integration__edw_vw_rpt_lcm_gt_scorecard') }}
),
itg_query_parameters as 
(
select * from {{ source('thaitg_integration', 'itg_query_parameters') }}
),
itg_mds_th_lcm_exchange_rate as (
select * from {{ ref('thaitg_integration__itg_mds_th_lcm_exchange_rate') }}
),

union_1 as (select
  identifier,
  cntry_cd,
  crncy_cd,
  cntry_nm,
  year_month,
  "year" as year,
  "month" as month,
  distributor_id,
  trim(retail_env) as store_type,
  trim(salesarea) as sales_area,
  coalesce(sum(sell_out), 0) as sell_out,
  coalesce(sum(gross_sell_out), 0) as gross_sell_out,
  coalesce(sum(net_sell_out), 0) as net_sell_out,
  coalesce(sum(sellout_target), 0) as sellout_target,
  coalesce(sum(planned_call_count), 0) as planned_call_count,
  coalesce(sum(visited_call_count), 0) as visited_call_count,
  coalesce(sum(effective_call_count), 0) as effective_call_count,
  coalesce(sum(coverage_stores_count), 0) as coverage_stores_count,
  coalesce(sum(reactivate_stores_count), 0) as reactivate_stores_count,
  coalesce(sum(inactive_stores_count), 0) as inactive_stores_count,
  (
    sum(coalesce(coverage_stores_count, '0')) - sum(coalesce(inactive_stores_count, '0')) + sum(coalesce(reactivate_stores_count, '0'))
  ) as active_stores_count,
  '0' as msl_actual_count,
  '0' as msl_target_count,
  coalesce(sum(sales_order_count), 0) as sales_order_count,
  coalesce(sum(on_time_count), 0) as on_time_count,
  coalesce(sum(in_full_count), 0) as in_full_count,
  coalesce(sum(otif_count), 0) as otif_count,
  null as exch_rate
from edw_vw_rpt_lcm_gt_scorecard
where
  "year" >= (
    date_part(year, current_timestamp()) - (
      select
        cast(parameter_value as int)
      from itg_query_parameters
      where
        country_code = 'LCM' AND parameter_name = 'LCM_GT_Scorecard_Retention_Period'
    )
  )
  and upper(cntry_cd) = 'LA'
Group by
  identifier,
  cntry_cd,
  crncy_cd,
  cntry_nm,
  year_month,
  "year",
  "month",
  distributor_id,
  trim(retail_env),
  trim(salesarea)
  ),

union_2 as (
select
  cbd.*,
  exch_rt.exch_rate
from (
  select
    identifier,
    cntry_cd,
    crncy_cd,
    cntry_nm,
    year_month,
    "year" as year,
    "month" as month,
    distributor_id,
    trim(retail_env) as store_type,
    trim(salesarea) as sales_area,
    coalesce(sum(sell_out), 0) as sell_out,
    coalesce(sum(gross_sell_out), 0) as gross_sell_out,
    coalesce(sum(net_sell_out), 0) as net_sell_out,
    coalesce(sum(sellout_target), 0) as sellout_target,
    coalesce(sum(planned_call_count), 0) as planned_call_count,
    coalesce(sum(visited_call_count), 0) as visited_call_count,
    coalesce(sum(effective_call_count), 0) as effective_call_count,
    coalesce(sum(coverage_stores_count), 0) as coverage_stores_count,
    coalesce(sum(reactivate_stores_count), 0) as reactivate_stores_count,
    coalesce(sum(inactive_stores_count), 0) as inactive_stores_count,
    (
      sum(coalesce(coverage_stores_count, '0')) - sum(coalesce(inactive_stores_count, '0')) + sum(coalesce(reactivate_stores_count, '0'))
    ) as active_stores_count,
    cast('0' as bigint) as msl_actual_count,
    cast('0' as bigint) as msl_target_count,
    coalesce(sum(sales_order_count), 0) as sales_order_count,
    coalesce(sum(on_time_count), 0) as on_time_count,
    coalesce(sum(in_full_count), 0) as in_full_count,
    coalesce(sum(otif_count), 0) as otif_count
  from edw_vw_rpt_lcm_gt_scorecard
  where
    "year" >= (
      date_part(year, current_timestamp()) - (
        select
          cast(parameter_value as int)
        from itg_query_parameters
        where
          country_code = 'LCM' and parameter_name = 'LCM_GT_Scorecard_Retention_Period'
      )
    )
    and upper(cntry_cd) = 'CBD'
  group by
    identifier,
    cntry_cd,
    crncy_cd,
    cntry_nm,
    year_month,
    "year",
    "month",
    distributor_id,
    trim(retail_env)  ,
    trim(salesarea) 
) as cbd
left join itg_mds_th_lcm_exchange_rate as exch_rt
  on trim(upper(exch_rt.cntry_key)) = 'CBD'
  and trim(upper(exch_rt.from_ccy)) = 'USD'
  and trim(upper(exch_rt.to_ccy)) = 'THB'
  and cast(year_month as text) >= cast((
    left(cast(valid_from as date), 4) || substring(cast(valid_from as date), 6, 2)
  ) as text)
  and cast(year_month as text) <= cast((
    left(cast(valid_to as date), 4) || substring(cast(valid_to as date), 6, 2)
  ) as text)
),
union_3 as (
select
  mym.*,
  exch_rt.exch_rate
from (
  select
    identifier,
    cntry_cd,
    crncy_cd,
    cntry_nm,
    year_month,
    "year" as year,
    "month" as month,
    distributor_id,
    trim(retail_env) as store_type,
    trim(salesarea) as sales_area,
    coalesce(sum(sell_out), 0) as sell_out,
    coalesce(sum(gross_sell_out), 0) as gross_sell_out,
    coalesce(sum(net_sell_out), 0) as net_sell_out,
    coalesce(sum(sellout_target), 0) as sellout_target,
    coalesce(sum(planned_call_count), 0) as planned_call_count,
    coalesce(sum(visited_call_count), 0) as visited_call_count,
    coalesce(sum(effective_call_count), 0) as effective_call_count,
    coalesce(sum(coverage_stores_count), 0) as coverage_stores_count,
    coalesce(sum(reactivate_stores_count), 0) as reactivate_stores_count,
    coalesce(sum(inactive_stores_count), 0) as inactive_stores_count,
    (
      sum(coalesce(coverage_stores_count, '0')) - sum(coalesce(inactive_stores_count, '0')) + sum(coalesce(reactivate_stores_count, '0'))
    ) as active_stores_count,
    cast('0' as bigint) as msl_actual_count,
    cast('0' as bigint) as msl_target_count,
    coalesce(sum(sales_order_count), 0) as sales_order_count,
    coalesce(sum(on_time_count), 0) as on_time_count,
    coalesce(sum(in_full_count), 0) as in_full_count,
    coalesce(sum(otif_count), 0) as otif_count
  from edw_vw_rpt_lcm_gt_scorecard
  where
    "year" >= (
      date_part(year, current_timestamp()) - (
        select
          cast(parameter_value as int)
        from itg_query_parameters
        where
          country_code = 'LCM' AND parameter_name = 'LCM_GT_Scorecard_Retention_Period'
      )
    )
    and upper(cntry_cd) = 'MYM'
  group by
    identifier,
    cntry_cd,
    crncy_cd,
    cntry_nm,
    year_month,
    "year",
    "month",
    distributor_id,
    trim(retail_env),
    trim(salesarea)
) as mym
left join itg_mds_th_lcm_exchange_rate as exch_rt
  on trim(upper(exch_rt.cntry_key)) = 'MYM'
  and trim(upper(exch_rt.from_ccy)) = 'MMK'
  and trim(upper(exch_rt.to_ccy)) = 'THB'
  and cast(year_month as text) >= cast((
    left(cast(valid_from as date), 4) || substring(cast(valid_from as date), 6, 2)
  ) as text)
  and cast(year_month as text) <= cast((
    left(cast(valid_to as date), 4) || substring(cast(valid_to as date), 6, 2)
  ) as text)
),
transformed as (
select * from union_1
 union all
select * from union_2
 union all
select * from union_3
),
final as (
select
    identifier::varchar(20) as identifier,
    cntry_cd::varchar(4) as cntry_cd,
    crncy_cd::varchar(3) as crncy_cd,
    cntry_nm::varchar(20) as cntry_nm,
    year_month::varchar(10) as year_month,
    year::number(18,0) as year,
    month::number(18,0) as month,
    distributor_id::varchar(20) as distributor_id,
    store_type::varchar(100) as store_type,
    sales_area::varchar(500) as sales_area,
    sell_out::number(38,6) as sell_out_actual,
    gross_sell_out::number(38,6) as gross_sell_out_actual,
    net_sell_out::number(38,6) as net_sell_out_actual,
    sellout_target::number(38,6) as sellout_target,
    planned_call_count::number(38,0) as planned_call_count,
    visited_call_count::number(38,0) as visited_call_count,
    effective_call_count::number(38,0) as effective_call_count,
    coverage_stores_count::number(38,0) as coverage_stores_count,
    reactivate_stores_count::number(38,0) as reactivate_stores_count,
    inactive_stores_count::number(38,0) as inactive_stores_count,
    active_stores_count::number(38,0) as active_store_count,
    msl_actual_count::number(38,0) as msl_actual_count,
    msl_target_count::number(38,0) as msl_target_count,
    sales_order_count::number(38,0) as sales_order_count,
    on_time_count::number(38,0) as on_time_count,
    in_full_count::number(38,0) as in_full_count,
    otif_count::number(38,0) as otif_count,
    exch_rate::number(28,7) as exch_rate
from transformed
)
select * from final