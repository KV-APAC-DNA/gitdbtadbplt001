with vw_rpt_th_gt_scorecard as(
 select * from {{ ref('thaedw_integration__vw_rpt_th_gt_scorecard') }}
)

SELECT identifier::varchar(20) as identifier,
       cntry_cd::varchar(5) as cntry_cd,
       crncy_cd::varchar(3) as crncy_cd,
       cntry_nm::varchar(8) as cntry_nm,
       year_month::varchar(14) as year_month,
       to_number(year,18,0) as year,
       to_number(month,18,0) as month,
       distributor_id,
       store_type,
       sales_area,
       sell_out_actual,
       to_number(gross_sell_out_actual,38,6) as gross_sell_out_actual,
       net_sell_out_actual,
       sell_out_target,
       msl_actual_count,
       msl_target_count,
       planned_call_count,
       visited_call_count,
       effective_call_count,
       sales_order_count,
       on_time_count,
       in_time_count,
       otif_count,
       coverage_stores_count,
       to_number(reactivate_stores_count,38,0) as reactivate_stores_count,
       to_number(inactive_stores_count,38,0) as inactive_stores_count,
       to_number(active_stores_count,38,0) as active_stores_count,
       route_com_all,
	   effective_call_all,
       planned_store ,
       effective_store_in_plan,
       effective_store_all,
       coalesce(total_skus,0) as total_skus,
       coalesce(total_stores,0) as total_stores
FROM vw_rpt_th_gt_scorecard
