with source as (
    select * from {{ ref('thaedw_integration__edw_rpt_th_gt_scorecard') }}
),
final as (
    select 
        on_time_count as "on_time_count",
        cntry_cd as "cntry_cd",
        msl_target_count as "msl_target_count",
        net_sell_out_actual as "net_sell_out_actual",
        sales_order_count as "sales_order_count",
        sales_area as "sales_area",
        identifier as "identifier",
        store_type as "store_type",
        effective_call_count as "effective_call_count",
        planned_store as "planned_store",
        planned_call_count as "planned_call_count",
        year as "year",
        total_skus as "total_skus",
        sell_out_target as "sell_out_target",
        visited_call_count as "visited_call_count",
        year_month as "year_month",
        active_stores_count as "active_stores_count",
        effective_store_all as "effective_store_all",
        month as "month",
        reactivate_stores_count as "reactivate_stores_count",
        total_stores as "total_stores",
        effective_call_all as "effective_call_all",
        otif_count as "otif_count",
        msl_actual_count as "msl_actual_count",
        route_com_all as "route_com_all",
        gross_sell_out_actual as "gross_sell_out_actual",
        in_time_count as "in_time_count",
        sell_out_actual as "sell_out_actual",
        crncy_cd as "crncy_cd",
        distributor_id as "distributor_id",
        inactive_stores_count as "inactive_stores_count",
        coverage_stores_count as "coverage_stores_count",
        effective_store_in_plan as "effective_store_in_plan",
        cntry_nm as "cntry_nm"
    from source
)
select * from final