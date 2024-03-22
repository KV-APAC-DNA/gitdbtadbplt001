with source as (
    select * from {{ ref('thaedw_integration__edw_rpt_lcm_gt_scorecard') }}
),
final as (
    select 
        otif_count as "otif_count",
        distributor_id as "distributor_id",
        msl_target_count as "msl_target_count",
        month as "month",
        active_store_count as "active_store_count",
        net_sell_out_actual as "net_sell_out_actual",
        msl_actual_count as "msl_actual_count",
        effective_call_count as "effective_call_count",
        cntry_cd as "cntry_cd",
        sales_area as "sales_area",
        year_month as "year_month",
        sales_order_count as "sales_order_count",
        on_time_count as "on_time_count",
        gross_sell_out_actual as "gross_sell_out_actual",
        visited_call_count as "visited_call_count",
        exch_rate as "exch_rate",
        sellout_target as "sellout_target",
        year as "year",
        inactive_stores_count as "inactive_stores_count",
        identifier as "identifier",
        crncy_cd as "crncy_cd",
        in_full_count as "in_full_count",
        coverage_stores_count as "coverage_stores_count",
        store_type as "store_type",
        reactivate_stores_count as "reactivate_stores_count",
        cntry_nm as "cntry_nm",
        planned_call_count as "planned_call_count",
        sell_out_actual as "sell_out_actual"
    from source
)
select * from final