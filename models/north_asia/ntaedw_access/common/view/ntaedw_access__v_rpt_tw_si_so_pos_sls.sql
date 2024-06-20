with source as (
    select * from {{ ref('ntaedw_integration__v_rpt_tw_si_so_pos_sls') }}
),
final as (
    select
        data_set as "data_set",
        fisc_per as "fisc_per",
        fisc_yr as "fisc_yr",
        prod_hier_l1 as "prod_hier_l1",
        prod_hier_l2 as "prod_hier_l2",
        prod_hier_l3 as "prod_hier_l3",
        prod_hier_l4 as "prod_hier_l4",
        prod_hier_l5 as "prod_hier_l5",
        prod_hier_l6 as "prod_hier_l6",
        prod_hier_l7 as "prod_hier_l7",
        prod_hier_l8 as "prod_hier_l8",
        prod_hier_l9 as "prod_hier_l9",
        sap_matl_num as "sap_matl_num",
        channel as "channel",
        sls_grp as "sls_grp",
        strategy_customer_hierachy_name as "strategy_customer_hierachy_name",
        to_crncy as "to_crncy",
        ex_rt as "ex_rt",
        ean_num as "ean_num",
        sls_amt as "sls_amt",
        sls_return_amt as "sls_return_amt",
        sls_qty as "sls_qty",
        sls_amt_l3m as "sls_amt_l3m",
        sls_return_amt_13m as "sls_return_amt_13m",
        sls_qty_l3m as "sls_qty_l3m",
        sls_amt_l6m as "sls_amt_l6m",
        sls_return_amt_l6m as "sls_return_amt_l6m",
        sls_qty_l6m as "sls_qty_l6m",
        rf_amount_si as "rf_amount_si",
        rf_qty_si as "rf_qty_si",
        rf_amount_so as "rf_amount_so",
        rf_qty_so as "rf_qty_so",
        rf_amount_si_l2m as "rf_amount_si_l2m",
        rf_qty_si_l2m as "rf_qty_si_l2m",
        rf_amount_so_l2m as "rf_amount_so_l2m",
        rf_qty_so_l2m as "rf_qty_so_l2m"
    from source
)
select * from final