with source as (
    select * from {{ ref('ntaedw_integration__v_rpt_nts_vs_tgt') }}
),
final as (
    select
        fisc_yr_per as "fisc_yr_per",
        fisc_day as "fisc_day",
        sls_ofc as "sls_ofc",
        sls_ofc_desc as "sls_ofc_desc",
        channel as "channel",
        sls_grp as "sls_grp",
        sls_grp_desc as "sls_grp_desc",
        ctry_key as "ctry_key",
        ctry_nm as "ctry_nm",
        store_type as "store_type",
        prod_hier_lvl2 as "prod_hier_lvl2",
        prod_hier_lvl4 as "prod_hier_lvl4",
        target_type as "target_type",
        from_crncy as "from_crncy",
        to_crncy as "to_crncy",
        ex_rt as "ex_rt",
        amt_obj_crncy as "amt_obj_crncy",
        target_amt as "target_amt",
        total_working_days as "total_working_days",
        working_days_elapsed as "working_days_elapsed"
    from source
)
select * from final