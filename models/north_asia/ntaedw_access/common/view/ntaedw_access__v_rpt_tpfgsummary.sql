with source as (
    select * from {{ ref('ntaedw_integration__v_rpt_tpfgsummary') }}
),
final as (
    select
        identifier as "identifier",
        cntry_key as "cntry_key",
        cust_num as "cust_num",
        channel as "channel",
        prft_ctr as "prft_ctr",
        matl as "matl",
        matl_desc as "matl_desc",
        mega_brnd_desc as "mega_brnd_desc",
        brnd_desc as "brnd_desc",
        franchise as "franchise",
        prod_minor as "prod_minor",
        sls_grp as "sls_grp",
        sls_grp_desc as "sls_grp_desc",
        sls_ofc as "sls_ofc",
        sls_ofc_desc as "sls_ofc_desc",
        category_1 as "category_1",
        categroy_2 as "categroy_2",
        platform_ca as "platform_ca",
        amt_obj_crncy as "amt_obj_crncy",
        obj_crncy_co_obj as "obj_crncy_co_obj",
        edw_cust_nm as "edw_cust_nm",
        from_crncy as "from_crncy",
        to_crncy as "to_crncy",
        ex_rt_typ as "ex_rt_typ",
        ex_rt as "ex_rt",
        brand_group as "brand_group",
        fisc_day as "fisc_day",
        fisc_yr_per as "fisc_yr_per",
        qty as "qty",
        units_sold as "units_sold",
        uom as "uom",
        acct_hier_desc as "acct_hier_desc",
        acct_hier_shrt_desc as "acct_hier_shrt_desc",
        cntry_cd as "cntry_cd",
        company_nm as "company_nm",
        fert_flag as "fert_flag",
        store_type as "store_type",
        free_good_value as "free_good_value",
        pre_apsc_cogs as "pre_apsc_cogs",
        package_cost as "package_cost",
        labour_cost as "labour_cost",
        valuation_class as "valuation_class",
        target_type as "target_type",
        target_value as "target_value",
        target_amount as "target_amount"
    from source
)
select * from final