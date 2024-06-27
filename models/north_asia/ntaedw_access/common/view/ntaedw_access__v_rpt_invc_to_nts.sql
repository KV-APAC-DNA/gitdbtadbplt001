with source as (
    select * from {{ ref('ntaedw_integration__v_rpt_invc_to_nts') }}
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
        mega_brnd_desc as "mega_brnd_desc",
        matl_num as "matl_num",
        brnd_desc as "brnd_desc",
        category as "category",
        matl_desc as "matl_desc",
        cust_num as "cust_num",
        ctry_key as "ctry_key",
        ctry_nm as "ctry_nm",
        store_type as "store_type",
        prod_hier_lvl1 as "prod_hier_lvl1",
        prod_hier_lvl2 as "prod_hier_lvl2",
        prod_hier_lvl3 as "prod_hier_lvl3",
        prod_hier_lvl4 as "prod_hier_lvl4",
        prod_hier_lvl5 as "prod_hier_lvl5",
        prod_hier_lvl6 as "prod_hier_lvl6",
        prod_hier_lvl7 as "prod_hier_lvl7",
        prod_hier_lvl8 as "prod_hier_lvl8",
        prod_hier_lvl9 as "prod_hier_lvl9",
        ean_num as "ean_num",
        ciw_desc as "ciw_desc",
        ciw_code as "ciw_code",
        ciw_bucket as "ciw_bucket",
        acct_num as "acct_num",
        acct_nm as "acct_nm",
        from_crncy as "from_crncy",
        to_crncy as "to_crncy",
        amt_obj_crncy as "amt_obj_crncy",
        ex_rt as "ex_rt",
        edw_cust_nm as "edw_cust_nm"
    from source
)
select * from final