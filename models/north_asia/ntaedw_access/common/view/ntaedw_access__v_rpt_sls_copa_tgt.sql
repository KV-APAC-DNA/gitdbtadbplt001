with source as (
    select * from {{ ref('ntaedw_integration__v_rpt_sls_copa_tgt') }}
),
final as (
    select
        co_cd as "co_cd",
        cust_num as "cust_num",
        ctry_nm as "ctry_nm",
        ctry_key as "ctry_key",
        sls_grp as "sls_grp",
        rgn as "rgn",
        fisc_yr_per as "fisc_yr_per",
        from_crncy as "from_crncy",
        to_crncy as "to_crncy",
        ex_rt_typ as "ex_rt_typ",
        ex_rt as "ex_rt",
        amt_obj_crncy as "amt_obj_crncy",
        obj_crncy_co_obj as "obj_crncy_co_obj",
        sls_trgt as "sls_trgt",
        cust_sls_grp as "cust_sls_grp",
        sls_grp_desc as "sls_grp_desc",
        cust_sls_ofc as "cust_sls_ofc",
        sls_ofc_desc as "sls_ofc_desc",
        channel as "channel",
        edw_cust_nm as "edw_cust_nm",
        acct_hier_desc as "acct_hier_desc",
        acct_hier_shrt_desc as "acct_hier_shrt_desc",
        company_nm as "company_nm"
    from source
)
select * from final