with wks_rpt_copa_customergp as
(
    select * from {{ ref('aspwks_integration__wks_rpt_copa_customergp') }}
),
wks_rpt_cogs_customergp as
(
    select * from {{ ref('aspwks_integration__wks_rpt_cogs_customergp') }}
),
final as
(
    SELECT 
    main.fisc_yr,
    fisc_yr_per,
    jj_qtr,
    fisc_day,
    ctry_nm,
    "cluster",
    sls_org,
    main.Prft_ctr,
    obj_crncy_co_obj,
    from_crncy,
    to_crncy,
    matl_num,
    cust_num,
    ciw_tt_tp_classification,
    acct_hier_shrt_desc,
    qty,
    amt_lcy,
    amt_usd 
    from
    (SELECT 
    *
    from  wks_rpt_copa_customergp trans
    union all
    SELECT 
    *
    from  wks_rpt_cogs_customergp  cogs
    )main	
)
select * from final