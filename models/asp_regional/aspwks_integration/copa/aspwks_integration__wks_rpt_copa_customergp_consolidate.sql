with wks_rpt_customergp as
(
    select * from {{ ref('aspwks_integration__wks_rpt_customergp') }}
),
final as
(
    SELECT distinct
    fisc_yr,
    fisc_yr_per,
    ctry_nm,
    "cluster",
    sls_org,
    Prft_ctr,
    obj_crncy_co_obj,
    from_crncy,
    to_crncy,
    matl_num,
    cust_num,
    ciw_tt_tp_classification,
    acct_hier_shrt_desc
    from wks_rpt_customergp
)
select * from final