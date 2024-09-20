with wks_rpt_customergp as
(
    select * from {{ ref('aspwks_integration__wks_rpt_customergp') }}
),
final as
(
    SELECT customergp.fisc_yr,customergp.fisc_yr_per,prft_ctr,customergp.matl_num ,customergp.cust_num , customergp.ctry_nm ,
    customergp.sls_org, acct_hier_shrt_desc,
    ciw_tt_tp_classification,to_crncy,
    sum(customergp.amt_lcy) AS amt_lcy,sum(customergp.amt_usd) AS amt_usd, sum (qty) as qty
    FROM 
    wks_rpt_customergp AS customergp
    group by customergp.fisc_yr,customergp.fisc_yr_per,prft_ctr,customergp.matl_num ,customergp.cust_num , customergp.ctry_nm ,
    customergp.sls_org,
    acct_hier_shrt_desc,ciw_tt_tp_classification,to_crncy
)
select * from final