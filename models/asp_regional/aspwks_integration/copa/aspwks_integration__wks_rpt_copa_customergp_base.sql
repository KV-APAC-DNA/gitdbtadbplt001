with wks_rpt_copa_customergp_consolidate as
(
    select * from {{ ref('aspwks_integration__wks_rpt_copa_customergp_consolidate') }}
),
wks_rpt_copa_customergp_temp_sum as
(
    select * from {{ ref('aspwks_integration__wks_rpt_copa_customergp_temp_sum') }}
),
final as
(
    select custgp.fisc_yr,
            custgp.fisc_yr_per,
            custgp.ctry_nm,
            custgp."cluster",
            custgp.sls_org,
            custgp.Prft_ctr,
            custgp.obj_crncy_co_obj,
            custgp.from_crncy,
            custgp.to_crncy,
            custgp.matl_num,
            custgp.cust_num,
            case when custgp.to_crncy != 'USD' then ntstt.amt_lcy else 0 end as ntstt_lcy,
            case when custgp.to_crncy = 'USD' then ntstt.amt_usd else 0 end as ntstt_usd,
            case when custgp.to_crncy != 'USD' then ntstp.amt_lcy else 0 end as ntstp_lcy,
            case when custgp.to_crncy = 'USD' then ntstp.amt_usd else 0 end AS ntstp_usd,
            case when custgp.to_crncy != 'USD' then nts.amt_lcy else 0 end AS nts_lcy,
            case when custgp.to_crncy = 'USD' then nts.amt_usd else 0 end AS nts_usd,
            case when custgp.to_crncy != 'USD' then gts.amt_lcy else 0 end AS gts_lcy,
            case when custgp.to_crncy = 'USD' then gts.amt_usd else 0 end AS gts_usd,
            case when custgp.to_crncy != 'USD' then std_Cogs.amt_lcy else 0 end AS stdcogs_lcy,
            case when custgp.to_crncy = 'USD' then std_Cogs.amt_usd else 0 end AS stdcogs_usd,
            case when custgp.to_crncy != 'USD' then cfreegood.amt_lcy else 0 end AS cfreegood_lcy,
            case when custgp.to_crncy = 'USD' then cfreegood.amt_usd else 0 end AS cfreegood_usd,
            case when custgp.to_crncy != 'USD' then rtn.amt_lcy else 0 end AS rtn_lcy,
            case when custgp.to_crncy = 'USD' then rtn.amt_usd else 0 end AS rtn_usd,
            case when custgp.to_crncy != 'USD' then glhd.amt_lcy else 0 end AS glhd_lcy,
            case when custgp.to_crncy = 'USD' then glhd.amt_usd else 0 end AS glhd_usd
    
    from    wks_rpt_copa_customergp_consolidate custgp

    left join (select * from wks_rpt_copa_customergp_temp_sum where ciw_tt_tp_classification = 'Trade Term') ntstt 
                                    ON  custgp.fisc_yr_per = ntstt.fisc_yr_per
                AND  coalesce(nullif(custgp.matl_num,''),'0') = coalesce(nullif(ntstt.matl_num,''),'0') 
                AND  coalesce(nullif(custgp.cust_num,''),'0') = coalesce(nullif(ntstt.cust_num,''),'0')
                AND  custgp.ctry_nm = ntstt.ctry_nm 
                            AND  custgp.sls_org = ntstt.sls_org 
                            AND  custgp.Prft_ctr = ntstt.Prft_Ctr 
                            AND custgp.to_crncy = ntstt.to_crncy
                            AND custgp.acct_hier_shrt_desc = ntstt.acct_hier_shrt_desc
                            AND custgp.ciw_tt_tp_classification = ntstt.ciw_tt_tp_classification
    left join (select * from wks_rpt_copa_customergp_temp_sum where ciw_tt_tp_classification = 'Trade Promotion') ntstp 
                                    ON  custgp.fisc_yr_per = ntstp.fisc_yr_per
                AND  coalesce(nullif(custgp.matl_num,''),'0') = coalesce(nullif(ntstp.matl_num,''),'0') 
                AND  coalesce(nullif(custgp.cust_num,''),'0') = coalesce(nullif(ntstp.cust_num,''),'0')
                AND  custgp.ctry_nm = ntstp.ctry_nm 
                            AND  custgp.sls_org = ntstp.sls_org 
                            AND  custgp.Prft_ctr = ntstp.Prft_Ctr 
                            AND custgp.to_crncy = ntstp.to_crncy
                            AND custgp.acct_hier_shrt_desc = ntstp.acct_hier_shrt_desc
                            AND custgp.ciw_tt_tp_classification = ntstp.ciw_tt_tp_classification
    left join (select * from wks_rpt_copa_customergp_temp_sum where ciw_tt_tp_classification = 'NTS') nts
                                    ON  custgp.fisc_yr_per = nts.fisc_yr_per
                AND  coalesce(nullif(custgp.matl_num,''),'0') = coalesce(nullif(nts.matl_num,''),'0') 
                AND  coalesce(nullif(custgp.cust_num,''),'0') = coalesce(nullif(nts.cust_num,''),'0')
                AND  custgp.ctry_nm = nts.ctry_nm 
                            AND  custgp.sls_org = nts.sls_org 
                            AND  custgp.Prft_ctr = nts.Prft_Ctr
                            AND custgp.to_crncy = nts.to_crncy
                            AND custgp.acct_hier_shrt_desc = nts.acct_hier_shrt_desc
                            AND custgp.ciw_tt_tp_classification = nts.ciw_tt_tp_classification
    left join (select * from wks_rpt_copa_customergp_temp_sum where ciw_tt_tp_classification = 'GTS') gts
                                    ON  custgp.fisc_yr_per = gts.fisc_yr_per
                AND  coalesce(nullif(custgp.matl_num,''),'0') = coalesce(nullif(gts.matl_num,''),'0') 
                AND  coalesce(nullif(custgp.cust_num,''),'0') = coalesce(nullif(gts.cust_num,''),'0')
                AND  custgp.ctry_nm = gts.ctry_nm 
                            AND  custgp.sls_org = gts.sls_org 
                            AND  custgp.Prft_ctr = gts.Prft_Ctr 
                            AND custgp.to_crncy = gts.to_crncy
                            AND custgp.acct_hier_shrt_desc = gts.acct_hier_shrt_desc
                            AND custgp.ciw_tt_tp_classification = gts.ciw_tt_tp_classification
    left join (select * from wks_rpt_copa_customergp_temp_sum where ciw_tt_tp_classification = 'STD.COGS') std_cogs
                                    ON  custgp.fisc_yr_per = std_cogs.fisc_yr_per
                AND  coalesce(nullif(custgp.matl_num,''),'0') = coalesce(nullif(std_Cogs.matl_num,''),'0') 
                AND  coalesce(nullif(custgp.cust_num,''),'0') = coalesce(nullif(std_cogs.cust_num,''),'0')
                AND  custgp.ctry_nm = std_Cogs.ctry_nm 
                            AND  custgp.sls_org = std_Cogs.sls_org 
                            AND  custgp.Prft_ctr = std_cogs.Prft_Ctr
                            AND custgp.to_crncy = std_cogs.to_crncy
                            AND custgp.acct_hier_shrt_desc = std_cogs.acct_hier_shrt_desc
                            AND custgp.ciw_tt_tp_classification = std_cogs.ciw_tt_tp_classification                      
    left join (select * from wks_rpt_copa_customergp_temp_sum where ciw_tt_tp_classification = 'CONS.FREE.GOODS') cfreegood
                                        ON  custgp.fisc_yr_per = cfreegood.fisc_yr_per
                AND  coalesce(nullif(custgp.matl_num,''),'0') = coalesce(nullif(cfreegood.matl_num,''),'0') 
                AND  coalesce(nullif(custgp.cust_num,''),'0') = coalesce(nullif(cfreegood.cust_num,''),'0')
                AND  custgp.ctry_nm = cfreegood.ctry_nm 
                            AND  custgp.sls_org = cfreegood.sls_org 
                            AND  custgp.Prft_ctr = cfreegood.Prft_Ctr
                            AND custgp.to_crncy = cfreegood.to_crncy
                            AND custgp.acct_hier_shrt_desc = cfreegood.acct_hier_shrt_desc 
                            AND custgp.ciw_tt_tp_classification = cfreegood.ciw_tt_tp_classification                                              
    left join (select * from wks_rpt_copa_customergp_temp_sum where ciw_tt_tp_classification = 'RTN') rtn
                                        ON  custgp.fisc_yr_per = rtn.fisc_yr_per
                AND  coalesce(nullif(custgp.matl_num,''),'0') = coalesce(nullif(rtn.matl_num,''),'0') 
                AND  coalesce(nullif(custgp.cust_num,''),'0') = coalesce(nullif(rtn.cust_num,''),'0')
                AND  custgp.ctry_nm = rtn.ctry_nm 
                            AND  custgp.sls_org = rtn.sls_org 
                            AND  custgp.Prft_ctr = rtn.Prft_Ctr
                            AND custgp.to_crncy = rtn.to_crncy
                            AND custgp.acct_hier_shrt_desc = rtn.acct_hier_shrt_desc
                            AND custgp.ciw_tt_tp_classification = rtn.ciw_tt_tp_classification                                            
    left join (select * from wks_rpt_copa_customergp_temp_sum where ciw_tt_tp_classification = 'HDPM') glhd
                                    ON  custgp.fisc_yr_per = glhd.fisc_yr_per
                AND  coalesce(nullif(custgp.matl_num,''),'0') = coalesce(nullif(glhd.matl_num,''),'0') 
                AND  coalesce(nullif(custgp.cust_num,''),'0') = coalesce(nullif(glhd.cust_num,''),'0')
                AND  custgp.ctry_nm = glhd.ctry_nm 
                            AND  custgp.sls_org = glhd.sls_org 
                            AND  custgp.Prft_ctr = glhd.Prft_Ctr
                            AND custgp.to_crncy = glhd.to_crncy
                            AND custgp.acct_hier_shrt_desc = glhd.acct_hier_shrt_desc
                            AND custgp.ciw_tt_tp_classification = glhd.ciw_tt_tp_classification 
)
select * from final