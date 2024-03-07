with edw_copa_trans_fact as 
(
    select * from DEV_DNA_CORE.snapaspedw_integration.edw_copa_trans_fact
),
t2 as
(
    SELECT 
        t1.co_cd,
        t1.ctry_key AS cntry_nm,
        t1.caln_day AS bill_dt,
        (
            (t1.fisc_yr)::text || "substring"((t1.fisc_yr_per)::text, 6)
        ) AS jj_mnth_id,
        CASE
            WHEN (ltrim((t1.matl_num)::text, '0'::text) = ''::text) THEN NULL::text
            ELSE ltrim((t1.matl_num)::text, '0'::text)
        END AS material,
        ltrim((t1.cust_num)::text, '0'::text) AS customer,
        t1.sls_org,
        t1.plnt,
        t1.dstr_chnl,
        ltrim((t1.acct_num)::text, '0'::text) AS acct_no,
        t1.bill_typ,
        t1.sls_ofc,
        t1.sls_grp,
        t1.sls_dist,
        t1.cust_grp,
        ltrim((t1.cust_sls)::text, '0'::text) AS cust_sls,
        t1.fisc_yr,
        t1.pstng_per,
        sum(t1.amt_obj_crncy) AS base_val,
        sum(
            CASE
                WHEN ((t1.acct_hier_desc)::text = 'Gross Trade Shipment'::text) THEN t1.qty
                ELSE (0)::numeric
            END
        ) AS sls_qty,
        sum(
            CASE
                WHEN ((t1.acct_hier_desc)::text = 'Returns'::text) THEN t1.qty 
                ELSE (0)::numeric
            END
        ) AS ret_qty,
        sum(
            CASE
                WHEN ((t1.acct_hier_desc)::text = 'Gross Trade Shipment'::text) THEN t1.amt_obj_crncy
                ELSE (0)::numeric
            END
        ) AS gts_val,
        sum(
            CASE
                WHEN ((t1.acct_hier_desc)::text = 'Returns'::text) THEN t1.amt_obj_crncy
                ELSE (0)::numeric
            END
        ) AS ret_val,
        sum(
            CASE
                WHEN ((t1.acct_hier_desc)::text = 'Sales Allowances'::text) THEN t1.amt_obj_crncy
                ELSE (0)::numeric
            END
        ) AS sls_alwnce,
        sum(
            CASE
                WHEN ((t1.acct_hier_desc)::text = 'Cash Discount'::text) THEN t1.amt_obj_crncy
                ELSE (0)::numeric
            END
        ) AS cash_dscnt,
        sum(
            CASE
                WHEN ((t1.acct_hier_desc)::text = 'Hidden Dsct Markdown Allowance'::text) THEN t1.amt_obj_crncy
                ELSE (0)::numeric
            END
        ) AS hdn_dscnt_mrkdwn_alwnce,
        sum(
            CASE
                WHEN ((t1.acct_hier_desc)::text = 'Hidden Dsct Profit Margin'::text) THEN t1.amt_obj_crncy
                ELSE (0)::numeric
            END
        ) AS hdn_dscnt_prft_mrgn,
        sum(
            CASE
                WHEN ((t1.acct_hier_desc)::text = 'Hidden Dsct Promotion'::text) THEN t1.amt_obj_crncy
                ELSE (0)::numeric
            END
        ) AS hdn_dscnt_prmtn,
        sum(
            CASE
                WHEN ((t1.acct_hier_desc)::text = 'Logistics Fees'::text) THEN t1.amt_obj_crncy
                ELSE (0)::numeric
            END
        ) AS log_fees,
        sum(
            CASE
                WHEN ((t1.acct_hier_desc)::text = 'Non-Government Chargebacks'::text) THEN t1.amt_obj_crncy
                ELSE (0)::numeric
            END
        ) AS ngov_chrgbck,
        sum(
            CASE
                WHEN ((t1.acct_hier_desc)::text = 'Pricing/Space Others'::text) THEN t1.amt_obj_crncy
                ELSE (0)::numeric
            END
        ) AS prc_sp_oth,
        sum(
            CASE
                WHEN ((t1.acct_hier_desc)::text = 'Profit Margin Allowance'::text) THEN t1.amt_obj_crncy
                ELSE (0)::numeric
            END
        ) AS prft_mrgn_alwnce,
        sum(
            CASE
                WHEN ((t1.acct_hier_desc)::text = 'Term & Logistic Others'::text) THEN t1.amt_obj_crncy
                ELSE (0)::numeric
            END
        ) AS trm_log_oth,
        sum(
            CASE
                WHEN ((t1.acct_hier_desc)::text = 'Volume Growth Funds'::text) THEN t1.amt_obj_crncy
                ELSE (0)::numeric
            END
        ) AS vol_grwth_fnds,
        sum(
            CASE
                WHEN ((t1.acct_hier_desc)::text = 'Net Trade Sales'::text) THEN t1.qty
                ELSE (0)::numeric
            END
        ) AS nts_qty,
        sum(
            CASE
                WHEN ((t1.acct_hier_desc)::text = 'Net Trade Sales'::text) THEN t1.amt_obj_crncy
                ELSE (0)::numeric
            END
        ) AS nts_val
    FROM edw_copa_trans_fact t1
    WHERE (
            ((t1.sls_org)::text = '2400'::text)
            OR ((t1.sls_org)::text = '2500'::text)
        )
    GROUP BY 
    t1.co_cd,
        t1.ctry_key,
        t1.caln_day,
        t1.fisc_yr_per,
        t1.matl_num,
        t1.cust_num,
        t1.sls_org,
        t1.plnt,
        t1.dstr_chnl,
        t1.acct_num,
        t1.bill_typ,
        t1.sls_ofc,
        t1.sls_grp,
        t1.sls_dist,
        t1.cust_grp,
        t1.cust_sls,
        t1.fisc_yr,
        t1.pstng_per
),
transformed as
(
    SELECT 
        t2.co_cd,
        t2.cntry_nm,
        t2.bill_dt AS pstng_dt,
        (t2.jj_mnth_id)::character varying AS jj_mnth_id,
        (t2.material)::character varying AS item_cd,
        (t2.customer)::character varying AS cust_id,
        t2.sls_org,
        t2.plnt,
        t2.dstr_chnl,
        (t2.acct_no)::character varying AS acct_no,
        t2.bill_typ,
        t2.sls_ofc,
        t2.sls_grp,
        t2.sls_dist,
        t2.cust_grp,
        (t2.cust_sls)::character varying AS cust_sls,
        t2.fisc_yr,
        t2.pstng_per,
        t2.base_val,
        t2.sls_qty,
        t2.ret_qty,
        (t2.sls_qty - t2.ret_qty) AS sls_less_rtn_qty,
        t2.gts_val,
        t2.ret_val,
        (t2.gts_val - t2.ret_val) AS gts_less_rtn_val,
        NULL::numeric(38, 5) AS trdng_term_val,
        (
            t2.gts_val - (
                (
                    (
                        (
                            (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    (t2.sls_alwnce + t2.cash_dscnt) + t2.hdn_dscnt_mrkdwn_alwnce
                                                ) + t2.hdn_dscnt_prft_mrgn
                                            ) + t2.hdn_dscnt_prmtn
                                        ) + t2.log_fees
                                    ) + t2.ngov_chrgbck
                                ) + t2.prc_sp_oth
                            ) + t2.prft_mrgn_alwnce
                        ) + t2.trm_log_oth
                    ) + t2.vol_grwth_fnds
                ) + t2.ret_val
            )
        ) AS tp_val,
        NULL::numeric(38, 5) AS trde_prmtn_val,
        NULL::numeric(38, 5) AS off_inv_trde_prmtn_val,
        t2.nts_val,
        t2.nts_qty,
        NULL::numeric(38, 5) AS cogs
    FROM t2

),
final as
(
    select
        co_cd,
        cntry_nm,
        pstng_dt,
        jj_mnth_id,
        item_cd,
        cust_id,
        sls_org,
        plnt,
        dstr_chnl,
        acct_no,
        bill_typ,
        sls_ofc,
        sls_grp,
        sls_dist,
        cust_grp,
        cust_sls,
        fisc_yr,
        pstng_per,
        base_val,
        sls_qty,
        ret_qty,
        sls_less_rtn_qty,
        gts_val,
        ret_val,
        gts_less_rtn_val,
        trdng_term_val,
        tp_val,
        trde_prmtn_val,
        off_inv_trde_prmtn_val,
        nts_val,
        nts_qty,
        cogs
    from transformed
)
select * from final
   
    
