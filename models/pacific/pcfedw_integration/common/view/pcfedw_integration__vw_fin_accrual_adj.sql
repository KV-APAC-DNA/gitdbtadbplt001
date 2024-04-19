with edw_fin_accrual_adj_ref as
(
    select * from snappcfedw_integration.edw_fin_accrual_adj_ref
),
edw_ciw_accnt_lkp as
(
    select * from snappcfedw_integration.edw_ciw_accnt_lkp
),
edw_account_dim as
(
    select * from snapaspedw_integration.edw_account_dim
),
final as
(
    SELECT 
        faar.time_period,
        faar.cust_id,
        faar.prod_mjr_cd,
        faar.sap_accnt,
        ead.acct_nm AS sap_accnt_desc,
        faar.amt_obj_crncy AS base_measure,
        CASE
        WHEN (trim(accnt.key_measure)::text = 'Efficiency'::text) THEN faar.amt_obj_crncy
        ELSE (0)::numeric
        END AS sales_eff_val,
        CASE
            WHEN (trim(accnt.key_measure)::text = 'Joint Growth Fund: Shopper Indirect'::text) THEN faar.amt_obj_crncy
            ELSE (0)::numeric
        END AS sales_jgf_si_val,
        CASE
            WHEN (trim(accnt.key_measure)::text = 'Payment Terms'::text) THEN faar.amt_obj_crncy
            ELSE (0)::numeric
        END AS sales_pmt_terms_val,
        CASE
            WHEN (trim(accnt.key_measure)::text = 'Data and Insights'::text) THEN faar.amt_obj_crncy
            ELSE (0)::numeric
        END AS sales_datains_val,
        CASE
            WHEN (trim(accnt.key_measure)::text = 'Expenses and Adjustments'::text) THEN faar.amt_obj_crncy
            ELSE (0)::numeric
        END AS sales_exp_adj_val,
        CASE
            WHEN (trim(accnt.key_measure)::text = 'Joint Growth Fund: Shopper Direct'::text) THEN faar.amt_obj_crncy
            ELSE (0)::numeric
        END AS sales_jgf_sd_val,
        faar.amt_obj_crncy AS sales_nts_val,
        faar.local_ccy
    FROM edw_fin_accrual_adj_ref faar,
        edw_ciw_accnt_lkp accnt,
        (
            SELECT 
                DISTINCT edw_account_dim.acct_num,
                edw_account_dim.acct_nm
            FROM edw_account_dim
            WHERE (edw_account_dim.bravo_acct_l1)::text <> ''::text
        ) ead
    WHERE (trim(faar.sap_accnt)::text = trim(accnt.sap_account)::text)
        AND (ltrim((ead.acct_num)::text, '0'::text) = trim(accnt.sap_account)::text)
)
select * from final