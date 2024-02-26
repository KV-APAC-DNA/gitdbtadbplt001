with
edw_copa_trans_fact  as (
     select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }} 
),
itg_sg_ciw_mapping as
(
     select * from {{ ref('sgpitg_integration__itg_sg_ciw_mapping') }} 
),
transformed_1 as
(
    SELECT
        t1.co_cd,
        t1.ctry_key AS cntry_nm,
        t1.caln_day AS bill_dt,
        (
        CAST((
            t1.fisc_yr
        ) AS TEXT) || SUBSTRING(CAST((
            t1.fisc_yr_per
        ) AS TEXT), 6)
        ) AS jj_mnth_id,
        t1.matl_num AS material,
        t1.cust_num AS customer,
        t1.sls_org,
        t1.plnt,
        t1.dstr_chnl,
        LTRIM(CAST((
        t1.acct_num
        ) AS TEXT), CAST('0' AS TEXT)) AS acct_no,
        t1.bill_typ,
        t1.sls_ofc,
        t1.sls_grp,
        t1.sls_dist,
        t1.cust_grp,
        t1.cust_sls,
        t1.fisc_yr,
        t1.pstng_per,
        SUM(t1.amt_obj_crncy) AS base_val,
        SUM(
        CASE
            WHEN (
            UPPER(CAST((
                iscd.posted_where
            ) AS TEXT)) = CAST('GTS' AS TEXT)
            )
            THEN t1.qty
            ELSE CAST((
            0
            ) AS DECIMAL)
        END
        ) AS sls_qty,
        SUM(
        CASE
            WHEN (
            UPPER(CAST((
                iscd.posted_where
            ) AS TEXT)) = CAST('RETURN' AS TEXT)
            )
            THEN t1.qty
            ELSE CAST((
            0
            ) AS DECIMAL)
        END
        ) AS ret_qty,
        SUM(
        CASE
            WHEN (
            UPPER(CAST((
                iscd.posted_where
            ) AS TEXT)) = CAST('GTS' AS TEXT)
            )
            THEN t1.amt_obj_crncy
            ELSE CAST((
            0
            ) AS DECIMAL)
        END
        ) AS gts_val,
        SUM(
        CASE
            WHEN (
            UPPER(CAST((
                iscd.posted_where
            ) AS TEXT)) = CAST('RETURN' AS TEXT)
            )
            THEN t1.amt_obj_crncy
            ELSE CAST((
            0
            ) AS DECIMAL)
        END
        ) AS ret_val,
        SUM(
        CASE
            WHEN (
            UPPER(CAST((
                iscd.posted_where
            ) AS TEXT)) = CAST('TT' AS TEXT)
            )
            THEN t1.amt_obj_crncy
            ELSE CAST((
            0
            ) AS DECIMAL)
        END
        ) AS trdng_term_val,
        SUM(
        CASE
            WHEN (
            UPPER(CAST((
                iscd.posted_where
            ) AS TEXT)) = CAST('TP ON-INVOICE' AS TEXT)
            )
            THEN t1.amt_obj_crncy
            ELSE CAST((
            0
            ) AS DECIMAL)
        END
        ) AS trde_prmtn_val,
        SUM(
        CASE
            WHEN (
            UPPER(CAST((
                iscd.posted_where
            ) AS TEXT)) = CAST('TP OFF-INVOICE (ACCRUAL)' AS TEXT)
            )
            THEN t1.amt_obj_crncy
            ELSE CAST((
            0
            ) AS DECIMAL)
        END
        ) AS off_inv_trde_prmtn_val,
        SUM(
        CASE
            WHEN (
            UPPER(CAST((
                iscd.posted_where
            ) AS TEXT)) = CAST('COGS' AS TEXT)
            )
            THEN t1.amt_obj_crncy
            ELSE CAST((
            0
            ) AS DECIMAL)
        END
        ) AS cogs
    FROM (
        edw_copa_trans_fact AS t1
        LEFT JOIN itg_sg_ciw_mapping AS iscd
            ON (
            (
                (
                CAST((
                    iscd.gl
                ) AS TEXT) = LTRIM(CAST((
                    t1.acct_num
                ) AS TEXT), CAST('0' AS TEXT))
                )
            )
            )
    )
    WHERE
        (
        (
            CAST((
            t1.co_cd
            ) AS TEXT) = CAST('4481' AS TEXT)
        )
        AND (
            CAST((
            t1.acct_hier_desc
            ) AS TEXT) <> CAST('Net Trade Sales' AS TEXT)
        )
        )
    GROUP BY
        t1.co_cd,
        t1.ctry_key,
        t1.caln_day,
        t1.fisc_yr_per,
        t1.matl_num,
        t1.cust_num,
        t1.sls_org,
        t1.acct_num,
        t1.plnt,
        t1.dstr_chnl,
        t1.bill_typ,
        t1.sls_ofc,
        t1.sls_grp,
        t1.sls_dist,
        t1.cust_grp,
        t1.cust_sls,
        t1.fisc_yr,
        t1.pstng_per
),
set1 as
(
    SELECT
  t2.co_cd,
  t2.cntry_nm,
  t2.bill_dt AS pstng_dt,
  CAST((
    t2.jj_mnth_id
  ) AS VARCHAR) AS jj_mnth_id,
  t2.material AS item_cd,
  t2.customer AS cust_id,
  t2.sls_org,
  t2.plnt,
  t2.dstr_chnl,
  CAST((
    t2.acct_no
  ) AS VARCHAR) AS acct_no,
  t2.bill_typ,
  t2.sls_ofc,
  t2.sls_grp,
  t2.sls_dist,
  t2.cust_grp,
  t2.cust_sls,
  t2.fisc_yr,
  t2.pstng_per,
  t2.base_val,
  t2.sls_qty,
  t2.ret_qty,
  (
    t2.sls_qty - t2.ret_qty
  ) AS sls_less_rtn_qty,
  t2.gts_val,
  t2.ret_val,
  (
    t2.gts_val - t2.ret_val
  ) AS gts_less_rtn_val,
  t2.trdng_term_val,
  (
    t2.gts_val - (
      t2.ret_val + t2.trdng_term_val
    )
  ) AS tp_val,
  t2.trde_prmtn_val,
  t2.off_inv_trde_prmtn_val,
  (
    t2.gts_val - (
      (
        (
          t2.ret_val + t2.trdng_term_val
        ) + t2.trde_prmtn_val
      ) + t2.off_inv_trde_prmtn_val
    )
  ) AS nts_val,
  (
    t2.sls_qty - t2.ret_qty
  ) AS nts_qty,
  t2.cogs
FROM transformed_1 t2
),
transformed_2 as(
    SELECT 
        t1.co_cd,
        t1.ctry_key AS cntry_nm,
        t1.caln_day AS bill_dt,
        (
            CAST(
                (
                    t1.fisc_yr
                ) AS TEXT
            ) || SUBSTRING(
                CAST(
                    (
                        t1.fisc_yr_per
                    ) AS TEXT
                ),
                6
            )
        ) AS jj_mnth_id,
        t1.matl_num AS material,
        t1.cust_num AS customer,
        t1.sls_org,
        t1.plnt,
        t1.dstr_chnl,
        t1.bill_typ,
        t1.sls_ofc,
        t1.sls_grp,
        t1.sls_dist,
        t1.cust_grp,
        t1.cust_sls,
        t1.fisc_yr,
        t1.pstng_per,
        SUM(t1.amt_obj_crncy) AS base_val,
        SUM(
            CASE
                WHEN (
                    CAST((t1.acct_hier_desc) AS TEXT) = CAST('Gross Trade Shipment' AS TEXT)
                ) THEN t1.qty
                ELSE CAST((0) AS DECIMAL)
            END
        ) AS sls_qty,
        SUM(
            CASE
                WHEN (
                    CAST((t1.acct_hier_desc) AS TEXT) = CAST('Returns' AS TEXT)
                ) THEN t1.qty
                ELSE CAST((0) AS DECIMAL)
            END
        ) AS ret_qty,
        SUM(
            CASE
                WHEN (
                    CAST((t1.acct_hier_desc) AS TEXT) = CAST('Gross Trade Shipment' AS TEXT)
                ) THEN t1.amt_obj_crncy
                ELSE CAST(
                    (0) AS DECIMAL
                )
            END
        ) AS gts_val,
        SUM(
            CASE
                WHEN (
                    CAST((t1.acct_hier_desc) AS TEXT) = CAST('Returns' AS TEXT)
                ) THEN t1.amt_obj_crncy
                ELSE CAST((0) AS DECIMAL)
            END
        ) AS ret_val,
        SUM(
            CASE
                WHEN (
                    CAST((t1.acct_hier_desc) AS TEXT) = CAST('Sales Allowances' AS TEXT)
                ) THEN t1.amt_obj_crncy
                ELSE CAST((0) AS DECIMAL)
            END
        ) AS sls_alwnce,
        SUM(
            CASE
                WHEN (
                    CAST((t1.acct_hier_desc) AS TEXT) = CAST('Cash Discount' AS TEXT)
                ) THEN t1.amt_obj_crncy
                ELSE CAST((0) AS DECIMAL)
            END
        ) AS cash_dscnt,
        SUM(
            CASE
                WHEN (
                    CAST((t1.acct_hier_desc) AS TEXT) = CAST('Hidden Dsct Markdown Allowance' AS TEXT)
                ) THEN t1.amt_obj_crncy
                ELSE CAST((0) AS DECIMAL)
            END
        ) AS hdn_dscnt_mrkdwn_alwnce,
        SUM(
            CASE
                WHEN (
                    CAST((t1.acct_hier_desc) AS TEXT) = CAST('Hidden Dsct Profit Margin' AS TEXT)
                ) THEN t1.amt_obj_crncy
                ELSE CAST((0) AS DECIMAL)
            END
        ) AS hdn_dscnt_prft_mrgn,
        SUM(
            CASE
                WHEN (
                    CAST((t1.acct_hier_desc) AS TEXT) = CAST('Hidden Dsct Promotion' AS TEXT)
                ) THEN t1.amt_obj_crncy
                ELSE CAST((0) AS DECIMAL)
            END
        ) AS hdn_dscnt_prmtn,
        SUM(
            CASE
                WHEN (
                    CAST(
                        (
                            t1.acct_hier_desc
                        ) AS TEXT
                    ) = CAST('Logistics Fees' AS TEXT)
                ) THEN t1.amt_obj_crncy
                ELSE CAST(
                    (0) AS DECIMAL
                )
            END
        ) AS log_fees,
        SUM(
            CASE
                WHEN (
                    CAST(
                        (
                            t1.acct_hier_desc
                        ) AS TEXT
                    ) = CAST('Non-Government Chargebacks' AS TEXT)
                ) THEN t1.amt_obj_crncy
                ELSE CAST(
                    (0) AS DECIMAL
                )
            END
        ) AS ngov_chrgbck,
        SUM(
            CASE
                WHEN (
                    CAST(
                        (
                            t1.acct_hier_desc
                        ) AS TEXT
                    ) = CAST('Pricing/Space Others' AS TEXT)
                ) THEN t1.amt_obj_crncy
                ELSE CAST(
                    (0) AS DECIMAL
                )
            END
        ) AS prc_sp_oth,
        SUM(
            CASE
                WHEN (
                    CAST(
                        (
                            t1.acct_hier_desc
                        ) AS TEXT
                    ) = CAST('Profit Margin Allowance' AS TEXT)
                ) THEN t1.amt_obj_crncy
                ELSE CAST(
                    (0) AS DECIMAL
                )
            END
        ) AS prft_mrgn_alwnce,
        SUM(
            CASE
                WHEN (
                    CAST(
                        (
                            t1.acct_hier_desc
                        ) AS TEXT
                    ) = CAST('Term & Logistic Others' AS TEXT)
                ) THEN t1.amt_obj_crncy
                ELSE CAST(
                    (0) AS DECIMAL
                )
            END
        ) AS trm_log_oth,
        SUM(
            CASE
                WHEN (
                    CAST(
                        (
                            t1.acct_hier_desc
                        ) AS TEXT
                    ) = CAST('Volume Growth Funds' AS TEXT)
                ) THEN t1.amt_obj_crncy
                ELSE CAST(
                    (0) AS DECIMAL
                )
            END
        ) AS vol_grwth_fnds,
        SUM(
            CASE
                WHEN (
                    CAST(
                        (
                            t1.acct_hier_desc
                        ) AS TEXT
                    ) = CAST('Net Trade Sales' AS TEXT)
                ) THEN t1.qty
                ELSE CAST(
                    (0) AS DECIMAL
                )
            END
        ) AS nts_qty,
        SUM(
            CASE
                WHEN (
                    CAST(
                        (
                            t1.acct_hier_desc
                        ) AS TEXT
                    ) = CAST('Net Trade Sales' AS TEXT)
                ) THEN t1.amt_obj_crncy
                ELSE CAST(
                    (0) AS DECIMAL
                )
            END
        ) AS nts_val
    FROM edw_copa_trans_fact AS t1
    WHERE (
            CAST(
                (t1.sls_org) AS TEXT
            ) = CAST('2300' AS TEXT)
        )
        AND t1.ctry_key::text = 'SG'
    GROUP BY t1.co_cd,
        t1.ctry_key,
        t1.caln_day,
        t1.fisc_yr_per,
        t1.matl_num,
        t1.cust_num,
        t1.sls_org,
        t1.plnt,
        t1.dstr_chnl,
        t1.bill_typ,
        t1.sls_ofc,
        t1.sls_grp,
        t1.sls_dist,
        t1.cust_grp,
        t1.cust_sls,
        t1.fisc_yr,
        t1.pstng_per
                
),
set2 as
(
   SELECT 
        t2.co_cd,
        t2.cntry_nm,
        t2.bill_dt AS pstng_dt,
        CAST(
            (t2.jj_mnth_id) AS VARCHAR
        ) AS jj_mnth_id,
        t2.material AS item_cd,
        t2.customer AS cust_id,
        t2.sls_org,
        t2.plnt,
        t2.dstr_chnl,
        CAST(
            (
                CAST(NULL AS VARCHAR)
            ) AS VARCHAR(10)
        ) AS acct_no,
        t2.bill_typ,
        t2.sls_ofc,
        t2.sls_grp,
        t2.sls_dist,
        t2.cust_grp,
        t2.cust_sls,
        t2.fisc_yr,
        t2.pstng_per,
        t2.base_val,
        t2.sls_qty,
        t2.ret_qty,
        (
            t2.sls_qty - t2.ret_qty
        ) AS sls_less_rtn_qty,
        t2.gts_val,
        t2.ret_val,
        (
            t2.gts_val - t2.ret_val
        ) AS gts_less_rtn_val,
        CAST(
            (
                CAST(NULL AS DECIMAL)
            ) AS DECIMAL(38, 5)
        ) AS trdng_term_val,
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
                                                    (
                                                        t2.sls_alwnce + t2.cash_dscnt
                                                    ) + t2.hdn_dscnt_mrkdwn_alwnce
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
        CAST(
            (
                CAST(NULL AS DECIMAL)
            ) AS DECIMAL(38, 5)
        ) AS trde_prmtn_val,
        CAST(
            (
                CAST(NULL AS DECIMAL)
            ) AS DECIMAL(38, 5)
        ) AS off_inv_trde_prmtn_val,
        t2.nts_val,
        t2.nts_qty,
        CAST(
            (
                CAST(NULL AS DECIMAL)
            ) AS DECIMAL(38, 5)
        ) AS cogs
    FROM transformed_2 t2
),
final as
(
    select * from set1
    union all
    select * from set2
)
select * from final