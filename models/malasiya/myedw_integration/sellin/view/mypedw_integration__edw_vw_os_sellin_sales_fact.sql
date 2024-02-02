with edw_copa_trans_fact as(
    select * from DEV_DNA_CORE.ASPEDW_INTEGRATION.EDW_COPA_TRANS_FACT
),
itg_my_ciw_map as(
    select * from DEV_DNA_CORE.MYSITG_INTEGRATION.ITG_MY_CIW_MAP
),

transformed as(
    select
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
    t2.tp_val,
    t2.trde_prmtn_val,
    CAST((
      CAST(NULL AS DECIMAL)
    ) AS DECIMAL(38, 5)) AS off_inv_trde_prmtn_val,
    (
      t2.gts_val - (
        (
          t2.ret_val + t2.trdng_term_val
        ) + t2.trde_prmtn_val
      )
    ) AS nts_val,
    (
      t2.sls_qty - t2.ret_qty
    ) AS nts_qty,
    CAST((
      CAST(NULL AS DECIMAL)
    ) AS DECIMAL(38, 5)) AS cogs
  FROM (
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
            (
              UPPER(CAST((
                imcd.acct_type
              ) AS TEXT)) = CAST('TRADE PRICE' AS TEXT)
            )
            AND (
              CAST((
                t1.acct_hier_desc
              ) AS TEXT) <> CAST('Net Trade Sales' AS TEXT)
            )
          )
          THEN CASE
            WHEN (
              CAST((
                t1.acct_hier_desc
              ) AS TEXT) <> CAST('Gross Trade Shipment' AS TEXT)
            )
            THEN (
              t1.amt_obj_crncy * CAST((
                -1
              ) AS DECIMAL)
            )
            ELSE t1.amt_obj_crncy
          END
          ELSE CAST((
            0
          ) AS DECIMAL)
        END
      ) AS tp_val,
      SUM(
        CASE
          WHEN (
            (
              UPPER(CAST((
                imcd.acct_type1
              ) AS TEXT)) = CAST('GTS' AS TEXT)
            )
            AND (
              CAST((
                t1.acct_hier_desc
              ) AS TEXT) = CAST('Gross Trade Shipment' AS TEXT)
            )
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
            (
              UPPER(CAST((
                imcd.acct_type1
              ) AS TEXT)) = CAST('RETURNS' AS TEXT)
            )
            AND (
              CAST((
                t1.acct_hier_desc
              ) AS TEXT) = CAST('Returns' AS TEXT)
            )
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
            (
              UPPER(CAST((
                imcd.acct_type1
              ) AS TEXT)) = CAST('GTS' AS TEXT)
            )
            AND (
              CAST((
                t1.acct_hier_desc
              ) AS TEXT) = CAST('Gross Trade Shipment' AS TEXT)
            )
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
            (
              UPPER(CAST((
                imcd.acct_type1
              ) AS TEXT)) = CAST('RETURNS' AS TEXT)
            )
            AND (
              CAST((
                t1.acct_hier_desc
              ) AS TEXT) = CAST('Returns' AS TEXT)
            )
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
            (
              UPPER(CAST((
                imcd.acct_type1
              ) AS TEXT)) = CAST('TRADING TERM' AS TEXT)
            )
            AND (
              CAST((
                t1.acct_hier_desc
              ) AS TEXT) <> CAST('Net Trade Sales' AS TEXT)
            )
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
            (
              UPPER(CAST((
                imcd.acct_type1
              ) AS TEXT)) = CAST('TRADE PROMOTION' AS TEXT)
            )
            AND (
              CAST((
                t1.acct_hier_desc
              ) AS TEXT) <> CAST('Net Trade Sales' AS TEXT)
            )
          )
          THEN t1.amt_obj_crncy
          ELSE CAST((
            0
          ) AS DECIMAL)
        END
      ) AS trde_prmtn_val
    FROM (
      edw_copa_trans_fact AS t1
        LEFT JOIN itg_my_ciw_map AS imcd
          ON (
            (
              (
                CAST((
                  imcd.acct_num
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
          ) AS TEXT) = CAST('4130' AS TEXT)
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
  ) AS t2


)

select * from transformed