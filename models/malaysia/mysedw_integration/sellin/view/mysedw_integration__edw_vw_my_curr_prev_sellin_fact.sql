with edw_my_sellin_prev_dt_snpsht as(
    select * from {{ source('mysedw_integration', 'edw_my_sellin_prev_dt_snpsht') }}
),

edw_vw_my_sellin_sales_fact as(
    select * from {{ ref('mysedw_integration__edw_vw_my_sellin_sales_fact') }}
),
transformed as (
    select
  vosst.co_cd,
  vosst.cntry_nm,
  vosst.pstng_dt,
  vosst.jj_mnth_id,
  vosst.item_cd,
  vosst.cust_id,
  vosst.sls_org,
  vosst.plnt,
  vosst.dstr_chnl,
  vosst.acct_no,
  vosst.bill_typ,
  vosst.sls_ofc,
  vosst.sls_grp,
  vosst.sls_dist,
  vosst.cust_grp,
  vosst.cust_sls,
  vosst.fisc_yr,
  vosst.pstng_per,
  (
    vosst.base_val - COALESCE(mspds.base_val, CAST((
      CAST((
        0
      ) AS DECIMAL)
    ) AS DECIMAL(18, 0)))
  ) AS base_val,
  (
    vosst.sls_qty - COALESCE(mspds.sls_qty, CAST((
      CAST((
        0
      ) AS DECIMAL)
    ) AS DECIMAL(18, 0)))
  ) AS sls_qty,
  (
    vosst.ret_qty - COALESCE(mspds.ret_qty, CAST((
      CAST((
        0
      ) AS DECIMAL)
    ) AS DECIMAL(18, 0)))
  ) AS ret_qty,
  (
    vosst.sls_less_rtn_qty - COALESCE(mspds.sls_less_rtn_qty, CAST((
      CAST((
        0
      ) AS DECIMAL)
    ) AS DECIMAL(18, 0)))
  ) AS sls_less_rtn_qty,
  (
    vosst.gts_val - COALESCE(mspds.gts_val, CAST((
      CAST((
        0
      ) AS DECIMAL)
    ) AS DECIMAL(18, 0)))
  ) AS gts_val,
  (
    vosst.ret_val - COALESCE(mspds.ret_val, CAST((
      CAST((
        0
      ) AS DECIMAL)
    ) AS DECIMAL(18, 0)))
  ) AS ret_val,
  (
    vosst.gts_less_rtn_val - COALESCE(mspds.gts_less_rtn_val, CAST((
      CAST((
        0
      ) AS DECIMAL)
    ) AS DECIMAL(18, 0)))
  ) AS gts_less_rtn_val,
  (
    vosst.trdng_term_val - COALESCE(mspds.trdng_term_val, CAST((
      CAST((
        0
      ) AS DECIMAL)
    ) AS DECIMAL(18, 0)))
  ) AS trdng_term_val,
  (
    vosst.tp_val - COALESCE(mspds.tp_val, CAST((
      CAST((
        0
      ) AS DECIMAL)
    ) AS DECIMAL(18, 0)))
  ) AS tp_val,
  (
    vosst.trde_prmtn_val - COALESCE(mspds.trde_prmtn_val, CAST((
      CAST((
        0
      ) AS DECIMAL)
    ) AS DECIMAL(18, 0)))
  ) AS trde_prmtn_val,
  (
    vosst.nts_val - COALESCE(mspds.nts_val, CAST((
      CAST((
        0
      ) AS DECIMAL)
    ) AS DECIMAL(18, 0)))
  ) AS nts_val,
  (
    vosst.nts_qty - COALESCE(mspds.nts_qty, CAST((
      CAST((
        0
      ) AS DECIMAL)
    ) AS DECIMAL(18, 0)))
  ) AS nts_qty,
  'Y' AS is_curr
FROM (
  edw_vw_my_sellin_sales_fact AS vosst
    LEFT JOIN edw_my_sellin_prev_dt_snpsht AS mspds
      ON (
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
                            (
                              (
                                (
                                  (
                                    (
                                      (
                                        (
                                          (
                                            CAST((
                                              mspds.co_cd
                                            ) AS TEXT) = CAST((
                                              vosst.co_cd
                                            ) AS TEXT)
                                          )
                                          AND (
                                            CAST((
                                              mspds.cntry_nm
                                            ) AS TEXT) = CAST((
                                              vosst.cntry_nm
                                            ) AS TEXT)
                                          )
                                        )
                                        AND (
                                          CAST((
                                            mspds.pstng_dt
                                          ) AS TEXT) = CAST((
                                            vosst.pstng_dt
                                          ) AS TEXT)
                                        )
                                      )
                                      AND (
                                        CAST((
                                          mspds.jj_mnth_id
                                        ) AS TEXT) = CAST((
                                          vosst.jj_mnth_id
                                        ) AS TEXT)
                                      )
                                    )
                                    AND (
                                      CAST((
                                        mspds.item_cd
                                      ) AS TEXT) = CAST((
                                        vosst.item_cd
                                      ) AS TEXT)
                                    )
                                  )
                                  AND (
                                    CAST((
                                      mspds.cust_id
                                    ) AS TEXT) = CAST((
                                      vosst.cust_id
                                    ) AS TEXT)
                                  )
                                )
                                AND (
                                  CAST((
                                    mspds.sls_org
                                  ) AS TEXT) = CAST((
                                    vosst.sls_org
                                  ) AS TEXT)
                                )
                              )
                              AND (
                                CAST((
                                  mspds.plnt
                                ) AS TEXT) = CAST((
                                  vosst.plnt
                                ) AS TEXT)
                              )
                            )
                            AND (
                              CAST((
                                mspds.dstr_chnl
                              ) AS TEXT) = CAST((
                                vosst.dstr_chnl
                              ) AS TEXT)
                            )
                          )
                          AND (
                            CAST((
                              mspds.acct_no
                            ) AS TEXT) = CAST((
                              vosst.acct_no
                            ) AS TEXT)
                          )
                        )
                        AND (
                          CAST((
                            mspds.bill_typ
                          ) AS TEXT) = CAST((
                            vosst.bill_typ
                          ) AS TEXT)
                        )
                      )
                      AND (
                        CAST((
                          mspds.sls_ofc
                        ) AS TEXT) = CAST((
                          vosst.sls_ofc
                        ) AS TEXT)
                      )
                    )
                    AND (
                      CAST((
                        mspds.sls_grp
                      ) AS TEXT) = CAST((
                        vosst.sls_grp
                      ) AS TEXT)
                    )
                  )
                  AND (
                    CAST((
                      mspds.sls_dist
                    ) AS TEXT) = CAST((
                      vosst.sls_dist
                    ) AS TEXT)
                  )
                )
                AND (
                  CAST((
                    mspds.cust_grp
                  ) AS TEXT) = CAST((
                    vosst.cust_grp
                  ) AS TEXT)
                )
              )
              AND (
                CAST((
                  mspds.cust_sls
                ) AS TEXT) = CAST((
                  vosst.cust_sls
                ) AS TEXT)
              )
            )
            AND (
              mspds.fisc_yr = vosst.fisc_yr
            )
          )
          AND (
            mspds.pstng_per = vosst.pstng_per
          )
        )
      )
)
WHERE
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
                      (
                        (
                          (
                            (
                              vosst.base_val - COALESCE(mspds.base_val, CAST((
                                CAST((
                                  0
                                ) AS DECIMAL)
                              ) AS DECIMAL(18, 0)))
                            ) <> CAST((
                              CAST((
                                0
                              ) AS DECIMAL)
                            ) AS DECIMAL(18, 0))
                          )
                          OR (
                            (
                              vosst.sls_qty - COALESCE(mspds.sls_qty, CAST((
                                CAST((
                                  0
                                ) AS DECIMAL)
                              ) AS DECIMAL(18, 0)))
                            ) <> CAST((
                              CAST((
                                0
                              ) AS DECIMAL)
                            ) AS DECIMAL(18, 0))
                          )
                        )
                        OR (
                          (
                            vosst.ret_qty - COALESCE(mspds.ret_qty, CAST((
                              CAST((
                                0
                              ) AS DECIMAL)
                            ) AS DECIMAL(18, 0)))
                          ) <> CAST((
                            CAST((
                              0
                            ) AS DECIMAL)
                          ) AS DECIMAL(18, 0))
                        )
                      )
                      OR (
                        (
                          vosst.sls_less_rtn_qty - COALESCE(mspds.sls_less_rtn_qty, CAST((
                            CAST((
                              0
                            ) AS DECIMAL)
                          ) AS DECIMAL(18, 0)))
                        ) <> CAST((
                          CAST((
                            0
                          ) AS DECIMAL)
                        ) AS DECIMAL(18, 0))
                      )
                    )
                    OR (
                      (
                        vosst.gts_val - COALESCE(mspds.gts_val, CAST((
                          CAST((
                            0
                          ) AS DECIMAL)
                        ) AS DECIMAL(18, 0)))
                      ) <> CAST((
                        CAST((
                          0
                        ) AS DECIMAL)
                      ) AS DECIMAL(18, 0))
                    )
                  )
                  OR (
                    (
                      vosst.ret_val - COALESCE(mspds.ret_val, CAST((
                        CAST((
                          0
                        ) AS DECIMAL)
                      ) AS DECIMAL(18, 0)))
                    ) <> CAST((
                      CAST((
                        0
                      ) AS DECIMAL)
                    ) AS DECIMAL(18, 0))
                  )
                )
                OR (
                  (
                    vosst.gts_less_rtn_val - COALESCE(mspds.gts_less_rtn_val, CAST((
                      CAST((
                        0
                      ) AS DECIMAL)
                    ) AS DECIMAL(18, 0)))
                  ) <> CAST((
                    CAST((
                      0
                    ) AS DECIMAL)
                  ) AS DECIMAL(18, 0))
                )
              )
              OR (
                (
                  vosst.trdng_term_val - COALESCE(mspds.trdng_term_val, CAST((
                    CAST((
                      0
                    ) AS DECIMAL)
                  ) AS DECIMAL(18, 0)))
                ) <> CAST((
                  CAST((
                    0
                  ) AS DECIMAL)
                ) AS DECIMAL(18, 0))
              )
            )
            OR (
              (
                vosst.tp_val - COALESCE(mspds.tp_val, CAST((
                  CAST((
                    0
                  ) AS DECIMAL)
                ) AS DECIMAL(18, 0)))
              ) <> CAST((
                CAST((
                  0
                ) AS DECIMAL)
              ) AS DECIMAL(18, 0))
            )
          )
          OR (
            (
              vosst.trde_prmtn_val - COALESCE(mspds.trde_prmtn_val, CAST((
                CAST((
                  0
                ) AS DECIMAL)
              ) AS DECIMAL(18, 0)))
            ) <> CAST((
              CAST((
                0
              ) AS DECIMAL)
            ) AS DECIMAL(18, 0))
          )
        )
        OR (
          (
            vosst.nts_val - COALESCE(mspds.nts_val, CAST((
              CAST((
                0
              ) AS DECIMAL)
            ) AS DECIMAL(18, 0)))
          ) <> CAST((
            CAST((
              0
            ) AS DECIMAL)
          ) AS DECIMAL(18, 0))
        )
      )
      OR (
        (
          vosst.nts_qty - COALESCE(mspds.nts_qty, CAST((
            CAST((
              0
            ) AS DECIMAL)
          ) AS DECIMAL(18, 0)))
        ) <> CAST((
          CAST((
            0
          ) AS DECIMAL)
        ) AS DECIMAL(18, 0))
      )
    )
    AND (
      CAST((
        vosst.cntry_nm
      ) AS TEXT) = CAST((
        CAST('MY' AS VARCHAR)
      ) AS TEXT)
    )
  )
UNION ALL
select
  edw_my_sellin_prev_dt_snpsht.co_cd,
  edw_my_sellin_prev_dt_snpsht.cntry_nm,
  edw_my_sellin_prev_dt_snpsht.pstng_dt,
  edw_my_sellin_prev_dt_snpsht.jj_mnth_id,
  edw_my_sellin_prev_dt_snpsht.item_cd,
  edw_my_sellin_prev_dt_snpsht.cust_id,
  edw_my_sellin_prev_dt_snpsht.sls_org,
  edw_my_sellin_prev_dt_snpsht.plnt,
  edw_my_sellin_prev_dt_snpsht.dstr_chnl,
  edw_my_sellin_prev_dt_snpsht.acct_no,
  edw_my_sellin_prev_dt_snpsht.bill_typ,
  edw_my_sellin_prev_dt_snpsht.sls_ofc,
  edw_my_sellin_prev_dt_snpsht.sls_grp,
  edw_my_sellin_prev_dt_snpsht.sls_dist,
  edw_my_sellin_prev_dt_snpsht.cust_grp,
  edw_my_sellin_prev_dt_snpsht.cust_sls,
  edw_my_sellin_prev_dt_snpsht.fisc_yr,
  edw_my_sellin_prev_dt_snpsht.pstng_per,
  edw_my_sellin_prev_dt_snpsht.base_val,
  edw_my_sellin_prev_dt_snpsht.sls_qty,
  edw_my_sellin_prev_dt_snpsht.ret_qty,
  edw_my_sellin_prev_dt_snpsht.sls_less_rtn_qty,
  edw_my_sellin_prev_dt_snpsht.gts_val,
  edw_my_sellin_prev_dt_snpsht.ret_val,
  edw_my_sellin_prev_dt_snpsht.gts_less_rtn_val,
  edw_my_sellin_prev_dt_snpsht.trdng_term_val,
  edw_my_sellin_prev_dt_snpsht.tp_val,
  edw_my_sellin_prev_dt_snpsht.trde_prmtn_val,
  edw_my_sellin_prev_dt_snpsht.nts_val,
  edw_my_sellin_prev_dt_snpsht.nts_qty,
  'N' AS is_curr
from edw_my_sellin_prev_dt_snpsht
),

final as(
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
    nts_val,
    nts_qty,
    is_curr
    from transformed
)

select * from final