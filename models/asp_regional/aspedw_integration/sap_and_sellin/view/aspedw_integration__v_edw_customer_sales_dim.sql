--Import CTE
with edw_customer_sales_dim as (
    select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
),

edw_code_descriptions as (
   select * from {{ ref('aspedw_integration__edw_code_descriptions') }}
),
edw_subchnl_retail_env_mapping as 
(
   select * from {{ source('aspedw_integration','edw_subchnl_retail_env_mapping')}}
),
edw_code_descriptions_manual as 
(
   select * from {{ source('aspedw_integration','edw_code_descriptions_manual')}}
),
--Logical CTE

-- Final CTE
final as (
    (
  select
    cus_sales.clnt,
    cus_sales.cust_num,
    cus_sales.sls_org,
    cus_sales.dstr_chnl,
    cus_sales.div,
    cus_sales.obj_crt_prsn,
    cus_sales.rec_crt_dt,
    cus_sales.auth_grp,
    cus_sales.cust_del_flag,
    cus_sales.cust_stat_grp,
    cus_sales.cust_ord_blk,
    cus_sales.prc_pcdr_asgn,
    cus_sales.cust_grp,
    cus_sales.sls_dstrc,
    cus_sales.prc_grp,
    cus_sales.prc_list_typ,
    cus_sales.ord_prob_itm,
    cus_sales.incoterm1,
    cus_sales.incoterm2,
    cus_sales.cust_delv_blk,
    cus_sales.cmplt_delv_sls_ord,
    cus_sales.max_no_prtl_delv_allw_itm,
    cus_sales.prtl_delv_itm_lvl,
    cus_sales.ord_comb_in,
    cus_sales.btch_splt_allw,
    cus_sales.delv_prir,
    cus_sales.vend_acct_num,
    cus_sales.shipping_cond,
    cus_sales.bill_blk_cust,
    cus_sales.man_invc_maint,
    cus_sales.invc_dt,
    cus_sales.invc_list_sched,
    cus_sales.cost_est_in,
    cus_sales.val_lmt_cost_est,
    cus_sales.crncy_key,
    cus_sales.cust_clas,
    cus_sales.acct_asgnmt_grp,
    cus_sales.delv_plnt,
    cus_sales.sls_grp,
    cus_sales.sls_grp_desc,
    cus_sales.sls_ofc,
    cus_sales.sls_ofc_desc,
    cus_sales.itm_props,
    cus_sales.cust_grp1,
    cus_sales.cust_grp2,
    cus_sales.cust_grp3,
    cus_sales.cust_grp4,
    cus_sales.cust_grp5,
    cus_sales.cust_rebt_in,
    cus_sales.rebt_indx_cust_strt_prd,
    cus_sales.exch_rt_typ,
    cus_sales.prc_dtrmn_id,
    cus_sales.prod_attr_id1,
    cus_sales.prod_attr_id2,
    cus_sales.prod_attr_id3,
    cus_sales.prod_attr_id4,
    cus_sales.prod_attr_id5,
    cus_sales.prod_attr_id6,
    cus_sales.prod_attr_id7,
    cus_sales.prod_attr_id8,
    cus_sales.prod_attr_id9,
    cus_sales.prod_attr_id10,
    cus_sales.pymt_key_term,
    cus_sales.persnl_num,
    cus_sales.crt_dttm,
    cus_sales.updt_dttm,
    cus_sales.cur_sls_emp,
    cus_sales.lcl_cust_grp_1,
    cus_sales.lcl_cust_grp_2,
    cus_sales.lcl_cust_grp_3,
    cus_sales.lcl_cust_grp_4,
    cus_sales.lcl_cust_grp_5,
    cus_sales.lcl_cust_grp_6,
    cus_sales.lcl_cust_grp_7,
    cus_sales.lcl_cust_grp_8,
    cus_sales.prc_proc,
    cus_sales.par_del,
    cus_sales.max_num_pa,
    cus_sales.prnt_cust_key,
    cus_sales.bnr_key,
    cus_sales.bnr_frmt_key,
    cus_sales.go_to_mdl_key,
    cus_sales.chnl_key,
    cus_sales.sub_chnl_key,
    cus_sales.segmt_key,
    cus_sales.cust_set_1,
    cus_sales.cust_set_2,
    cus_sales.cust_set_3,
    cus_sales.cust_set_4,
    cus_sales.cust_set_5,
    cddes_pck.code_desc AS "parent customer",
    cddes_bnrkey.code_desc AS banner,
    cddes_bnrfmt.code_desc AS "banner format",
    cddes_chnl.code_desc AS channel,
    cddes_gtm.code_desc AS "go to model",
    cddes_subchnl.code_desc AS "sub channel",
    subchnl_retail_env.retail_env,
    ecdm.code_desc
  FROM (
    (
      (
        (
          (
            (
              (
                (
                  edw_customer_sales_dim AS cus_sales
                    LEFT JOIN edw_code_descriptions AS cddes_pck
                      ON (
                        (
                          (
                            CAST((
                              cddes_pck.code_type
                            ) AS TEXT) = CAST((
                              CAST('Parent Customer Key' AS VARCHAR)
                            ) AS TEXT)
                          )
                          AND (
                            CAST((
                              cddes_pck.code
                            ) AS TEXT) = CAST((
                              cus_sales.prnt_cust_key
                            ) AS TEXT)
                          )
                        )
                      )
                )
                LEFT JOIN edw_code_descriptions AS cddes_bnrkey
                  ON (
                    (
                      (
                        CAST((
                          cddes_bnrkey.code_type
                        ) AS TEXT) = CAST((
                          CAST('Banner Key' AS VARCHAR)
                        ) AS TEXT)
                      )
                      AND (
                        CAST((
                          cddes_bnrkey.code
                        ) AS TEXT) = CAST((
                          cus_sales.bnr_key
                        ) AS TEXT)
                      )
                    )
                  )
              )
              LEFT JOIN edw_code_descriptions AS cddes_bnrfmt
                ON (
                  (
                    (
                      CAST((
                        cddes_bnrfmt.code_type
                      ) AS TEXT) = CAST((
                        CAST('Banner Format Key' AS VARCHAR)
                      ) AS TEXT)
                    )
                    AND (
                      CAST((
                        cddes_bnrfmt.code
                      ) AS TEXT) = CAST((
                        cus_sales.bnr_frmt_key
                      ) AS TEXT)
                    )
                  )
                )
            )
            LEFT JOIN edw_code_descriptions AS cddes_chnl
              ON (
                (
                  (
                    CAST((
                      cddes_chnl.code_type
                    ) AS TEXT) = CAST((
                      CAST('Channel Key' AS VARCHAR)
                    ) AS TEXT)
                  )
                  AND (
                    CAST((
                      cddes_chnl.code
                    ) AS TEXT) = CAST((
                      cus_sales.chnl_key
                    ) AS TEXT)
                  )
                )
              )
          )
          LEFT JOIN edw_code_descriptions AS cddes_gtm
            ON (
              (
                (
                  CAST((
                    cddes_gtm.code_type
                  ) AS TEXT) = CAST((
                    CAST('Go To Model Key' AS VARCHAR)
                  ) AS TEXT)
                )
                AND (
                  CAST((
                    cddes_gtm.code
                  ) AS TEXT) = CAST((
                    cus_sales.go_to_mdl_key
                  ) AS TEXT)
                )
              )
            )
        )
        LEFT JOIN edw_code_descriptions AS cddes_subchnl
          ON (
            (
              (
                CAST((
                  cddes_subchnl.code_type
                ) AS TEXT) = CAST((
                  CAST('Sub Channel Key' AS VARCHAR)
                ) AS TEXT)
              )
              AND (
                CAST((
                  cddes_subchnl.code
                ) AS TEXT) = CAST((
                  cus_sales.sub_chnl_key
                ) AS TEXT)
              )
            )
          )
      )
      LEFT JOIN edw_subchnl_retail_env_mapping AS subchnl_retail_env
        ON (
          (
            UPPER(CAST((
              subchnl_retail_env.sub_channel
            ) AS TEXT)) = UPPER(CAST((
              cddes_subchnl.code_desc
            ) AS TEXT))
          )
        )
    )
    LEFT JOIN (
      SELECT DISTINCT
        edw_code_descriptions_manual.source_type,
        edw_code_descriptions_manual.code_type,
        edw_code_descriptions_manual.code,
        edw_code_descriptions_manual.code_desc
      FROM edw_code_descriptions_manual
    ) AS ecdm
      ON (
        (
          CAST((
            cus_sales.cust_grp
          ) AS TEXT) = CAST((
            ecdm.code
          ) AS TEXT)
        )
      )
  )
  WHERE
    (
      NOT (
        (
          COALESCE(
            LTRIM(CAST((
              cus_sales.cust_num
            ) AS TEXT), CAST((
              CAST('0' AS VARCHAR)
            ) AS TEXT)),
            CAST((
              CAST('' AS VARCHAR)
            ) AS TEXT)
          ) || CAST((
            COALESCE(cus_sales.sls_org, CAST('' AS VARCHAR))
          ) AS TEXT)
        ) IN (
          SELECT
            (
              COALESCE(
                LTRIM(
                  CAST((
                    edw_customer_sales_dim.cust_num
                  ) AS TEXT),
                  CAST((
                    CAST('0' AS VARCHAR)
                  ) AS TEXT)
                ),
                CAST((
                  CAST('' AS VARCHAR)
                ) AS TEXT)
              ) || CAST((
                COALESCE(edw_customer_sales_dim.sls_org, CAST('' AS VARCHAR))
              ) AS TEXT)
            )
          FROM edw_customer_sales_dim
          WHERE
            (
              (
                (
                  (
                    (
                      (
                        (
                          LTRIM(
                            CAST((
                              edw_customer_sales_dim.cust_num
                            ) AS TEXT),
                            CAST((
                              CAST('0' AS VARCHAR)
                            ) AS TEXT)
                          ) = CAST((
                            CAST('134106' AS VARCHAR)
                          ) AS TEXT)
                        )
                        OR (
                          LTRIM(
                            CAST((
                              edw_customer_sales_dim.cust_num
                            ) AS TEXT),
                            CAST((
                              CAST('0' AS VARCHAR)
                            ) AS TEXT)
                          ) = CAST((
                            CAST('134258' AS VARCHAR)
                          ) AS TEXT)
                        )
                      )
                      OR (
                        LTRIM(
                          CAST((
                            edw_customer_sales_dim.cust_num
                          ) AS TEXT),
                          CAST((
                            CAST('0' AS VARCHAR)
                          ) AS TEXT)
                        ) = CAST((
                          CAST('134559' AS VARCHAR)
                        ) AS TEXT)
                      )
                    )
                    OR (
                      LTRIM(
                        CAST((
                          edw_customer_sales_dim.cust_num
                        ) AS TEXT),
                        CAST((
                          CAST('0' AS VARCHAR)
                        ) AS TEXT)
                      ) = CAST((
                        CAST('135353' AS VARCHAR)
                      ) AS TEXT)
                    )
                  )
                  OR (
                    LTRIM(
                      CAST((
                        edw_customer_sales_dim.cust_num
                      ) AS TEXT),
                      CAST((
                        CAST('0' AS VARCHAR)
                      ) AS TEXT)
                    ) = CAST((
                      CAST('135520' AS VARCHAR)
                    ) AS TEXT)
                  )
                )
                OR (
                  LTRIM(
                    CAST((
                      edw_customer_sales_dim.cust_num
                    ) AS TEXT),
                    CAST((
                      CAST('0' AS VARCHAR)
                    ) AS TEXT)
                  ) = CAST((
                    CAST('135117' AS VARCHAR)
                  ) AS TEXT)
                )
              )
              AND (
                CAST((
                  edw_customer_sales_dim.sls_org
                ) AS TEXT) = CAST((
                  CAST('100A' AS VARCHAR)
                ) AS TEXT)
              )
            )
        )
      )
    )
  UNION ALL
  SELECT
    cus_sales.clnt,
    cus_sales.cust_num,
    '100A' AS sls_org,
    '15' AS dstr_chnl,
    cus_sales.div,
    cus_sales.obj_crt_prsn,
    cus_sales.rec_crt_dt,
    cus_sales.auth_grp,
    cus_sales.cust_del_flag,
    cus_sales.cust_stat_grp,
    cus_sales.cust_ord_blk,
    cus_sales.prc_pcdr_asgn,
    cus_sales.cust_grp,
    cus_sales.sls_dstrc,
    cus_sales.prc_grp,
    cus_sales.prc_list_typ,
    cus_sales.ord_prob_itm,
    cus_sales.incoterm1,
    cus_sales.incoterm2,
    cus_sales.cust_delv_blk,
    cus_sales.cmplt_delv_sls_ord,
    cus_sales.max_no_prtl_delv_allw_itm,
    cus_sales.prtl_delv_itm_lvl,
    cus_sales.ord_comb_in,
    cus_sales.btch_splt_allw,
    cus_sales.delv_prir,
    cus_sales.vend_acct_num,
    cus_sales.shipping_cond,
    cus_sales.bill_blk_cust,
    cus_sales.man_invc_maint,
    cus_sales.invc_dt,
    cus_sales.invc_list_sched,
    cus_sales.cost_est_in,
    cus_sales.val_lmt_cost_est,
    cus_sales.crncy_key,
    cus_sales.cust_clas,
    cus_sales.acct_asgnmt_grp,
    cus_sales.delv_plnt,
    cus_sales.sls_grp,
    cus_sales.sls_grp_desc,
    cus_sales.sls_ofc,
    cus_sales.sls_ofc_desc,
    cus_sales.itm_props,
    cus_sales.cust_grp1,
    cus_sales.cust_grp2,
    cus_sales.cust_grp3,
    cus_sales.cust_grp4,
    cus_sales.cust_grp5,
    cus_sales.cust_rebt_in,
    cus_sales.rebt_indx_cust_strt_prd,
    cus_sales.exch_rt_typ,
    cus_sales.prc_dtrmn_id,
    cus_sales.prod_attr_id1,
    cus_sales.prod_attr_id2,
    cus_sales.prod_attr_id3,
    cus_sales.prod_attr_id4,
    cus_sales.prod_attr_id5,
    cus_sales.prod_attr_id6,
    cus_sales.prod_attr_id7,
    cus_sales.prod_attr_id8,
    cus_sales.prod_attr_id9,
    cus_sales.prod_attr_id10,
    cus_sales.pymt_key_term,
    cus_sales.persnl_num,
    cus_sales.crt_dttm,
    cus_sales.updt_dttm,
    cus_sales.cur_sls_emp,
    cus_sales.lcl_cust_grp_1,
    cus_sales.lcl_cust_grp_2,
    cus_sales.lcl_cust_grp_3,
    cus_sales.lcl_cust_grp_4,
    cus_sales.lcl_cust_grp_5,
    cus_sales.lcl_cust_grp_6,
    cus_sales.lcl_cust_grp_7,
    cus_sales.lcl_cust_grp_8,
    cus_sales.prc_proc,
    cus_sales.par_del,
    cus_sales.max_num_pa,
    cus_sales.prnt_cust_key,
    cus_sales.bnr_key,
    cus_sales.bnr_frmt_key,
    cus_sales.go_to_mdl_key,
    cus_sales.chnl_key,
    cus_sales.sub_chnl_key,
    cus_sales.segmt_key,
    cus_sales.cust_set_1,
    cus_sales.cust_set_2,
    cus_sales.cust_set_3,
    cus_sales.cust_set_4,
    cus_sales.cust_set_5,
    cddes_pck.code_desc AS "parent customer",
    cddes_bnrkey.code_desc AS banner,
    cddes_bnrfmt.code_desc AS "banner format",
    cddes_chnl.code_desc AS channel,
    cddes_gtm.code_desc AS "go to model",
    cddes_subchnl.code_desc AS "sub channel",
    subchnl_retail_env.retail_env,
    replace(NULL ,'UNKNOWN') AS code_desc
  FROM (
    (
      (
        (
          (
            (
              (
                edw_customer_sales_dim AS cus_sales
                  LEFT JOIN edw_code_descriptions AS cddes_pck
                    ON (
                      (
                        (
                          CAST((
                            cddes_pck.code_type
                          ) AS TEXT) = CAST((
                            CAST('Parent Customer Key' AS VARCHAR)
                          ) AS TEXT)
                        )
                        AND (
                          CAST((
                            cddes_pck.code
                          ) AS TEXT) = CAST((
                            cus_sales.prnt_cust_key
                          ) AS TEXT)
                        )
                      )
                    )
              )
              LEFT JOIN edw_code_descriptions AS cddes_bnrkey
                ON (
                  (
                    (
                      CAST((
                        cddes_bnrkey.code_type
                      ) AS TEXT) = CAST((
                        CAST('Banner Key' AS VARCHAR)
                      ) AS TEXT)
                    )
                    AND (
                      CAST((
                        cddes_bnrkey.code
                      ) AS TEXT) = CAST((
                        cus_sales.bnr_key
                      ) AS TEXT)
                    )
                  )
                )
            )
            LEFT JOIN edw_code_descriptions AS cddes_bnrfmt
              ON (
                (
                  (
                    CAST((
                      cddes_bnrfmt.code_type
                    ) AS TEXT) = CAST((
                      CAST('Banner Format Key' AS VARCHAR)
                    ) AS TEXT)
                  )
                  AND (
                    CAST((
                      cddes_bnrfmt.code
                    ) AS TEXT) = CAST((
                      cus_sales.bnr_frmt_key
                    ) AS TEXT)
                  )
                )
              )
          )
          LEFT JOIN edw_code_descriptions AS cddes_chnl
            ON (
              (
                (
                  CAST((
                    cddes_chnl.code_type
                  ) AS TEXT) = CAST((
                    CAST('Channel Key' AS VARCHAR)
                  ) AS TEXT)
                )
                AND (
                  CAST((
                    cddes_chnl.code
                  ) AS TEXT) = CAST((
                    cus_sales.chnl_key
                  ) AS TEXT)
                )
              )
            )
        )
        LEFT JOIN edw_code_descriptions AS cddes_gtm
          ON (
            (
              (
                CAST((
                  cddes_gtm.code_type
                ) AS TEXT) = CAST((
                  CAST('Go To Model Key' AS VARCHAR)
                ) AS TEXT)
              )
              AND (
                CAST((
                  cddes_gtm.code
                ) AS TEXT) = CAST((
                  cus_sales.go_to_mdl_key
                ) AS TEXT)
              )
            )
          )
      )
      LEFT JOIN edw_code_descriptions AS cddes_subchnl
        ON (
          (
            (
              CAST((
                cddes_subchnl.code_type
              ) AS TEXT) = CAST((
                CAST('Sub Channel Key' AS VARCHAR)
              ) AS TEXT)
            )
            AND (
              CAST((
                cddes_subchnl.code
              ) AS TEXT) = CAST((
                cus_sales.sub_chnl_key
              ) AS TEXT)
            )
          )
        )
    )
    LEFT JOIN edw_subchnl_retail_env_mapping AS subchnl_retail_env
      ON (
        (
          UPPER(CAST((
            subchnl_retail_env.sub_channel
          ) AS TEXT)) = UPPER(CAST((
            cddes_subchnl.code_desc
          ) AS TEXT))
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
                  CAST((
                    cus_sales.cust_num
                  ) AS TEXT) = CAST((
                    CAST('0000134106' AS VARCHAR)
                  ) AS TEXT)
                )
                OR (
                  CAST((
                    cus_sales.cust_num
                  ) AS TEXT) = CAST((
                    CAST('0000134258' AS VARCHAR)
                  ) AS TEXT)
                )
              )
              OR (
                CAST((
                  cus_sales.cust_num
                ) AS TEXT) = CAST((
                  CAST('0000134559' AS VARCHAR)
                ) AS TEXT)
              )
            )
            OR (
              CAST((
                cus_sales.cust_num
              ) AS TEXT) = CAST((
                CAST('0000135353' AS VARCHAR)
              ) AS TEXT)
            )
          )
          OR (
            CAST((
              cus_sales.cust_num
            ) AS TEXT) = CAST((
              CAST('0000135520' AS VARCHAR)
            ) AS TEXT)
          )
        )
        OR (
          CAST((
            cus_sales.cust_num
          ) AS TEXT) = CAST((
            CAST('0000135117' AS VARCHAR)
          ) AS TEXT)
        )
      )
      AND (
        CAST((
          cus_sales.sls_org
        ) AS TEXT) = CAST((
          CAST('8888' AS VARCHAR)
        ) AS TEXT)
      )
    )
)
UNION ALL
SELECT
  cus_sales.clnt,
  cus_sales.cust_num,
  '100A' AS sls_org,
  '19' AS dstr_chnl,
  cus_sales.div,
  cus_sales.obj_crt_prsn,
  cus_sales.rec_crt_dt,
  cus_sales.auth_grp,
  cus_sales.cust_del_flag,
  cus_sales.cust_stat_grp,
  cus_sales.cust_ord_blk,
  cus_sales.prc_pcdr_asgn,
  cus_sales.cust_grp,
  cus_sales.sls_dstrc,
  cus_sales.prc_grp,
  cus_sales.prc_list_typ,
  cus_sales.ord_prob_itm,
  cus_sales.incoterm1,
  cus_sales.incoterm2,
  cus_sales.cust_delv_blk,
  cus_sales.cmplt_delv_sls_ord,
  cus_sales.max_no_prtl_delv_allw_itm,
  cus_sales.prtl_delv_itm_lvl,
  cus_sales.ord_comb_in,
  cus_sales.btch_splt_allw,
  cus_sales.delv_prir,
  cus_sales.vend_acct_num,
  cus_sales.shipping_cond,
  cus_sales.bill_blk_cust,
  cus_sales.man_invc_maint,
  cus_sales.invc_dt,
  cus_sales.invc_list_sched,
  cus_sales.cost_est_in,
  cus_sales.val_lmt_cost_est,
  cus_sales.crncy_key,
  cus_sales.cust_clas,
  cus_sales.acct_asgnmt_grp,
  cus_sales.delv_plnt,
  cus_sales.sls_grp,
  cus_sales.sls_grp_desc,
  cus_sales.sls_ofc,
  cus_sales.sls_ofc_desc,
  cus_sales.itm_props,
  cus_sales.cust_grp1,
  cus_sales.cust_grp2,
  cus_sales.cust_grp3,
  cus_sales.cust_grp4,
  cus_sales.cust_grp5,
  cus_sales.cust_rebt_in,
  cus_sales.rebt_indx_cust_strt_prd,
  cus_sales.exch_rt_typ,
  cus_sales.prc_dtrmn_id,
  cus_sales.prod_attr_id1,
  cus_sales.prod_attr_id2,
  cus_sales.prod_attr_id3,
  cus_sales.prod_attr_id4,
  cus_sales.prod_attr_id5,
  cus_sales.prod_attr_id6,
  cus_sales.prod_attr_id7,
  cus_sales.prod_attr_id8,
  cus_sales.prod_attr_id9,
  cus_sales.prod_attr_id10,
  cus_sales.pymt_key_term,
  cus_sales.persnl_num,
  cus_sales.crt_dttm,
  cus_sales.updt_dttm,
  cus_sales.cur_sls_emp,
  cus_sales.lcl_cust_grp_1,
  cus_sales.lcl_cust_grp_2,
  cus_sales.lcl_cust_grp_3,
  cus_sales.lcl_cust_grp_4,
  cus_sales.lcl_cust_grp_5,
  cus_sales.lcl_cust_grp_6,
  cus_sales.lcl_cust_grp_7,
  cus_sales.lcl_cust_grp_8,
  cus_sales.prc_proc,
  cus_sales.par_del,
  cus_sales.max_num_pa,
  cus_sales.prnt_cust_key,
  cus_sales.bnr_key,
  cus_sales.bnr_frmt_key,
  cus_sales.go_to_mdl_key,
  cus_sales.chnl_key,
  cus_sales.sub_chnl_key,
  cus_sales.segmt_key,
  cus_sales.cust_set_1,
  cus_sales.cust_set_2,
  cus_sales.cust_set_3,
  cus_sales.cust_set_4,
  cus_sales.cust_set_5,
  cddes_pck.code_desc AS "parent customer",
  cddes_bnrkey.code_desc AS banner,
  cddes_bnrfmt.code_desc AS "banner format",
  cddes_chnl.code_desc AS channel,
  cddes_gtm.code_desc AS "go to model",
  cddes_subchnl.code_desc AS "sub channel",
  subchnl_retail_env.retail_env,
  replace(NULL ,'UNKNOWN') AS code_desc
FROM (
  (
    (
      (
        (
          (
            (
              edw_customer_sales_dim AS cus_sales
                LEFT JOIN edw_code_descriptions AS cddes_pck
                  ON (
                    (
                      (
                        CAST((
                          cddes_pck.code_type
                        ) AS TEXT) = CAST((
                          CAST('Parent Customer Key' AS VARCHAR)
                        ) AS TEXT)
                      )
                      AND (
                        CAST((
                          cddes_pck.code
                        ) AS TEXT) = CAST((
                          cus_sales.prnt_cust_key
                        ) AS TEXT)
                      )
                    )
                  )
            )
            LEFT JOIN edw_code_descriptions AS cddes_bnrkey
              ON (
                (
                  (
                    CAST((
                      cddes_bnrkey.code_type
                    ) AS TEXT) = CAST((
                      CAST('Banner Key' AS VARCHAR)
                    ) AS TEXT)
                  )
                  AND (
                    CAST((
                      cddes_bnrkey.code
                    ) AS TEXT) = CAST((
                      cus_sales.bnr_key
                    ) AS TEXT)
                  )
                )
              )
          )
          LEFT JOIN edw_code_descriptions AS cddes_bnrfmt
            ON (
              (
                (
                  CAST((
                    cddes_bnrfmt.code_type
                  ) AS TEXT) = CAST((
                    CAST('Banner Format Key' AS VARCHAR)
                  ) AS TEXT)
                )
                AND (
                  CAST((
                    cddes_bnrfmt.code
                  ) AS TEXT) = CAST((
                    cus_sales.bnr_frmt_key
                  ) AS TEXT)
                )
              )
            )
        )
        LEFT JOIN edw_code_descriptions AS cddes_chnl
          ON (
            (
              (
                CAST((
                  cddes_chnl.code_type
                ) AS TEXT) = CAST((
                  CAST('Channel Key' AS VARCHAR)
                ) AS TEXT)
              )
              AND (
                CAST((
                  cddes_chnl.code
                ) AS TEXT) = CAST((
                  cus_sales.chnl_key
                ) AS TEXT)
              )
            )
          )
      )
      LEFT JOIN edw_code_descriptions AS cddes_gtm
        ON (
          (
            (
              CAST((
                cddes_gtm.code_type
              ) AS TEXT) = CAST((
                CAST('Go To Model Key' AS VARCHAR)
              ) AS TEXT)
            )
            AND (
              CAST((
                cddes_gtm.code
              ) AS TEXT) = CAST((
                cus_sales.go_to_mdl_key
              ) AS TEXT)
            )
          )
        )
    )
    LEFT JOIN edw_code_descriptions AS cddes_subchnl
      ON (
        (
          (
            CAST((
              cddes_subchnl.code_type
            ) AS TEXT) = CAST((
              CAST('Sub Channel Key' AS VARCHAR)
            ) AS TEXT)
          )
          AND (
            CAST((
              cddes_subchnl.code
            ) AS TEXT) = CAST((
              cus_sales.sub_chnl_key
            ) AS TEXT)
          )
        )
      )
  )
  LEFT JOIN edw_subchnl_retail_env_mapping AS subchnl_retail_env
    ON (
      (
        UPPER(CAST((
          subchnl_retail_env.sub_channel
        ) AS TEXT)) = UPPER(CAST((
          cddes_subchnl.code_desc
        ) AS TEXT))
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
                CAST((
                  cus_sales.cust_num
                ) AS TEXT) = CAST((
                  CAST('0000134106' AS VARCHAR)
                ) AS TEXT)
              )
              OR (
                CAST((
                  cus_sales.cust_num
                ) AS TEXT) = CAST((
                  CAST('0000134258' AS VARCHAR)
                ) AS TEXT)
              )
            )
            OR (
              CAST((
                cus_sales.cust_num
              ) AS TEXT) = CAST((
                CAST('0000134559' AS VARCHAR)
              ) AS TEXT)
            )
          )
          OR (
            CAST((
              cus_sales.cust_num
            ) AS TEXT) = CAST((
              CAST('0000135353' AS VARCHAR)
            ) AS TEXT)
          )
        )
        OR (
          CAST((
            cus_sales.cust_num
          ) AS TEXT) = CAST((
            CAST('0000135520' AS VARCHAR)
          ) AS TEXT)
        )
      )
      OR (
        CAST((
          cus_sales.cust_num
        ) AS TEXT) = CAST((
          CAST('0000135117' AS VARCHAR)
        ) AS TEXT)
      )
    )
    AND (
      CAST((
        cus_sales.sls_org
      ) AS TEXT) = CAST((
        CAST('8888' AS VARCHAR)
      ) AS TEXT)
    )
  )

	)
  




--Final select
select * from final 