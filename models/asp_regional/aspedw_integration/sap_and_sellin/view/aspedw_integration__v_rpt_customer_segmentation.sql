{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized= "view"
    )
}}


with edw_company_dim as (
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_copa_trans_fact as (
    select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
edw_material_dim as (
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
v_intrm_reg_crncy_exch_fiscper as (
    select * from {{ ref('aspedw_integration__v_intrm_reg_crncy_exch_fiscper') }}
),
v_edw_customer_sales_dim as (
    select * from {{ ref('aspedw_integration__v_edw_customer_sales_dim') }}
),
edw_vw_greenlight_skus as (
    select * from {{ ref('aspedw_integration__edw_vw_greenlight_skus') }}
),
edw_code_descriptions_manual as (
    select * from {{ ref('aspedw_integration__edw_code_descriptions_manual') }}
),
edw_customer_base_dim as (
    select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
edw_sales_org_dim as (
    select * from {{ ref('aspedw_integration__edw_sales_org_dim') }}
),
edw_dstrbtn_chnl as (
    select * from {{ ref('aspedw_integration__edw_dstrbtn_chnl') }}
),
edw_invoice_fact as (
    select * from {{ ref('aspedw_integration__edw_invoice_fact') }}
),
itg_otif_glbl_con_reporting as (
    select * from {{ ref('aspitg_integration__itg_otif_glbl_con_reporting') }}
),
itg_mds_ap_sales_ops_map as (
    select * from {{ ref('aspitg_integration__itg_mds_ap_sales_ops_map') }}
),

final as(
    select
  copa."datasource",
  copa.fisc_yr,
  copa.fisc_yr_per,
  copa.fisc_day,
  copa.ctry_nm,
  copa.co_cd,
  copa.company_nm,
  copa.sls_org,
  sls_org_dim.sls_org_nm,
  copa.dstr_chnl,
  dist_chnl_dim.txtsh as dstr_chnl_nm,
  copa."CLUSTER",
  copa.obj_crncy_co_obj,
  cust_sales."parent customer",
  cust_sales.banner,
  cust_sales."banner format",
  cust_sales.channel,
  cust_sales."go to model",
  cust_sales."sub channel",
  cust_sales.retail_env,
  copa.cust_num,
  cust_dim.cust_nm AS customer_name,
  CASE
    WHEN (
      CAST((
        copa.co_cd
      ) AS TEXT) = CAST((
        CAST('703A' AS VARCHAR)
      ) AS TEXT)
    )
    THEN CAST('SE001' AS VARCHAR)
    ELSE cust_sales.segmt_key
  END AS segmt_key,
  CASE
    WHEN (
      CAST((
        copa.co_cd
      ) AS TEXT) = CAST((
        CAST('703A' AS VARCHAR)
      ) AS TEXT)
    )
    THEN CAST('Lead' AS VARCHAR)
    ELSE code_desc.code_desc
  END AS segment,
  COALESCE(gn.greenlight_sku_flag, CAST('N/A' AS VARCHAR)) AS greenlight_sku_flag,
  SUM(copa.nts_usd) AS nts_usd,
  SUM(copa.nts_lcy) AS nts_lcy,
  SUM(copa.gts_usd) AS gts_usd,
  SUM(copa.gts_lcy) AS gts_lcy,
  SUM(copa.numerator) AS numerator,
  SUM(copa.denominator) AS denominator
FROM (
  (
    (
      (
        (
          (
            (
              SELECT
                CAST('Sellin' AS VARCHAR) AS "datasource",
                copa.fisc_yr,
                copa.fisc_yr_per,
                TO_DATE(
                  (
                    (
                      SUBSTRING(CAST((
                        CAST((
                          copa.fisc_yr_per
                        ) AS VARCHAR)
                      ) AS TEXT), 6, 8) || CAST((
                        CAST('01' AS VARCHAR)
                      ) AS TEXT)
                    ) || SUBSTRING(CAST((
                      CAST((
                        copa.fisc_yr_per
                      ) AS VARCHAR)
                    ) AS TEXT), 1, 4)
                  ),
                  CAST((
                    CAST('MMDDYYYY' AS VARCHAR)
                  ) AS TEXT)
                ) AS fisc_day,
                CASE
                  WHEN (
                    (
                      (
                        (
                          (
                            (
                              (
                                LTRIM(
                                  CAST((
                                    copa.cust_num
                                  ) AS TEXT),
                                  CAST((
                                    CAST((
                                      0
                                    ) AS VARCHAR)
                                  ) AS TEXT)
                                ) = CAST((
                                  CAST('134559' AS VARCHAR)
                                ) AS TEXT)
                              )
                              OR (
                                LTRIM(
                                  CAST((
                                    copa.cust_num
                                  ) AS TEXT),
                                  CAST((
                                    CAST((
                                      0
                                    ) AS VARCHAR)
                                  ) AS TEXT)
                                ) = CAST((
                                  CAST('134106' AS VARCHAR)
                                ) AS TEXT)
                              )
                            )
                            OR (
                              LTRIM(
                                CAST((
                                  copa.cust_num
                                ) AS TEXT),
                                CAST((
                                  CAST((
                                    0
                                  ) AS VARCHAR)
                                ) AS TEXT)
                              ) = CAST((
                                CAST('134258' AS VARCHAR)
                              ) AS TEXT)
                            )
                          )
                          OR (
                            LTRIM(
                              CAST((
                                copa.cust_num
                              ) AS TEXT),
                              CAST((
                                CAST((
                                  0
                                ) AS VARCHAR)
                              ) AS TEXT)
                            ) = CAST((
                              CAST('134855' AS VARCHAR)
                            ) AS TEXT)
                          )
                        )
                        AND (
                          LTRIM(
                            CAST((
                              copa.acct_num
                            ) AS TEXT),
                            CAST((
                              CAST((
                                0
                              ) AS VARCHAR)
                            ) AS TEXT)
                          ) <> CAST((
                            CAST('403185' AS VARCHAR)
                          ) AS TEXT)
                        )
                      )
                      AND (
                        CAST((
                          mat.mega_brnd_desc
                        ) AS TEXT) <> CAST((
                          CAST('Vogue Int\' l ' AS VARCHAR)
                        ) AS TEXT)
                      )
                    )
                    AND (
                      copa.fisc_yr = 2018
                    )
                  )
                  THEN CAST(' China Selfcare ' AS VARCHAR)
                  ELSE cmp.ctry_group
                END AS ctry_nm,
                copa.matl_num,
                copa.co_cd,
                cmp.company_nm,
                copa.sls_org,
                copa.dstr_chnl,
                CASE
                  WHEN (
                    (
                      (
                        (
                          (
                            (
                              (
                                LTRIM(
                                  CAST((
                                    copa.cust_num
                                  ) AS TEXT),
                                  CAST((
                                    CAST((
                                      0
                                    ) AS VARCHAR)
                                  ) AS TEXT)
                                ) = CAST((
                                  CAST(' 134559 ' AS VARCHAR)
                                ) AS TEXT)
                              )
                              OR (
                                LTRIM(
                                  CAST((
                                    copa.cust_num
                                  ) AS TEXT),
                                  CAST((
                                    CAST((
                                      0
                                    ) AS VARCHAR)
                                  ) AS TEXT)
                                ) = CAST((
                                  CAST(' 134106 ' AS VARCHAR)
                                ) AS TEXT)
                              )
                            )
                            OR (
                              LTRIM(
                                CAST((
                                  copa.cust_num
                                ) AS TEXT),
                                CAST((
                                  CAST((
                                    0
                                  ) AS VARCHAR)
                                ) AS TEXT)
                              ) = CAST((
                                CAST(' 134258 ' AS VARCHAR)
                              ) AS TEXT)
                            )
                          )
                          OR (
                            LTRIM(
                              CAST((
                                copa.cust_num
                              ) AS TEXT),
                              CAST((
                                CAST((
                                  0
                                ) AS VARCHAR)
                              ) AS TEXT)
                            ) = CAST((
                              CAST(' 134855 ' AS VARCHAR)
                            ) AS TEXT)
                          )
                        )
                        AND (
                          LTRIM(
                            CAST((
                              copa.acct_num
                            ) AS TEXT),
                            CAST((
                              CAST((
                                0
                              ) AS VARCHAR)
                            ) AS TEXT)
                          ) <> CAST((
                            CAST(' 403185 ' AS VARCHAR)
                          ) AS TEXT)
                        )
                      )
                      AND (
                        CAST((
                          mat.mega_brnd_desc
                        ) AS TEXT) <> CAST((
                          CAST(' Vogue Int\'l' AS VARCHAR)
                        ) AS TEXT)
                      )
                    )
                    AND (
                      copa.fisc_yr = 2018
                    )
                  )
                  THEN CAST('China' AS VARCHAR)
                  ELSE cmp."CLUSTER"
                END AS "CLUSTER",
                CASE
                  WHEN (
                    CAST((
                      cmp.ctry_group
                    ) AS TEXT) = CAST((
                      CAST('India' AS VARCHAR)
                    ) AS TEXT)
                  )
                  THEN CAST('INR' AS VARCHAR)
                  WHEN (
                    CAST((
                      cmp.ctry_group
                    ) AS TEXT) = CAST((
                      CAST('Philippines' AS VARCHAR)
                    ) AS TEXT)
                  )
                  THEN CAST('PHP' AS VARCHAR)
                  WHEN (
                    (
                      CAST((
                        cmp.ctry_group
                      ) AS TEXT) = CAST((
                        CAST('China Selfcare' AS VARCHAR)
                      ) AS TEXT)
                    )
                    OR (
                      CAST((
                        cmp.ctry_group
                      ) AS TEXT) = CAST((
                        CAST('China Personal Care' AS VARCHAR)
                      ) AS TEXT)
                    )
                  )
                  THEN CAST('RMB' AS VARCHAR)
                  ELSE copa.obj_crncy_co_obj
                END AS obj_crncy_co_obj,
                copa.div,
                copa.cust_num,
                CASE
                  WHEN (
                    (
                      CAST((
                        copa.acct_hier_shrt_desc
                      ) AS TEXT) = CAST((
                        CAST('NTS' AS VARCHAR)
                      ) AS TEXT)
                    )
                    AND (
                      CAST((
                        exch_rate.to_crncy
                      ) AS TEXT) = CAST((
                        CAST('USD' AS VARCHAR)
                      ) AS TEXT)
                    )
                  )
                  THEN SUM((
                    copa.amt_obj_crncy * exch_rate.ex_rt
                  ))
                  ELSE CAST((
                    CAST((
                      0
                    ) AS DECIMAL)
                  ) AS DECIMAL(18, 0))
                END AS nts_usd,
                CASE
                  WHEN (
                    (
                      CAST((
                        copa.acct_hier_shrt_desc
                      ) AS TEXT) = CAST((
                        CAST('NTS' AS VARCHAR)
                      ) AS TEXT)
                    )
                    AND (
                      CAST((
                        exch_rate.to_crncy
                      ) AS TEXT) = CAST((
                        CASE
                          WHEN (
                            (
                              CAST((
                                cmp.ctry_group
                              ) AS TEXT) = CAST((
                                CAST('India' AS VARCHAR)
                              ) AS TEXT)
                            )
                            OR (
                              (
                                cmp.ctry_group IS NULL
                              ) AND (
                                'India' IS NULL
                              )
                            )
                          )
                          THEN CAST('INR' AS VARCHAR)
                          WHEN (
                            (
                              CAST((
                                cmp.ctry_group
                              ) AS TEXT) = CAST((
                                CAST('Philippines' AS VARCHAR)
                              ) AS TEXT)
                            )
                            OR (
                              (
                                cmp.ctry_group IS NULL
                              ) AND (
                                'Philippines' IS NULL
                              )
                            )
                          )
                          THEN CAST('PHP' AS VARCHAR)
                          WHEN (
                            (
                              CAST((
                                cmp.ctry_group
                              ) AS TEXT) = CAST((
                                CAST('China Selfcare' AS VARCHAR)
                              ) AS TEXT)
                            )
                            OR (
                              (
                                cmp.ctry_group IS NULL
                              ) AND (
                                'China Selfcare' IS NULL
                              )
                            )
                          )
                          THEN CAST('RMB' AS VARCHAR)
                          WHEN (
                            (
                              CAST((
                                cmp.ctry_group
                              ) AS TEXT) = CAST((
                                CAST('China Personal Care' AS VARCHAR)
                              ) AS TEXT)
                            )
                            OR (
                              (
                                cmp.ctry_group IS NULL
                              ) AND (
                                'China Personal Care' IS NULL
                              )
                            )
                          )
                          THEN CAST('RMB' AS VARCHAR)
                          ELSE copa.obj_crncy_co_obj
                        END
                      ) AS TEXT)
                    )
                  )
                  THEN SUM((
                    copa.amt_obj_crncy * exch_rate.ex_rt
                  ))
                  ELSE CAST((
                    CAST((
                      0
                    ) AS DECIMAL)
                  ) AS DECIMAL(18, 0))
                END AS nts_lcy,
                CASE
                  WHEN (
                    (
                      CAST((
                        copa.acct_hier_shrt_desc
                      ) AS TEXT) = CAST((
                        CAST('GTS' AS VARCHAR)
                      ) AS TEXT)
                    )
                    AND (
                      CAST((
                        exch_rate.to_crncy
                      ) AS TEXT) = CAST((
                        CAST('USD' AS VARCHAR)
                      ) AS TEXT)
                    )
                  )
                  THEN SUM((
                    copa.amt_obj_crncy * exch_rate.ex_rt
                  ))
                  ELSE CAST((
                    CAST((
                      0
                    ) AS DECIMAL)
                  ) AS DECIMAL(18, 0))
                END AS gts_usd,
                CASE
                  WHEN (
                    (
                      CAST((
                        copa.acct_hier_shrt_desc
                      ) AS TEXT) = CAST((
                        CAST('GTS' AS VARCHAR)
                      ) AS TEXT)
                    )
                    AND (
                      CAST((
                        exch_rate.to_crncy
                      ) AS TEXT) = CAST((
                        CASE
                          WHEN (
                            (
                              CAST((
                                cmp.ctry_group
                              ) AS TEXT) = CAST((
                                CAST('India' AS VARCHAR)
                              ) AS TEXT)
                            )
                            OR (
                              (
                                cmp.ctry_group IS NULL
                              ) AND (
                                'India' IS NULL
                              )
                            )
                          )
                          THEN CAST('INR' AS VARCHAR)
                          WHEN (
                            (
                              CAST((
                                cmp.ctry_group
                              ) AS TEXT) = CAST((
                                CAST('Philippines' AS VARCHAR)
                              ) AS TEXT)
                            )
                            OR (
                              (
                                cmp.ctry_group IS NULL
                              ) AND (
                                'Philippines' IS NULL
                              )
                            )
                          )
                          THEN CAST('PHP' AS VARCHAR)
                          WHEN (
                            (
                              CAST((
                                cmp.ctry_group
                              ) AS TEXT) = CAST((
                                CAST('China Selfcare' AS VARCHAR)
                              ) AS TEXT)
                            )
                            OR (
                              (
                                cmp.ctry_group IS NULL
                              ) AND (
                                'China Selfcare' IS NULL
                              )
                            )
                          )
                          THEN CAST('RMB' AS VARCHAR)
                          WHEN (
                            (
                              CAST((
                                cmp.ctry_group
                              ) AS TEXT) = CAST((
                                CAST('China Personal Care' AS VARCHAR)
                              ) AS TEXT)
                            )
                            OR (
                              (
                                cmp.ctry_group IS NULL
                              ) AND (
                                'China Personal Care' IS NULL
                              )
                            )
                          )
                          THEN CAST('RMB' AS VARCHAR)
                          ELSE copa.obj_crncy_co_obj
                        END
                      ) AS TEXT)
                    )
                  )
                  THEN SUM((
                    copa.amt_obj_crncy * exch_rate.ex_rt
                  ))
                  ELSE CAST((
                    CAST((
                      0
                    ) AS DECIMAL)
                  ) AS DECIMAL(18, 0))
                END AS gts_lcy,
                CASE
                  WHEN (
                    (
                      CAST((
                        copa.acct_hier_shrt_desc
                      ) AS TEXT) = CAST((
                        CAST('EQ' AS VARCHAR)
                      ) AS TEXT)
                    )
                    AND (
                      CAST((
                        exch_rate.to_crncy
                      ) AS TEXT) = CAST((
                        CAST('USD' AS VARCHAR)
                      ) AS TEXT)
                    )
                  )
                  THEN SUM((
                    copa.amt_obj_crncy * exch_rate.ex_rt
                  ))
                  ELSE CAST((
                    CAST((
                      0
                    ) AS DECIMAL)
                  ) AS DECIMAL(18, 0))
                END AS eq_usd,
                CASE
                  WHEN (
                    (
                      CAST((
                        copa.acct_hier_shrt_desc
                      ) AS TEXT) = CAST((
                        CAST('EQ' AS VARCHAR)
                      ) AS TEXT)
                    )
                    AND (
                      CAST((
                        exch_rate.to_crncy
                      ) AS TEXT) = CAST((
                        CASE
                          WHEN (
                            (
                              CAST((
                                cmp.ctry_group
                              ) AS TEXT) = CAST((
                                CAST('India' AS VARCHAR)
                              ) AS TEXT)
                            )
                            OR (
                              (
                                cmp.ctry_group IS NULL
                              ) AND (
                                'India' IS NULL
                              )
                            )
                          )
                          THEN CAST('INR' AS VARCHAR)
                          WHEN (
                            (
                              CAST((
                                cmp.ctry_group
                              ) AS TEXT) = CAST((
                                CAST('Philippines' AS VARCHAR)
                              ) AS TEXT)
                            )
                            OR (
                              (
                                cmp.ctry_group IS NULL
                              ) AND (
                                'Philippines' IS NULL
                              )
                            )
                          )
                          THEN CAST('PHP' AS VARCHAR)
                          WHEN (
                            (
                              CAST((
                                cmp.ctry_group
                              ) AS TEXT) = CAST((
                                CAST('China Selfcare' AS VARCHAR)
                              ) AS TEXT)
                            )
                            OR (
                              (
                                cmp.ctry_group IS NULL
                              ) AND (
                                'China Selfcare' IS NULL
                              )
                            )
                          )
                          THEN CAST('RMB' AS VARCHAR)
                          WHEN (
                            (
                              CAST((
                                cmp.ctry_group
                              ) AS TEXT) = CAST((
                                CAST('China Personal Care' AS VARCHAR)
                              ) AS TEXT)
                            )
                            OR (
                              (
                                cmp.ctry_group IS NULL
                              ) AND (
                                'China Personal Care' IS NULL
                              )
                            )
                          )
                          THEN CAST('RMB' AS VARCHAR)
                          ELSE copa.obj_crncy_co_obj
                        END
                      ) AS TEXT)
                    )
                  )
                  THEN SUM((
                    copa.amt_obj_crncy * exch_rate.ex_rt
                  ))
                  ELSE CAST((
                    CAST(NULL AS DECIMAL)
                  ) AS DECIMAL(18, 0))
                END AS eq_lcy,
                CASE
                  WHEN (
                    CAST((
                      cmp.ctry_group
                    ) AS TEXT) = CAST((
                      CAST('India' AS VARCHAR)
                    ) AS TEXT)
                  )
                  THEN CAST('INR' AS VARCHAR)
                  WHEN (
                    CAST((
                      cmp.ctry_group
                    ) AS TEXT) = CAST((
                      CAST('Philippines' AS VARCHAR)
                    ) AS TEXT)
                  )
                  THEN CAST('PHP' AS VARCHAR)
                  WHEN (
                    (
                      CAST((
                        cmp.ctry_group
                      ) AS TEXT) = CAST((
                        CAST('China Selfcare' AS VARCHAR)
                      ) AS TEXT)
                    )
                    OR (
                      CAST((
                        cmp.ctry_group
                      ) AS TEXT) = CAST((
                        CAST('China Personal Care' AS VARCHAR)
                      ) AS TEXT)
                    )
                  )
                  THEN CAST('RMB' AS VARCHAR)
                  ELSE exch_rate.from_crncy
                END AS from_crncy,
                exch_rate.to_crncy,
                CASE
                  WHEN (
                    (
                      CAST((
                        copa.acct_hier_shrt_desc
                      ) AS TEXT) = CAST((
                        CAST('NTS' AS VARCHAR)
                      ) AS TEXT)
                    )
                    AND (
                      CAST((
                        exch_rate.to_crncy
                      ) AS TEXT) = CAST((
                        CAST('USD' AS VARCHAR)
                      ) AS TEXT)
                    )
                  )
                  THEN SUM(copa.qty)
                  ELSE CAST((
                    CAST((
                      0
                    ) AS DECIMAL)
                  ) AS DECIMAL(18, 0))
                END AS nts_qty,
                CASE
                  WHEN (
                    (
                      CAST((
                        copa.acct_hier_shrt_desc
                      ) AS TEXT) = CAST((
                        CAST('GTS' AS VARCHAR)
                      ) AS TEXT)
                    )
                    AND (
                      CAST((
                        exch_rate.to_crncy
                      ) AS TEXT) = CAST((
                        CAST('USD' AS VARCHAR)
                      ) AS TEXT)
                    )
                  )
                  THEN SUM(copa.qty)
                  ELSE CAST((
                    CAST((
                      0
                    ) AS DECIMAL)
                  ) AS DECIMAL(18, 0))
                END AS gts_qty,
                CASE
                  WHEN (
                    (
                      CAST((
                        copa.acct_hier_shrt_desc
                      ) AS TEXT) = CAST((
                        CAST('EQ' AS VARCHAR)
                      ) AS TEXT)
                    )
                    AND (
                      CAST((
                        exch_rate.to_crncy
                      ) AS TEXT) = CAST((
                        CAST('USD' AS VARCHAR)
                      ) AS TEXT)
                    )
                  )
                  THEN SUM(copa.qty)
                  ELSE CAST((
                    CAST((
                      0
                    ) AS DECIMAL)
                  ) AS DECIMAL(18, 0))
                END AS eq_qty,
                0 AS numerator,
                0 AS denominator
              FROM (
                (
                  (
                    edw_copa_trans_fact AS copa
                      LEFT JOIN edw_company_dim AS cmp
                        ON (
                          (
                            CAST((
                              copa.co_cd
                            ) AS TEXT) = CAST((
                              cmp.co_cd
                            ) AS TEXT)
                          )
                        )
                  )
                  LEFT JOIN edw_material_dim AS mat
                    ON (
                      (
                        CAST((
                          copa.matl_num
                        ) AS TEXT) = CAST((
                          mat.matl_num
                        ) AS TEXT)
                      )
                    )
                )
                LEFT JOIN v_intrm_reg_crncy_exch_fiscper AS exch_rate
                  ON (
                    (
                      (
                        (
                          CAST((
                            copa.obj_crncy_co_obj
                          ) AS TEXT) = CAST((
                            exch_rate.from_crncy
                          ) AS TEXT)
                        )
                        AND (
                          copa.fisc_yr_per = exch_rate.fisc_per
                        )
                      )
                      AND CASE
                        WHEN (
                          CAST((
                            exch_rate.to_crncy
                          ) AS TEXT) <> CAST((
                            CAST('USD' AS VARCHAR)
                          ) AS TEXT)
                        )
                        THEN (
                          CAST((
                            exch_rate.to_crncy
                          ) AS TEXT) = CAST((
                            CASE
                              WHEN (
                                (
                                  CAST((
                                    cmp.ctry_group
                                  ) AS TEXT) = CAST((
                                    CAST('India' AS VARCHAR)
                                  ) AS TEXT)
                                )
                                OR (
                                  (
                                    cmp.ctry_group IS NULL
                                  ) AND (
                                    'India' IS NULL
                                  )
                                )
                              )
                              THEN CAST('INR' AS VARCHAR)
                              WHEN (
                                (
                                  CAST((
                                    cmp.ctry_group
                                  ) AS TEXT) = CAST((
                                    CAST('Philippines' AS VARCHAR)
                                  ) AS TEXT)
                                )
                                OR (
                                  (
                                    cmp.ctry_group IS NULL
                                  ) AND (
                                    'Philippines' IS NULL
                                  )
                                )
                              )
                              THEN CAST('PHP' AS VARCHAR)
                              WHEN (
                                (
                                  CAST((
                                    cmp.ctry_group
                                  ) AS TEXT) = CAST((
                                    CAST('China Selfcare' AS VARCHAR)
                                  ) AS TEXT)
                                )
                                OR (
                                  (
                                    cmp.ctry_group IS NULL
                                  ) AND (
                                    'China Selfcare' IS NULL
                                  )
                                )
                              )
                              THEN CAST('RMB' AS VARCHAR)
                              WHEN (
                                (
                                  CAST((
                                    cmp.ctry_group
                                  ) AS TEXT) = CAST((
                                    CAST('China Personal Care' AS VARCHAR)
                                  ) AS TEXT)
                                )
                                OR (
                                  (
                                    cmp.ctry_group IS NULL
                                  ) AND (
                                    'China Personal Care' IS NULL
                                  )
                                )
                              )
                              THEN CAST('RMB' AS VARCHAR)
                              ELSE copa.obj_crncy_co_obj
                            END
                          ) AS TEXT)
                        )
                        ELSE (
                          CAST((
                            exch_rate.to_crncy
                          ) AS TEXT) = CAST((
                            CAST('USD' AS VARCHAR)
                          ) AS TEXT)
                        )
                      END
                    )
                  )
              )
              WHERE
                (
                  (
                    (
                      (
                        CAST((
                          copa.acct_hier_shrt_desc
                        ) AS TEXT) = CAST((
                          CAST('GTS' AS VARCHAR)
                        ) AS TEXT)
                      )
                      OR (
                        CAST((
                          copa.acct_hier_shrt_desc
                        ) AS TEXT) = CAST((
                          CAST('NTS' AS VARCHAR)
                        ) AS TEXT)
                      )
                    )
                    OR (
                      CAST((
                        copa.acct_hier_shrt_desc
                      ) AS TEXT) = CAST((
                        CAST('EQ' AS VARCHAR)
                      ) AS TEXT)
                    )
                  )
                  AND (
                    CAST((CAST((copa.fisc_yr_per) AS VARCHAR))AS TEXT) >= (
                      (
                        (
                          CAST(
                            (
                              DATE_PART(YEAR, CURRENT_TIMESTAMP())-2::VARCHAR
                            ) AS TEXT) 
                          || 
                          CAST(
                            (
                              0::VARCHAR
                              ) 
                            AS TEXT)
                        ) || CAST(
                            (
                              0::VARCHAR
                              ) 
                            AS TEXT)
                      ) || CAST(
                            (
                              1::VARCHAR
                              ) 
                            AS TEXT)
                    )
                  )
                )
              GROUP BY
                1,
                copa.fisc_yr,
                copa.fisc_yr_per,
                copa.obj_crncy_co_obj,
                copa.matl_num,
                copa.co_cd,
                cmp.company_nm,
                copa.sls_org,
                copa.dstr_chnl,
                copa.div,
                copa.cust_num,
                copa.acct_num,
                copa.acct_hier_shrt_desc,
                exch_rate.from_crncy,
                exch_rate.to_crncy,
                cmp.ctry_group,
                cmp."CLUSTER",
                mat.mega_brnd_desc
            ) AS copa
            LEFT JOIN v_edw_customer_sales_dim AS cust_sales
              ON (
                (
                  (
                    (
                      (
                        CAST((
                          copa.sls_org
                        ) AS TEXT) = CAST((
                          cust_sales.sls_org
                        ) AS TEXT)
                      )
                      AND (
                        CAST((
                          copa.dstr_chnl
                        ) AS TEXT) = CAST((
                          cust_sales.dstr_chnl
                        ) AS TEXT)
                      )
                    )
                    AND (
                      CAST((
                        copa.div
                      ) AS TEXT) = CAST((
                        cust_sales.div
                      ) AS TEXT)
                    )
                  )
                  AND (
                    CAST((
                      copa.cust_num
                    ) AS TEXT) = CAST((
                      cust_sales.cust_num
                    ) AS TEXT)
                  )
                )
              )
          )
          LEFT JOIN EDW_VW_GREENLIGHT_SKUS AS gn
            ON (
              (
                (
                  (
                    LTRIM(
                      CAST((
                        copa.matl_num
                      ) AS TEXT),
                      CAST((
                        CAST((
                          0
                        ) AS VARCHAR)
                      ) AS TEXT)
                    ) = LTRIM(gn.matl_num, CAST((
                      CAST((
                        0
                      ) AS VARCHAR)
                    ) AS TEXT))
                  )
                  AND (
                    CAST((
                      copa.sls_org
                    ) AS TEXT) = CAST((
                      gn.sls_org
                    ) AS TEXT)
                  )
                )
                AND (
                  CAST((
                    copa.dstr_chnl
                  ) AS TEXT) = CAST((
                    gn.dstr_chnl
                  ) AS TEXT)
                )
              )
            )
        )
        LEFT JOIN edw_code_descriptions_manual AS code_desc
          ON (
            (
              (
                CAST((
                  code_desc.code
                ) AS TEXT) = CAST((
                  cust_sales.segmt_key
                ) AS TEXT)
              )
              AND (
                CAST((
                  code_desc.code_type
                ) AS TEXT) = CAST((
                  CAST('Customer Segmentation Key' AS VARCHAR)
                ) AS TEXT)
              )
            )
          )
      )
      LEFT JOIN edw_customer_base_dim AS cust_dim
        ON (
          (
            CAST((
              copa.cust_num
            ) AS TEXT) = CAST((
              cust_dim.cust_num
            ) AS TEXT)
          )
        )
    )
    LEFT JOIN edw_sales_org_dim AS sls_org_dim
      ON (
        (
          CAST((
            copa.sls_org
          ) AS TEXT) = CAST((
            sls_org_dim.sls_org
          ) AS TEXT)
        )
      )
  )
  LEFT JOIN edw_dstrbtn_chnl AS dist_chnl_dim
    ON (
      (
        CAST((
          copa.dstr_chnl
        ) AS TEXT) = CAST((
          dist_chnl_dim.distr_chan
        ) AS TEXT)
      )
    )
)
WHERE
  (
    (
      (
        (
          CAST((
            copa.ctry_nm
          ) AS TEXT) <> CAST((
            CAST('OTC' AS VARCHAR)
          ) AS TEXT)
        )
        AND (
          CAST((
            copa.ctry_nm
          ) AS TEXT) <> CAST((
            CAST('India' AS VARCHAR)
          ) AS TEXT)
        )
      )
      AND (
        CAST((
          copa.ctry_nm
        ) AS TEXT) <> CAST((
          CAST('Japan' AS VARCHAR)
        ) AS TEXT)
      )
    )
    AND (
      CAST((
        copa.ctry_nm
      ) AS TEXT) <> CAST((
        CAST('APSC Regional' AS VARCHAR)
      ) AS TEXT)
    )
  )
GROUP BY
  copa."datasource",
  copa.fisc_yr,
  copa.fisc_yr_per,
  copa.fisc_day,
  copa.ctry_nm,
  copa.co_cd,
  copa.company_nm,
  copa.sls_org,
  sls_org_dim.sls_org_nm,
  copa.dstr_chnl,
  dist_chnl_dim.txtsh,
  copa."CLUSTER",
  copa.obj_crncy_co_obj,
  cust_sales."parent customer",
  cust_sales.banner,
  cust_sales."banner format",
  cust_sales.channel,
  cust_sales."go to model",
  cust_sales."sub channel",
  cust_sales.retail_env,
  copa.from_crncy,
  copa.to_crncy,
  copa.cust_num,
  cust_dim.cust_nm,
  cust_sales.segmt_key,
  code_desc.code_desc,
  gn.greenlight_sku_flag
UNION ALL
SELECT
  "map".dataset AS "datasource",
  CAST((
    otif.fiscal_yr
  ) AS DECIMAL(18, 0)) AS fisc_yr,
  CAST((
    (
      (
        TO_CHAR(
          CAST(CAST((
            TO_DATE(
              (
                LEFT(CAST((
                  otif.fiscal_yr_mo
                ) AS TEXT), 4) || RIGHT(CAST((
                  otif.fiscal_yr_mo
                ) AS TEXT), 2)
              ),
              CAST((
                CAST('YYYYMM' AS VARCHAR)
              ) AS TEXT)
            )
          ) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
          CAST((
            CAST('YYYY' AS VARCHAR)
          ) AS TEXT)
        ) || CAST((
          CAST('0' AS VARCHAR)
        ) AS TEXT)
      ) || TO_CHAR(
        CAST(CAST((
          TO_DATE(
            (
              LEFT(CAST((
                otif.fiscal_yr_mo
              ) AS TEXT), 4) || RIGHT(CAST((
                otif.fiscal_yr_mo
              ) AS TEXT), 2)
            ),
            CAST((
              CAST('YYYYMM' AS VARCHAR)
            ) AS TEXT)
          )
        ) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
        CAST((
          CAST('MM' AS VARCHAR)
        ) AS TEXT)
      )
    )
  ) AS INT) AS fisc_yr_per,
  TO_DATE(
    (
      (
        RIGHT(CAST((
          otif.fiscal_yr_mo
        ) AS TEXT), 2) || CAST((
          CAST('01' AS VARCHAR)
        ) AS TEXT)
      ) || LEFT(CAST((
        otif.fiscal_yr_mo
      ) AS TEXT), 4)
    ),
    CAST((
      CAST('MMDDYYYY' AS VARCHAR)
    ) AS TEXT)
  ) AS fisc_day,
  "map".destination_market AS ctry_nm,
  inv.co_cd,
  cmp.company_nm,
  otif.salesorg AS sls_org,
  sls_org_dim.sls_org_nm,
  inv.dstr_chnl,
  dist_chnl_dim.txtsh AS dstr_chnl_nm,
  "map".destination_cluster AS "CLUSTER",
  CAST(NULL AS VARCHAR) AS obj_crncy_co_obj,
  cust_sales."parent customer",
  cust_sales.banner,
  cust_sales."banner format",
  cust_sales.channel,
  cust_sales."go to model",
  cust_sales."sub channel",
  cust_sales.retail_env,
  inv.cust_num,
  cust_dim.cust_nm AS customer_name,
  CASE
    WHEN (
      CAST((
        inv.co_cd
      ) AS TEXT) = CAST((
        CAST('703A' AS VARCHAR)
      ) AS TEXT)
    )
    THEN CAST('SE001' AS VARCHAR)
    ELSE cust_sales.segmt_key
  END AS segmt_key,
  CASE
    WHEN (
      CAST((
        inv.co_cd
      ) AS TEXT) = CAST((
        CAST('703A' AS VARCHAR)
      ) AS TEXT)
    )
    THEN CAST('Lead' AS VARCHAR)
    ELSE code_desc.code_desc
  END AS segment,
  COALESCE(gn.greenlight_sku_flag, CAST('N/A' AS VARCHAR)) AS greenlight_sku_flag,
  0 AS nts_usd,
  0 AS nts_lcy,
  0 AS gts_usd,
  0 AS gts_lcy,
  SUM(otif.numerator_unit_otifd_delivery) AS numerator,
  SUM(otif.denom_unit_otifd) AS denominator
FROM (
  (
    (
      (
        (
          (
            (
              (
                (
                  itg_otif_glbl_con_reporting AS otif
                    LEFT JOIN itg_mds_ap_sales_ops_map AS "map"
                      ON (
                        (
                          (
                            (
                              CAST((
                                otif.country
                              ) AS TEXT) = CAST((
                                "map".source_market
                              ) AS TEXT)
                            )
                            AND (
                              CAST((
                                otif.cluster_name
                              ) AS TEXT) = CAST((
                                "map".source_cluster
                              ) AS TEXT)
                            )
                          )
                          AND (
                            CAST((
                              "map".dataset
                            ) AS TEXT) = CAST((
                              CAST('OTIF-D' AS VARCHAR)
                            ) AS TEXT)
                          )
                        )
                      )
                )
                LEFT JOIN (
                  SELECT DISTINCT
                    edw_invoice_fact.co_cd,
                    edw_invoice_fact.dstr_chnl,
                    edw_invoice_fact.div,
                    edw_invoice_fact.sls_org,
                    edw_invoice_fact.sls_doc,
                    edw_invoice_fact.cust_num,
                    edw_invoice_fact.matl_num
                  FROM edw_invoice_fact
                ) AS inv
                  ON (
                    (
                      (
                        (
                          (
                            CAST((
                              otif.doc_number
                            ) AS TEXT) = CAST((
                              inv.sls_doc
                            ) AS TEXT)
                          )
                          AND (
                            CAST((
                              otif.salesorg
                            ) AS TEXT) = CAST((
                              inv.sls_org
                            ) AS TEXT)
                          )
                        )
                        AND (
                          CAST((
                            otif.sold_to_nbr
                          ) AS TEXT) = CAST((
                            inv.cust_num
                          ) AS TEXT)
                        )
                      )
                      AND (
                        CAST((
                          otif.material
                        ) AS TEXT) = CAST((
                          inv.matl_num
                        ) AS TEXT)
                      )
                    )
                  )
              )
              LEFT JOIN edw_company_dim AS cmp
                ON (
                  (
                    CAST((
                      inv.co_cd
                    ) AS TEXT) = CAST((
                      cmp.co_cd
                    ) AS TEXT)
                  )
                )
            )
            LEFT JOIN edw_customer_base_dim AS cust_dim
              ON (
                (
                  CAST((
                    inv.cust_num
                  ) AS TEXT) = CAST((
                    cust_dim.cust_num
                  ) AS TEXT)
                )
              )
          )
          LEFT JOIN edw_sales_org_dim AS sls_org_dim
            ON (
              (
                CAST((
                  otif.salesorg
                ) AS TEXT) = CAST((
                  sls_org_dim.sls_org
                ) AS TEXT)
              )
            )
        )
        LEFT JOIN edw_dstrbtn_chnl AS dist_chnl_dim
          ON (
            (
              CAST((
                inv.dstr_chnl
              ) AS TEXT) = CAST((
                dist_chnl_dim.distr_chan
              ) AS TEXT)
            )
          )
      )
      LEFT JOIN EDW_VW_GREENLIGHT_SKUS AS gn
        ON (
          (
            (
              (
                LTRIM(
                  CAST((
                    otif.material
                  ) AS TEXT),
                  CAST((
                    CAST((
                      0
                    ) AS VARCHAR)
                  ) AS TEXT)
                ) = LTRIM(gn.matl_num, CAST((
                  CAST((
                    0
                  ) AS VARCHAR)
                ) AS TEXT))
              )
              AND (
                CAST((
                  otif.salesorg
                ) AS TEXT) = CAST((
                  gn.sls_org
                ) AS TEXT)
              )
            )
            AND (
              CAST((
                inv.dstr_chnl
              ) AS TEXT) = CAST((
                gn.dstr_chnl
              ) AS TEXT)
            )
          )
        )
    )
    LEFT JOIN v_edw_customer_sales_dim AS cust_sales
      ON (
        (
          (
            (
              (
                CAST((
                  otif.salesorg
                ) AS TEXT) = CAST((
                  cust_sales.sls_org
                ) AS TEXT)
              )
              AND (
                CAST((
                  otif.sold_to_nbr
                ) AS TEXT) = CAST((
                  cust_sales.cust_num
                ) AS TEXT)
              )
            )
            AND (
              CAST((
                inv.dstr_chnl
              ) AS TEXT) = CAST((
                cust_sales.dstr_chnl
              ) AS TEXT)
            )
          )
          AND (
            CAST((
              inv.div
            ) AS TEXT) = CAST((
              cust_sales.div
            ) AS TEXT)
          )
        )
      )
  )
  LEFT JOIN edw_code_descriptions_manual AS code_desc
    ON (
      (
        (
          CAST((
            code_desc.code
          ) AS TEXT) = CAST((
            cust_sales.segmt_key
          ) AS TEXT)
        )
        AND (
          CAST((
            code_desc.code_type
          ) AS TEXT) = CAST((
            CAST('Customer Segmentation Key' AS VARCHAR)
          ) AS TEXT)
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
                      CAST((
                        otif."REGION"
                      ) AS TEXT) = CAST((
                        CAST('APAC' AS VARCHAR)
                      ) AS TEXT)
                    )
                    AND (
                      CAST((
                        otif.country
                      ) AS TEXT) <> CAST((
                        CAST('JP' AS VARCHAR)
                      ) AS TEXT)
                    )
                  )
                  AND (
                    CAST((
                      otif.no_charge_ind
                    ) AS TEXT) = CAST((
                      CAST('Revenue' AS VARCHAR)
                    ) AS TEXT)
                  )
                )
                AND (
                  otif.denom_unit_otifd <> CAST((
                    CAST((
                      CAST((
                        0
                      ) AS DECIMAL)
                    ) AS DECIMAL(18, 0))
                  ) AS DECIMAL(21, 2))
                )
              )
              AND (
                CAST((
                  otif.affiliate_flag
                ) AS TEXT) = CAST((
                  CAST('0' AS VARCHAR)
                ) AS TEXT)
              )
            )
            AND (
              CAST(

                  (DATE_PART(YEAR, CURRENT_TIMESTAMP())) - 3 :: VARCHAR
               AS TEXT) < CAST((
                otif.fiscal_yr
              ) AS TEXT)
            )
          )
          AND (
            CAST((
              "map".destination_market
            ) AS TEXT) <> CAST((
              CAST('OTC' AS VARCHAR)
            ) AS TEXT)
          )
        )
        AND (
          CAST((
            "map".destination_market
          ) AS TEXT) <> CAST((
            CAST('India' AS VARCHAR)
          ) AS TEXT)
        )
      )
      AND (
        CAST((
          "map".destination_market
        ) AS TEXT) <> CAST((
          CAST('Japan' AS VARCHAR)
        ) AS TEXT)
      )
    )
    AND (
      CAST((
        "map".destination_market
      ) AS TEXT) <> CAST((
        CAST('APSC Regional' AS VARCHAR)
      ) AS TEXT)
    )
  )
GROUP BY
  "map".dataset,
  CAST((
    otif.fiscal_yr
  ) AS DECIMAL(18, 0)),
  CAST((
    (
      (
        TO_CHAR(
          CAST(CAST((
            TO_DATE(
              (
                LEFT(CAST((
                  otif.fiscal_yr_mo
                ) AS TEXT), 4) || RIGHT(CAST((
                  otif.fiscal_yr_mo
                ) AS TEXT), 2)
              ),
              CAST((
                CAST('YYYYMM' AS VARCHAR)
              ) AS TEXT)
            )
          ) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
          CAST((
            CAST('YYYY' AS VARCHAR)
          ) AS TEXT)
        ) || CAST((
          CAST('0' AS VARCHAR)
        ) AS TEXT)
      ) || TO_CHAR(
        CAST(CAST((
          TO_DATE(
            (
              LEFT(CAST((
                otif.fiscal_yr_mo
              ) AS TEXT), 4) || RIGHT(CAST((
                otif.fiscal_yr_mo
              ) AS TEXT), 2)
            ),
            CAST((
              CAST('YYYYMM' AS VARCHAR)
            ) AS TEXT)
          )
        ) AS TIMESTAMPNTZ) AS TIMESTAMPNTZ),
        CAST((
          CAST('MM' AS VARCHAR)
        ) AS TEXT)
      )
    )
  ) AS INT),
  TO_DATE(
    (
      (
        RIGHT(CAST((
          otif.fiscal_yr_mo
        ) AS TEXT), 2) || CAST((
          CAST('01' AS VARCHAR)
        ) AS TEXT)
      ) || LEFT(CAST((
        otif.fiscal_yr_mo
      ) AS TEXT), 4)
    ),
    CAST((
      CAST('MMDDYYYY' AS VARCHAR)
    ) AS TEXT)
  ),
  "map".destination_market,
  inv.co_cd,
  cmp.company_nm,
  otif.salesorg,
  sls_org_dim.sls_org_nm,
  inv.dstr_chnl,
  dist_chnl_dim.txtsh,
  "map".destination_cluster,
  13,
  cust_sales."parent customer",
  cust_sales.banner,
  cust_sales."banner format",
  cust_sales.channel,
  cust_sales."go to model",
  cust_sales."sub channel",
  cust_sales.retail_env,
  inv.cust_num,
  cust_dim.cust_nm,
  cust_sales.segmt_key,
  code_desc.code_desc,
  gn.greenlight_sku_flag


  
)

select * from final
