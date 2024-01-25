with edw_copa_trans_fact as(
    select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
edw_company_dim as(
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_material_dim as(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
v_edw_customer_sales_dim as(
    select * from {{ ref('aspedw_integration__v_edw_customer_sales_dim') }}
),

v_intrm_reg_crncy_exch_fiscper as(
    select * from {{ ref('aspedw_integration__v_intrm_reg_crncy_exch_fiscper') }}
),
edw_acct_ciw_hier as(
    select * from {{ ref('aspedw_access__edw_acct_ciw_hier') }}
),
edw_account_ciw_dim as(
    select * from {{ ref('aspedw_integration__edw_account_ciw_dim') }}
),

edw_gch_producthierarchy as(
    select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),

transformed as(
SELECT
  fact.fisc_yr AS "fisc_yr",
  fact.fisc_yr_per AS "fisc_yr_per",
  fact.fisc_day AS "fisc_day",
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
                      fact.cust_num
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
                      fact.cust_num
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
                    fact.cust_num
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
                  fact.cust_num
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
                fact.acct_num
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
            CAST('Vogue Int''l' AS VARCHAR)
          ) AS TEXT)
        )
      )
      AND (
        fact.fisc_yr = 2018
      )
    )
    THEN CAST('China Selfcare' AS VARCHAR)
    ELSE fact.ctry_group
  END AS "ctry_nm",
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
                      fact.cust_num
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
                      fact.cust_num
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
                    fact.cust_num
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
                  fact.cust_num
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
                fact.acct_num
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
            CAST('Vogue Int''l' AS VARCHAR)
          ) AS TEXT)
        )
      )
      AND (
        fact.fisc_yr = 2018
      )
    )
    THEN CAST('China' AS VARCHAR)
    ELSE fact."cluster"
  END AS "cluster",
  fact.obj_crncy_co_obj AS "obj_crncy_co_obj",
  fact.from_crncy AS "from_crncy",
  ciw.acct_nm AS "acct_nm",
  LTRIM(CAST((
    ciw.acct_num
  ) AS TEXT), CAST((
    CAST((
      0
    ) AS VARCHAR)
  ) AS TEXT)) AS "acct_num",
  ciw.ciw_desc AS "ciw_desc",
  ciw.ciw_bucket AS "ciw_bucket",
  csw.csw_desc AS "csw_desc",
  mat.mega_brnd_desc AS "b1 mega-brand",
  mat.brnd_desc AS "b2 brand",
  mat.base_prod_desc AS "b3 base product",
  mat.varnt_desc AS "b4 variant",
  mat.put_up_desc AS "b5 put-up",
  fact.cust_num AS "cust_num",
  cus_sales_extn."parent customer",
  cus_sales_extn.banner AS "banner",
  cus_sales_extn."banner format",
  cus_sales_extn.channel AS "channel",
  cus_sales_extn."go to model",
  cus_sales_extn."sub channel",
  cus_sales_extn.retail_env AS "retail_env",
  gph.gcph_franchise AS "gcph_franchise",
  gph.gcph_brand AS "gcph_brand",
  gph.gcph_subbrand AS "gcph_subbrand",
  gph.gcph_variant AS "gcph_variant",
  gph.put_up_description AS "put_up_description",
  gph.gcph_needstate AS "gcph_needstate",
  gph.gcph_category AS "gcph_category",
  gph.gcph_subcategory AS "gcph_subcategory",
  gph.gcph_segment AS "gcph_segment",
  gph.gcph_subsegment AS "gcph_subsegment",
  gch.gcch_total_enterprise AS "gcch_total_enterprise",
  gch.gcch_retail_banner AS "gcch_retail_banner",
  gch.primary_format AS "gcch_primary_format",
  gch.distributor_attribute AS "gcch_distributor_attribute",
  fact.acct_hier_shrt_desc AS "acct_hier_shrt_desc",
  SUM(fact.qty) AS "qty",
  SUM(fact.amt_lcy) AS "amt_lcy",
  SUM(fact.amt_usd) AS "amt_usd"
FROM (
  (
    (
      (
        (
          (
            (
              SELECT
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
                company.ctry_group,
                company."cluster",
                copa.acct_num,
                copa.obj_crncy_co_obj,
                copa.matl_num,
                copa.co_cd,
                CASE
                  WHEN (
                    (
                      LTRIM(CAST((
                        copa.cust_num
                      ) AS TEXT), CAST((
                        CAST('0' AS VARCHAR)
                      ) AS TEXT)) = CAST((
                        CAST('135520' AS VARCHAR)
                      ) AS TEXT)
                    )
                    AND (
                      (
                        CAST((
                          copa.co_cd
                        ) AS TEXT) = CAST((
                          CAST('703A' AS VARCHAR)
                        ) AS TEXT)
                      )
                      OR (
                        CAST((
                          copa.co_cd
                        ) AS TEXT) = CAST((
                          CAST('8888' AS VARCHAR)
                        ) AS TEXT)
                      )
                    )
                  )
                  THEN CAST('100A' AS VARCHAR)
                  ELSE copa.sls_org
                END AS sls_org,
                CASE
                  WHEN (
                    (
                      LTRIM(CAST((
                        copa.cust_num
                      ) AS TEXT), CAST((
                        CAST('0' AS VARCHAR)
                      ) AS TEXT)) = CAST((
                        CAST('135520' AS VARCHAR)
                      ) AS TEXT)
                    )
                    AND (
                      (
                        CAST((
                          copa.co_cd
                        ) AS TEXT) = CAST((
                          CAST('703A' AS VARCHAR)
                        ) AS TEXT)
                      )
                      OR (
                        CAST((
                          copa.co_cd
                        ) AS TEXT) = CAST((
                          CAST('8888' AS VARCHAR)
                        ) AS TEXT)
                      )
                    )
                  )
                  THEN CAST('15' AS VARCHAR)
                  ELSE copa.dstr_chnl
                END AS dstr_chnl,
                copa.div,
                copa.cust_num,
                CASE
                  WHEN (
                    CAST((
                      company.ctry_group
                    ) AS TEXT) = CAST((
                      CAST('India' AS VARCHAR)
                    ) AS TEXT)
                  )
                  THEN CAST('INR' AS VARCHAR)
                  WHEN (
                    CAST((
                      company.ctry_group
                    ) AS TEXT) = CAST((
                      CAST('Philippines' AS VARCHAR)
                    ) AS TEXT)
                  )
                  THEN CAST('PHP' AS VARCHAR)
                  WHEN (
                    (
                      CAST((
                        company.ctry_group
                      ) AS TEXT) = CAST((
                        CAST('China Selfcare' AS VARCHAR)
                      ) AS TEXT)
                    )
                    OR (
                      CAST((
                        company.ctry_group
                      ) AS TEXT) = CAST((
                        CAST('China Personal Care' AS VARCHAR)
                      ) AS TEXT)
                    )
                  )
                  THEN CAST('RMB' AS VARCHAR)
                  WHEN (
                    CAST((
                      company.ctry_group
                    ) AS TEXT) = CAST((
                      CAST('Australia' AS VARCHAR)
                    ) AS TEXT)
                  )
                  THEN CAST('AUD' AS VARCHAR)
                  WHEN (
                    CAST((
                      company.ctry_group
                    ) AS TEXT) = CAST((
                      CAST('New Zealand' AS VARCHAR)
                    ) AS TEXT)
                  )
                  THEN CAST('NZD' AS VARCHAR)
                  WHEN (
                    CAST((
                      company.ctry_group
                    ) AS TEXT) = CAST((
                      CAST('APSC Regional' AS VARCHAR)
                    ) AS TEXT)
                  )
                  THEN CAST('RMB' AS VARCHAR)
                  ELSE exch_rate.from_crncy
                END AS from_crncy,
                copa.acct_hier_shrt_desc,
                CASE
                  WHEN (
                    CAST((
                      exch_rate.to_crncy
                    ) AS TEXT) = CAST((
                      CAST('USD' AS VARCHAR)
                    ) AS TEXT)
                  )
                  THEN SUM(copa.qty)
                  ELSE CAST((
                    CAST((
                      0
                    ) AS DECIMAL)
                  ) AS DECIMAL(18, 0))
                END AS qty,
                CASE
                  WHEN (
                    CAST((
                      exch_rate.to_crncy
                    ) AS TEXT) = CAST((
                      CASE
                        WHEN (
                          (
                            CAST((
                              company.ctry_group
                            ) AS TEXT) = CAST((
                              CAST('India' AS VARCHAR)
                            ) AS TEXT)
                          )
                          OR (
                            (
                              company.ctry_group IS NULL
                            ) AND (
                              'India' IS NULL
                            )
                          )
                        )
                        THEN CAST('INR' AS VARCHAR)
                        WHEN (
                          (
                            CAST((
                              company.ctry_group
                            ) AS TEXT) = CAST((
                              CAST('Philippines' AS VARCHAR)
                            ) AS TEXT)
                          )
                          OR (
                            (
                              company.ctry_group IS NULL
                            ) AND (
                              'Philippines' IS NULL
                            )
                          )
                        )
                        THEN CAST('PHP' AS VARCHAR)
                        WHEN (
                          (
                            CAST((
                              company.ctry_group
                            ) AS TEXT) = CAST((
                              CAST('China Selfcare' AS VARCHAR)
                            ) AS TEXT)
                          )
                          OR (
                            (
                              company.ctry_group IS NULL
                            ) AND (
                              'China Selfcare' IS NULL
                            )
                          )
                        )
                        THEN CAST('RMB' AS VARCHAR)
                        WHEN (
                          (
                            CAST((
                              company.ctry_group
                            ) AS TEXT) = CAST((
                              CAST('China Personal Care' AS VARCHAR)
                            ) AS TEXT)
                          )
                          OR (
                            (
                              company.ctry_group IS NULL
                            ) AND (
                              'China Personal Care' IS NULL
                            )
                          )
                        )
                        THEN CAST('RMB' AS VARCHAR)
                        WHEN (
                          (
                            CAST((
                              company.ctry_group
                            ) AS TEXT) = CAST((
                              CAST('Australia' AS VARCHAR)
                            ) AS TEXT)
                          )
                          OR (
                            (
                              company.ctry_group IS NULL
                            ) AND (
                              'Australia' IS NULL
                            )
                          )
                        )
                        THEN CAST('AUD' AS VARCHAR)
                        WHEN (
                          (
                            CAST((
                              company.ctry_group
                            ) AS TEXT) = CAST((
                              CAST('New Zealand' AS VARCHAR)
                            ) AS TEXT)
                          )
                          OR (
                            (
                              company.ctry_group IS NULL
                            ) AND (
                              'New Zealand' IS NULL
                            )
                          )
                        )
                        THEN CAST('NZD' AS VARCHAR)
                        WHEN (
                          (
                            CAST((
                              company.ctry_group
                            ) AS TEXT) = CAST((
                              CAST('APSC Regional' AS VARCHAR)
                            ) AS TEXT)
                          )
                          OR (
                            (
                              company.ctry_group IS NULL
                            ) AND (
                              'APSC Regional' IS NULL
                            )
                          )
                        )
                        THEN CAST('RMB' AS VARCHAR)
                        ELSE copa.obj_crncy_co_obj
                      END
                    ) AS TEXT)
                  )
                  THEN SUM((
                    copa.amt_obj_crncy * exch_rate.ex_rt
                  ))
                  ELSE CAST((
                    CAST((
                      0
                    ) AS DECIMAL)
                  ) AS DECIMAL(18, 0))
                END AS amt_lcy,
                CASE
                  WHEN (
                    CAST((
                      exch_rate.to_crncy
                    ) AS TEXT) = CAST((
                      CAST('USD' AS VARCHAR)
                    ) AS TEXT)
                  )
                  THEN SUM((
                    copa.amt_obj_crncy * exch_rate.ex_rt
                  ))
                  ELSE CAST((
                    CAST((
                      0
                    ) AS DECIMAL)
                  ) AS DECIMAL(18, 0))
                END AS amt_usd
              FROM (
                (
                  edw_copa_trans_fact AS copa
                    JOIN edw_company_dim AS company
                      ON (
                        (
                          CAST((
                            copa.co_cd
                          ) AS TEXT) = CAST((
                            company.co_cd
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
                                    company.ctry_group
                                  ) AS TEXT) = CAST((
                                    CAST('India' AS VARCHAR)
                                  ) AS TEXT)
                                )
                                OR (
                                  (
                                    company.ctry_group IS NULL
                                  ) AND (
                                    'India' IS NULL
                                  )
                                )
                              )
                              THEN CAST('INR' AS VARCHAR)
                              WHEN (
                                (
                                  CAST((
                                    company.ctry_group
                                  ) AS TEXT) = CAST((
                                    CAST('Philippines' AS VARCHAR)
                                  ) AS TEXT)
                                )
                                OR (
                                  (
                                    company.ctry_group IS NULL
                                  ) AND (
                                    'Philippines' IS NULL
                                  )
                                )
                              )
                              THEN CAST('PHP' AS VARCHAR)
                              WHEN (
                                (
                                  CAST((
                                    company.ctry_group
                                  ) AS TEXT) = CAST((
                                    CAST('China Selfcare' AS VARCHAR)
                                  ) AS TEXT)
                                )
                                OR (
                                  (
                                    company.ctry_group IS NULL
                                  ) AND (
                                    'China Selfcare' IS NULL
                                  )
                                )
                              )
                              THEN CAST('RMB' AS VARCHAR)
                              WHEN (
                                (
                                  CAST((
                                    company.ctry_group
                                  ) AS TEXT) = CAST((
                                    CAST('China Personal Care' AS VARCHAR)
                                  ) AS TEXT)
                                )
                                OR (
                                  (
                                    company.ctry_group IS NULL
                                  ) AND (
                                    'China Personal Care' IS NULL
                                  )
                                )
                              )
                              THEN CAST('RMB' AS VARCHAR)
                              WHEN (
                                (
                                  CAST((
                                    company.ctry_group
                                  ) AS TEXT) = CAST((
                                    CAST('Australia' AS VARCHAR)
                                  ) AS TEXT)
                                )
                                OR (
                                  (
                                    company.ctry_group IS NULL
                                  ) AND (
                                    'Australia' IS NULL
                                  )
                                )
                              )
                              THEN CAST('AUD' AS VARCHAR)
                              WHEN (
                                (
                                  CAST((
                                    company.ctry_group
                                  ) AS TEXT) = CAST((
                                    CAST('New Zealand' AS VARCHAR)
                                  ) AS TEXT)
                                )
                                OR (
                                  (
                                    company.ctry_group IS NULL
                                  ) AND (
                                    'New Zealand' IS NULL
                                  )
                                )
                              )
                              THEN CAST('NZD' AS VARCHAR)
                              WHEN (
                                (
                                  CAST((
                                    company.ctry_group
                                  ) AS TEXT) = CAST((
                                    CAST('APSC Regional' AS VARCHAR)
                                  ) AS TEXT)
                                )
                                OR (
                                  (
                                    company.ctry_group IS NULL
                                  ) AND (
                                    'APSC Regional' IS NULL
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
                    CAST((
                      copa.acct_hier_shrt_desc
                    ) AS TEXT) = CAST((
                      CAST('NTS' AS VARCHAR)
                    ) AS TEXT)
                  )
                  AND (
                    CAST((
                      CAST((
                        copa.fisc_yr_per
                      ) AS VARCHAR)
                    ) AS TEXT) >= (
                      (
                        (
                          CAST((
                            CAST((
                              (
                               DATE_PART(YEAR, to_date('2020-02-28 06:02:34.194913')) - 2 
                              )
                            ) AS VARCHAR)
                          ) AS TEXT) || CAST((
                            CAST((
                              0
                            ) AS VARCHAR)
                          ) AS TEXT)
                        ) || CAST((
                          CAST((
                            0
                          ) AS VARCHAR)
                        ) AS TEXT)
                      ) || CAST((
                        CAST((
                          1
                        ) AS VARCHAR)
                      ) AS TEXT)
                    )
                  )
                )
              GROUP BY
                company.ctry_group,
                company."cluster",
                copa.fisc_yr,
                copa.fisc_yr_per,
                copa.obj_crncy_co_obj,
                copa.acct_num,
                copa.matl_num,
                copa.co_cd,
                copa.sls_org,
                copa.dstr_chnl,
                copa.div,
                copa.cust_num,
                copa.acct_hier_shrt_desc,
                exch_rate.from_crncy,
                exch_rate.to_crncy
            ) AS fact
            LEFT JOIN edw_acct_ciw_hier AS ciw
              ON (
                (
                  (
                    LTRIM(
                      CAST((
                        fact.acct_num
                      ) AS TEXT),
                      CAST((
                        CAST((
                          0
                        ) AS VARCHAR)
                      ) AS TEXT)
                    ) = LTRIM(CAST((
                      ciw.acct_num
                    ) AS TEXT), CAST((
                      CAST((
                        0
                      ) AS VARCHAR)
                    ) AS TEXT))
                  )
                  AND (
                    CAST((
                      fact.acct_hier_shrt_desc
                    ) AS TEXT) = CAST((
                      ciw.measure_code
                    ) AS TEXT)
                  )
                )
              )
          )
          LEFT JOIN (
            SELECT
              a.acct_num,
              b.csw_acct_hier_name AS csw_desc
            FROM (
              (
                SELECT
                  edw_account_ciw_dim.acct_num,
                  edw_account_ciw_dim.bravo_acct_l3,
                  edw_account_ciw_dim.bravo_acct_l4
                FROM edw_account_ciw_dim
                WHERE
                  (
                    (
                      CAST((
                        edw_account_ciw_dim.chrt_acct
                      ) AS TEXT) = CAST((
                        CAST('CCOA' AS VARCHAR)
                      ) AS TEXT)
                    )
                    AND (
                      CAST((
                        edw_account_ciw_dim.bravo_acct_l2
                      ) AS TEXT) = CAST((
                        CAST('JJPLAC510001' AS VARCHAR)
                      ) AS TEXT)
                    )
                  )
              ) AS a
              LEFT JOIN (
                (
                  (
                    (
                      SELECT
                        CAST('JJPLAC512200' AS VARCHAR) AS csw_acct_hier_no,
                        CAST('Sales Return' AS VARCHAR) AS csw_acct_hier_name
                      UNION ALL
                      SELECT
                        CAST('JJPLAC512001' AS VARCHAR) AS csw_acct_hier_no,
                        CAST('Sales Discount & Reserve' AS VARCHAR) AS csw_acct_hier_name
                    )
                    UNION ALL
                    SELECT
                      CAST('JJPLAC513001' AS VARCHAR) AS csw_acct_hier_no,
                      CAST('Sales Incentive' AS VARCHAR) AS csw_acct_hier_name
                  )
                  UNION ALL
                  SELECT
                    CAST('JJPLAC514001' AS VARCHAR) AS csw_acct_hier_no,
                    CAST('Promo & Trade related' AS VARCHAR) AS csw_acct_hier_name
                )
                UNION ALL
                SELECT
                  CAST('JJPLAC511000' AS VARCHAR) AS csw_acct_hier_no,
                  CAST('Gross Trade Prod Sales' AS VARCHAR) AS csw_acct_hier_name
              ) AS b
                ON (
                  CASE
                    WHEN (
                      CAST((
                        a.bravo_acct_l4
                      ) AS TEXT) = CAST((
                        CAST('JJPLAC512200' AS VARCHAR)
                      ) AS TEXT)
                    )
                    THEN (
                      CAST((
                        b.csw_acct_hier_no
                      ) AS TEXT) = CAST((
                        a.bravo_acct_l4
                      ) AS TEXT)
                    )
                    ELSE (
                      CAST((
                        b.csw_acct_hier_no
                      ) AS TEXT) = CAST((
                        a.bravo_acct_l3
                      ) AS TEXT)
                    )
                  END
                )
            )
          ) AS csw
            ON (
              (
                LTRIM(
                  CAST((
                    fact.acct_num
                  ) AS TEXT),
                  CAST((
                    CAST((
                      0
                    ) AS VARCHAR)
                  ) AS TEXT)
                ) = LTRIM(CAST((
                  csw.acct_num
                ) AS TEXT), CAST((
                  CAST((
                    0
                  ) AS VARCHAR)
                ) AS TEXT))
              )
            )
        )
        LEFT JOIN edw_material_dim AS mat
          ON (
            (
              CAST((
                fact.matl_num
              ) AS TEXT) = CAST((
                mat.matl_num
              ) AS TEXT)
            )
          )
      )
      LEFT JOIN v_edw_customer_sales_dim AS cus_sales_extn
        ON (
          (
            (
              (
                (
                  CAST((
                    fact.sls_org
                  ) AS TEXT) = CAST((
                    cus_sales_extn.sls_org
                  ) AS TEXT)
                )
                AND (
                  CAST((
                    fact.dstr_chnl
                  ) AS TEXT) = CAST((
                    cus_sales_extn.dstr_chnl
                  ) AS TEXT)
                )
              )
              AND (
                CAST((
                  fact.div
                ) AS TEXT) = CAST((
                  cus_sales_extn.div
                ) AS TEXT)
              )
            )
            AND (
              CAST((
                fact.cust_num
              ) AS TEXT) = CAST((
                cus_sales_extn.cust_num
              ) AS TEXT)
            )
          )
        )
    )
    LEFT JOIN edw_gch_producthierarchy AS gph
      ON (
        (
          CAST((
            fact.matl_num
          ) AS TEXT) = CAST((
            gph.materialnumber
          ) AS TEXT)
        )
      )
  )
  LEFT JOIN ASPEDW_INTEGRATION.edw_gch_customerhierarchy AS gch
    ON (
      (
        CAST((
          fact.cust_num
        ) AS TEXT) = CAST((
          gch.customer
        ) AS TEXT)
      )
    )
)
GROUP BY
  fact.fisc_yr,
  fact.fisc_yr_per,
  fact.fisc_day,
  fact.ctry_group,
  fact."cluster",
  fact.obj_crncy_co_obj,
  fact.from_crncy,
  ciw.acct_nm,
  ciw.acct_num,
  ciw.ciw_desc,
  ciw.ciw_bucket,
  csw.csw_desc,
  mat.mega_brnd_desc,
  mat.brnd_desc,
  mat.base_prod_desc,
  mat.varnt_desc,
  mat.put_up_desc,
  fact.cust_num,
  fact.acct_num,
  cus_sales_extn."parent customer",
  cus_sales_extn.banner,
  cus_sales_extn."banner format",
  cus_sales_extn.channel,
  cus_sales_extn."go to model",
  cus_sales_extn."sub channel",
  cus_sales_extn.retail_env,
  gph.gcph_franchise,
  gph.gcph_brand,
  gph.gcph_subbrand,
  gph.gcph_variant,
  gph.put_up_description,
  gph.gcph_needstate,
  gph.gcph_category,
  gph.gcph_subcategory,
  gph.gcph_segment,
  gph.gcph_subsegment,
  gch.gcch_total_enterprise,
  gch.gcch_retail_banner,
  gch.primary_format,
  gch.distributor_attribute,
  fact.acct_hier_shrt_desc
)

select * from transformed