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
edw_vw_greenlight_skus as(
    select * from {{ ref('aspedw_integration__edw_vw_greenlight_skus') }}
),
v_intrm_reg_crncy_exch_fiscper as(
    select * from {{ ref('aspedw_integration__v_intrm_reg_crncy_exch_fiscper') }}
),
transformed as(
SELECT
  derived_table1.fisc_yr AS "fisc_yr",
  derived_table1.fisc_yr_per AS "fisc_yr_per",
  derived_table1.fisc_day AS "fisc_day",
  IFF(derived_table1.ctry_nm='',null,derived_table1.ctry_nm) AS "ctry_nm",
  IFF(derived_table1."cluster"='',null,derived_table1."cluster") as "cluster",
  IFF(derived_table1.obj_crncy_co_obj='',null,derived_table1.obj_crncy_co_obj) AS "obj_crncy_co_obj",
  IFF(derived_table1."b1 mega-brand"='',null,derived_table1."b1 mega-brand") as "b1 mega-brand",
  IFF(derived_table1."b2 brand"='',null,derived_table1."b2 brand") as "b2 brand",
  IFF(derived_table1."b3 base product"='',null,derived_table1."b3 base product") as "b3 base product",
  IFF(derived_table1."b4 variant"='',null,derived_table1."b4 variant") as "b4 variant",
  IFF(derived_table1."b5 put-up"='',null,derived_table1."b5 put-up") as "b5 put-up",
  IFF(derived_table1."prod h1 operating group"='',null,derived_table1."prod h1 operating group") as "prod h1 operating group",
  IFF(derived_table1."prod h2 franchise group"='',null,derived_table1."prod h2 franchise group") as "prod h2 franchise group",
  IFF(derived_table1."prod h3 franchise"='',null,derived_table1."prod h3 franchise") as "prod h3 franchise",
  IFF(derived_table1."prod h4 product franchise"='',null,derived_table1."prod h4 product franchise") as "prod h4 product franchise",
  IFF(derived_table1."prod h5 product major"='',null,derived_table1."prod h5 product major") as "prod h5 product major",
  IFF(derived_table1."prod h6 product minor"='',null,derived_table1."prod h6 product minor") as "prod h6 product minor",
  IFF(derived_table1.pka_franchise_desc='',null,derived_table1.pka_franchise_desc) AS "pka_franchise_desc",
  IFF(derived_table1.pka_brand_desc='',null,derived_table1.pka_brand_desc) AS "pka_brand_desc",
  IFF(derived_table1.pka_sub_brand_desc='',null,derived_table1.pka_sub_brand_desc) AS "pka_sub_brand_desc",
  IFF(derived_table1.pka_variant_desc='',null,derived_table1.pka_variant_desc) AS "pka_variant_desc",
  IFF(derived_table1.pka_sub_variant_desc='',null,derived_table1.pka_sub_variant_desc) AS "pka_sub_variant_desc",
  IFF(derived_table1.pka_flavor_desc='',null,derived_table1.pka_flavor_desc) AS "pka_flavor_desc",
  IFF(derived_table1.pka_ingredient_desc='',null,derived_table1.pka_ingredient_desc) AS "pka_ingredient_desc",
  IFF(derived_table1.pka_application_desc='',null,derived_table1.pka_application_desc) AS "pka_application_desc",
  IFF(derived_table1.pka_length_desc='',null,derived_table1.pka_length_desc) AS "pka_length_desc",
  IFF(derived_table1.pka_shape_desc='',null,derived_table1.pka_shape_desc) AS "pka_shape_desc",
  IFF(derived_table1.pka_spf_desc='',null,derived_table1.pka_spf_desc) AS "pka_spf_desc",
  IFF(derived_table1.pka_cover_desc='',null,derived_table1.pka_cover_desc) AS "pka_cover_desc",
  IFF(derived_table1.pka_form_desc='',null,derived_table1.pka_form_desc) AS "pka_form_desc",
  IFF(derived_table1.pka_size_desc='',null,derived_table1.pka_size_desc) AS "pka_size_desc",
  IFF(derived_table1.pka_character_desc='',null,derived_table1.pka_character_desc) AS "pka_character_desc",
  IFF(derived_table1.pka_package_desc='',null,derived_table1.pka_package_desc) AS "pka_package_desc",
  IFF(derived_table1.pka_attribute_13_desc='',null,derived_table1.pka_attribute_13_desc) AS "pka_attribute_13_desc",
  IFF(derived_table1.pka_attribute_14_desc='',null,derived_table1.pka_attribute_14_desc) AS "pka_attribute_14_desc",
  IFF(derived_table1.pka_sku_identification_desc='',null,derived_table1.pka_sku_identification_desc) AS "pka_sku_identification_desc",
  IFF(derived_table1.pka_one_time_relabeling_desc='',null,derived_table1.pka_one_time_relabeling_desc) AS "pka_one_time_relabeling_desc",
  IFF(derived_table1.pka_product_key='',null,derived_table1.pka_product_key) AS "pka_product_key",
  IFF(derived_table1.pka_product_key_description='',null,derived_table1.pka_product_key_description) AS "pka_product_key_description",
  IFF(derived_table1.pka_product_key_description_2='',null,derived_table1.pka_product_key_description_2) AS "pka_product_key_description_2",
  IFF(derived_table1.pka_root_code='',null,derived_table1.pka_root_code) AS "pka_root_code",
  IFF(derived_table1.pka_root_code_desc_1='',null,derived_table1.pka_root_code_desc_1) AS "pka_root_code_desc_1",
  IFF(derived_table1.pka_root_code_desc_2='',null,derived_table1.pka_root_code_desc_2) AS "pka_root_code_desc_2",
  IFF(derived_table1.greenlight_sku_flag='',null,derived_table1.greenlight_sku_flag) AS "greenlight_sku_flag",
  IFF(derived_table1."parent customer"='',null,derived_table1."parent customer") as "parent customer",
  IFF(derived_table1.banner='',null,derived_table1.banner) AS "banner",
  IFF(derived_table1."banner format"='',null,derived_table1."banner format") as "banner format",
  IFF(derived_table1.channel='',null,derived_table1.channel) AS "channel",
  IFF(derived_table1."go to model"='',null,derived_table1."go to model") as "go to model",
  IFF(derived_table1."sub channel"='',null,derived_table1."sub channel") as "sub channel",
  IFF(derived_table1.retail_env='',null,derived_table1.retail_env) AS "retail_env",
  SUM(derived_table1.nts_usd) AS "nts_usd",
  SUM(derived_table1.nts_lcy) AS "nts_lcy",
  SUM(derived_table1.gts_usd) AS "gts_usd",
  SUM(derived_table1.gts_lcy) AS "gts_lcy",
  SUM(derived_table1.eq_usd) AS "eq_usd",
  SUM(derived_table1.eq_lcy) AS "eq_lcy",
  derived_table1.from_crncy AS "from_crncy",
  derived_table1.to_crncy AS "to_crncy",
  SUM(derived_table1.nts_qty) AS "nts_qty",
  SUM(derived_table1.gts_qty) AS "gts_qty",
  SUM(derived_table1.eq_qty) AS "eq_qty",
  derived_table1."product code" as "product code",
  derived_table1."product name" as "product name"
FROM (
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
    CASE
      WHEN (
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
          copa.fisc_yr = 2018
        )
      )
      THEN CAST('China Selfcare' AS VARCHAR)
      ELSE cmp.ctry_group
    END AS ctry_nm,
    CASE
      WHEN (
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
          copa.fisc_yr = 2018
        )
      )
      THEN CAST('China' AS VARCHAR)
      ELSE cmp."cluster"
    END AS "cluster",
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
    mat.mega_brnd_desc AS "b1 mega-brand",
    mat.brnd_desc AS "b2 brand",
    mat.base_prod_desc AS "b3 base product",
    mat.varnt_desc AS "b4 variant",
    mat.put_up_desc AS "b5 put-up",
    mat.prodh1_txtmd AS "prod h1 operating group",
    mat.prodh2_txtmd AS "prod h2 franchise group",
    mat.prodh3_txtmd AS "prod h3 franchise",
    mat.prodh4_txtmd AS "prod h4 product franchise",
    mat.prodh5_txtmd AS "prod h5 product major",
    mat.prodh6_txtmd AS "prod h6 product minor",
    mat.pka_franchise_desc,
    mat.pka_brand_desc,
    mat.pka_sub_brand_desc,
    mat.pka_variant_desc,
    mat.pka_sub_variant_desc,
    mat.pka_flavor_desc,
    mat.pka_ingredient_desc,
    mat.pka_application_desc,
    mat.pka_length_desc,
    mat.pka_shape_desc,
    mat.pka_spf_desc,
    mat.pka_cover_desc,
    mat.pka_form_desc,
    mat.pka_size_desc,
    mat.pka_character_desc,
    mat.pka_package_desc,
    mat.pka_attribute_13_desc,
    mat.pka_attribute_14_desc,
    mat.pka_sku_identification_desc,
    mat.pka_one_time_relabeling_desc,
    mat.pka_product_key,
    mat.pka_product_key_description,
    mat.pka_product_key_description_2,
    mat.pka_root_code,
    mat.pka_root_code_desc_1,
    mat.pka_root_code_desc_2,
    COALESCE(gn.greenlight_sku_flag, CAST('N/A' AS VARCHAR)) AS greenlight_sku_flag,
    cus_sales_extn."parent customer",
    cus_sales_extn.banner,
    cus_sales_extn."banner format",
    cus_sales_extn.channel,
    cus_sales_extn."go to model",
    cus_sales_extn."sub channel",
    cus_sales_extn.retail_env,
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
                  CAST(
                    cmp.ctry_group
                   AS TEXT) = CAST((
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
    CAST('USD' AS VARCHAR) AS to_crncy,
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
    mat.matl_num AS "product code",
    mat.matl_desc AS "product name"
  FROM (
    (
      (
        (
          (
            edw_copa_trans_fact AS copa
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
          LEFT JOIN v_edw_customer_sales_dim AS cus_sales_extn
            ON (
              (
                (
                  (
                    (
                      CAST((
                        cus_sales_extn.sls_org
                      ) AS TEXT) = CAST((
                        CASE
                          WHEN (
                            (
                              (
                                copa.sls_org IS NULL
                              )
                              AND (
                                LTRIM(CAST((
                                  copa.cust_num
                                ) AS TEXT), CAST((
                                  CAST('0' AS VARCHAR)
                                ) AS TEXT)) = CAST((
                                  CAST('135520' AS VARCHAR)
                                ) AS TEXT)
                              )
                            )
                            AND (
                              CAST((
                                copa.co_cd
                              ) AS TEXT) = CAST((
                                CAST('703A' AS VARCHAR)
                              ) AS TEXT)
                            )
                          )
                          THEN CAST('100A' AS VARCHAR)
                          ELSE copa.sls_org
                        END
                      ) AS TEXT)
                    )
                    AND (
                      CAST((
                        cus_sales_extn.dstr_chnl
                      ) AS TEXT) = CAST((
                        CASE
                          WHEN (
                            (
                              (
                                copa.dstr_chnl IS NULL
                              )
                              AND (
                                LTRIM(CAST((
                                  copa.cust_num
                                ) AS TEXT), CAST((
                                  CAST('0' AS VARCHAR)
                                ) AS TEXT)) = CAST((
                                  CAST('135520' AS VARCHAR)
                                ) AS TEXT)
                              )
                            )
                            AND (
                              CAST((
                                copa.co_cd
                              ) AS TEXT) = CAST((
                                CAST('703A' AS VARCHAR)
                              ) AS TEXT)
                            )
                          )
                          THEN CAST('15' AS VARCHAR)
                          ELSE copa.dstr_chnl
                        END
                      ) AS TEXT)
                    )
                  )
                  AND (
                    CAST((
                      copa.div
                    ) AS TEXT) = CAST((
                      cus_sales_extn.div
                    ) AS TEXT)
                  )
                )
                AND (
                  CAST((
                    copa.cust_num
                  ) AS TEXT) = CAST((
                    cus_sales_extn.cust_num
                  ) AS TEXT)
                )
              )
            )
        )
        JOIN edw_company_dim AS cmp
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
      LEFT JOIN (
        SELECT
          edw_vw_greenlight_skus.co_cd,
          edw_vw_greenlight_skus.matl_num,
          edw_vw_greenlight_skus.pka_product_key,
          edw_vw_greenlight_skus.greenlight_sku_flag
        FROM edw_vw_greenlight_skus
        GROUP BY
          edw_vw_greenlight_skus.co_cd,
          edw_vw_greenlight_skus.matl_num,
          edw_vw_greenlight_skus.pka_product_key,
          edw_vw_greenlight_skus.greenlight_sku_flag
      ) AS gn
        ON (
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
                copa.co_cd
              ) AS TEXT) = CAST((
                gn.co_cd
              ) AS TEXT)
            )
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
                    DATE_PART(YEAR, CURRENT_DATE()) - 2
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
    copa.fisc_yr,
    copa.fisc_yr_per,
    copa.obj_crncy_co_obj,
    copa.cust_num,
    copa.acct_num,
    cmp.ctry_group,
    cmp."cluster",
    mat.mega_brnd_desc,
    mat.brnd_desc,
    mat.varnt_desc,
    mat.base_prod_desc,
    mat.put_up_desc,
    mat.prodh1_txtmd,
    mat.prodh2_txtmd,
    mat.prodh3_txtmd,
    mat.prodh4_txtmd,
    mat.prodh5_txtmd,
    mat.prodh6_txtmd,
    mat.pka_franchise_cd,
    mat.pka_franchise_desc,
    mat.pka_brand_cd,
    mat.pka_brand_desc,
    mat.pka_sub_brand_cd,
    mat.pka_sub_brand_desc,
    mat.pka_variant_cd,
    mat.pka_variant_desc,
    mat.pka_sub_variant_cd,
    mat.pka_sub_variant_desc,
    mat.pka_flavor_cd,
    mat.pka_flavor_desc,
    mat.pka_ingredient_cd,
    mat.pka_ingredient_desc,
    mat.pka_application_cd,
    mat.pka_application_desc,
    mat.pka_length_cd,
    mat.pka_length_desc,
    mat.pka_shape_cd,
    mat.pka_shape_desc,
    mat.pka_spf_cd,
    mat.pka_spf_desc,
    mat.pka_cover_cd,
    mat.pka_cover_desc,
    mat.pka_form_cd,
    mat.pka_form_desc,
    mat.pka_size_cd,
    mat.pka_size_desc,
    mat.pka_character_cd,
    mat.pka_character_desc,
    mat.pka_package_cd,
    mat.pka_package_desc,
    mat.pka_attribute_13_cd,
    mat.pka_attribute_13_desc,
    mat.pka_attribute_14_cd,
    mat.pka_attribute_14_desc,
    mat.pka_sku_identification_cd,
    mat.pka_sku_identification_desc,
    mat.pka_one_time_relabeling_cd,
    mat.pka_one_time_relabeling_desc,
    mat.pka_product_key,
    mat.pka_product_key_description,
    mat.pka_product_key_description_2,
    mat.pka_root_code,
    mat.pka_root_code_desc_1,
    mat.pka_root_code_desc_2,
    gn.greenlight_sku_flag,
    cus_sales_extn."parent customer",
    cus_sales_extn.banner,
    cus_sales_extn."banner format",
    cus_sales_extn.channel,
    cus_sales_extn."go to model",
    cus_sales_extn."sub channel",
    cus_sales_extn.retail_env,
    exch_rate.from_crncy,
    exch_rate.to_crncy,
    copa.acct_hier_shrt_desc,
    mat.matl_num,
    mat.matl_desc
) AS derived_table1
GROUP BY
  derived_table1.fisc_yr,
  derived_table1.fisc_yr_per,
  derived_table1.fisc_day,
  derived_table1.ctry_nm,
  derived_table1."cluster",
  derived_table1.obj_crncy_co_obj,
  derived_table1."b1 mega-brand",
  derived_table1."b2 brand",
  derived_table1."b3 base product",
  derived_table1."b4 variant",
  derived_table1."b5 put-up",
  derived_table1."prod h1 operating group",
  derived_table1."prod h2 franchise group",
  derived_table1."prod h3 franchise",
  derived_table1."prod h4 product franchise",
  derived_table1."prod h5 product major",
  derived_table1."prod h6 product minor",
  derived_table1.pka_franchise_desc,
  derived_table1.pka_brand_desc,
  derived_table1.pka_sub_brand_desc,
  derived_table1.pka_variant_desc,
  derived_table1.pka_sub_variant_desc,
  derived_table1.pka_flavor_desc,
  derived_table1.pka_ingredient_desc,
  derived_table1.pka_application_desc,
  derived_table1.pka_length_desc,
  derived_table1.pka_shape_desc,
  derived_table1.pka_spf_desc,
  derived_table1.pka_cover_desc,
  derived_table1.pka_form_desc,
  derived_table1.pka_size_desc,
  derived_table1.pka_character_desc,
  derived_table1.pka_package_desc,
  derived_table1.pka_attribute_13_desc,
  derived_table1.pka_attribute_14_desc,
  derived_table1.pka_sku_identification_desc,
  derived_table1.pka_one_time_relabeling_desc,
  derived_table1.pka_product_key,
  derived_table1.pka_product_key_description,
  derived_table1.pka_product_key_description_2,
  derived_table1.pka_root_code,
  derived_table1.pka_root_code_desc_1,
  derived_table1.pka_root_code_desc_2,
  derived_table1.greenlight_sku_flag,
  derived_table1."parent customer",
  derived_table1.banner,
  derived_table1."banner format",
  derived_table1.channel,
  derived_table1."go to model",
  derived_table1."sub channel",
  derived_table1.retail_env,
  derived_table1.from_crncy,
  derived_table1.to_crncy,
  derived_table1."product code",
  derived_table1."product name"
)
select * from transformed