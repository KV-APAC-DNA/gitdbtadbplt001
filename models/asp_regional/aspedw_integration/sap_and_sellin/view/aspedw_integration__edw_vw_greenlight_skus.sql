{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';"
    )
}}


with edw_material_dim as (
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_sales_org_dim as (
    select * from {{ ref('aspedw_integration__edw_sales_org_dim') }}
),
edw_company_dim as (
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
itg_mds_ap_greenlight_skus as (
    select * from {{ ref('aspitg_integration__itg_mds_ap_greenlight_skus') }}
),
edw_material_sales_dim as (
    select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),

final as(
SELECT
  ms.sls_org,
  ms.dstr_chnl,
  LTRIM(ms.matl_num, CAST((
    CAST((
      0
    ) AS VARCHAR)
  ) AS TEXT)) AS matl_num,
  ms.base_unit,
  ms.matl_grp_1,
  ms.prod_hierarchy,
  ms.matl_grp_2,
  ms.matl_grp_3,
  ms.matl_grp_4,
  ms.matl_grp_5,
  ms.ean_num,
  ms.delv_plnt,
  ms.itm_cat_grp,
  ms.lcl_matl_grp_1,
  ms.lcl_matl_grp_2,
  ms.lcl_matl_grp_3,
  ms.lcl_matl_grp_4,
  ms.lcl_matl_grp_5,
  ms.lcl_matl_grp_6,
  ms.mstr_cd,
  ms.med_desc,
  COALESCE(m.base_uom_cd, CAST('N/A' AS VARCHAR)) AS sap_base_uom_cd,
  COALESCE(m.prch_uom_cd, CAST('N/A' AS VARCHAR)) AS sap_prchse_uom_cd,
  COALESCE(m.matl_desc, CAST('N/A' AS VARCHAR)) AS matl_desc,
  COALESCE(m.matl_grp_cd, CAST('N/A' AS VARCHAR)) AS matl_grp_cd,
  COALESCE(m.prod_base, CAST('N/A' AS VARCHAR)) AS sap_base_prod_cd,
  COALESCE(m.base_prod_desc, CAST('N/A' AS VARCHAR)) AS base_prod_desc,
  COALESCE(m.mega_brnd_cd, CAST('N/A' AS VARCHAR)) AS sap_mega_brnd_cd,
  COALESCE(m.mega_brnd_desc, CAST('N/A' AS VARCHAR)) AS mega_brnd_desc,
  COALESCE(m.brnd_cd, CAST('N/A' AS VARCHAR)) AS sap_brnd_cd,
  COALESCE(m.brnd_desc, CAST('N/A' AS VARCHAR)) AS brnd_desc,
  COALESCE(m.vrnt, CAST('N/A' AS VARCHAR)) AS sap_vrnt_cd,
  COALESCE(m.varnt_desc, CAST('N/A' AS VARCHAR)) AS varnt_desc,
  COALESCE(m.put_up, CAST('N/A' AS VARCHAR)) AS sap_put_up_cd,
  COALESCE(m.put_up_desc, CAST('N/A' AS VARCHAR)) AS put_up_desc,
  COALESCE(m.prod_hier_cd, CAST('N/A' AS VARCHAR)) AS prod_hier_cd,
  COALESCE(m.prodh1, CAST('N/A' AS VARCHAR)) AS prodh1,
  COALESCE(m.prodh1_txtmd, CAST('N/A' AS VARCHAR)) AS prodh1_txtmd,
  COALESCE(m.prodh2, CAST('N/A' AS VARCHAR)) AS prodh2,
  COALESCE(m.prodh2_txtmd, CAST('N/A' AS VARCHAR)) AS prodh2_txtmd,
  COALESCE(m.prodh3, CAST('N/A' AS VARCHAR)) AS prodh3,
  COALESCE(m.prodh3_txtmd, CAST('N/A' AS VARCHAR)) AS prodh3_txtmd,
  COALESCE(m.prodh4, CAST('N/A' AS VARCHAR)) AS prodh4,
  COALESCE(m.prodh4_txtmd, CAST('N/A' AS VARCHAR)) AS prodh4_txtmd,
  COALESCE(m.prodh5, CAST('N/A' AS VARCHAR)) AS prodh5,
  COALESCE(m.prodh5_txtmd, CAST('N/A' AS VARCHAR)) AS prodh5_txtmd,
  COALESCE(m.prodh6, CAST('N/A' AS VARCHAR)) AS prodh6,
  COALESCE(m.prodh6_txtmd, CAST('N/A' AS VARCHAR)) AS prodh6_txtmd,
  COALESCE(m.matl_type_cd, CAST('N/A' AS VARCHAR)) AS matl_type_cd,
  COALESCE(m.matl_type_desc, CAST('N/A' AS VARCHAR)) AS matl_type_desc,
  COALESCE(m.pka_franchise_cd, CAST('N/A' AS VARCHAR)) AS pka_franchise_cd,
  COALESCE(m.pka_franchise_desc, CAST('N/A' AS VARCHAR)) AS pka_franchise_desc,
  COALESCE(m.pka_brand_cd, CAST('N/A' AS VARCHAR)) AS pka_brand_cd,
  COALESCE(m.pka_brand_desc, CAST('N/A' AS VARCHAR)) AS pka_brand_desc,
  COALESCE(m.pka_sub_brand_cd, CAST('N/A' AS VARCHAR)) AS pka_sub_brand_cd,
  COALESCE(m.pka_sub_brand_desc, CAST('N/A' AS VARCHAR)) AS pka_sub_brand_desc,
  COALESCE(m.pka_variant_cd, CAST('N/A' AS VARCHAR)) AS pka_variant_cd,
  COALESCE(m.pka_variant_desc, CAST('N/A' AS VARCHAR)) AS pka_variant_desc,
  COALESCE(m.pka_sub_variant_cd, CAST('N/A' AS VARCHAR)) AS pka_sub_variant_cd,
  COALESCE(m.pka_sub_variant_desc, CAST('N/A' AS VARCHAR)) AS pka_sub_variant_desc,
  COALESCE(m.pka_flavor_cd, CAST('N/A' AS VARCHAR)) AS pka_flavor_cd,
  COALESCE(m.pka_flavor_desc, CAST('N/A' AS VARCHAR)) AS pka_flavor_desc,
  COALESCE(m.pka_ingredient_cd, CAST('N/A' AS VARCHAR)) AS pka_ingredient_cd,
  COALESCE(m.pka_ingredient_desc, CAST('N/A' AS VARCHAR)) AS pka_ingredient_desc,
  COALESCE(m.pka_application_cd, CAST('N/A' AS VARCHAR)) AS pka_application_cd,
  COALESCE(m.pka_application_desc, CAST('N/A' AS VARCHAR)) AS pka_application_desc,
  COALESCE(m.pka_length_cd, CAST('N/A' AS VARCHAR)) AS pka_length_cd,
  COALESCE(m.pka_length_desc, CAST('N/A' AS VARCHAR)) AS pka_length_desc,
  COALESCE(m.pka_shape_cd, CAST('N/A' AS VARCHAR)) AS pka_shape_cd,
  COALESCE(m.pka_shape_desc, CAST('N/A' AS VARCHAR)) AS pka_shape_desc,
  COALESCE(m.pka_spf_cd, CAST('N/A' AS VARCHAR)) AS pka_spf_cd,
  COALESCE(m.pka_spf_desc, CAST('N/A' AS VARCHAR)) AS pka_spf_desc,
  COALESCE(m.pka_cover_cd, CAST('N/A' AS VARCHAR)) AS pka_cover_cd,
  COALESCE(m.pka_cover_desc, CAST('N/A' AS VARCHAR)) AS pka_cover_desc,
  COALESCE(m.pka_form_cd, CAST('N/A' AS VARCHAR)) AS pka_form_cd,
  COALESCE(m.pka_form_desc, CAST('N/A' AS VARCHAR)) AS pka_form_desc,
  COALESCE(m.pka_size_cd, CAST('N/A' AS VARCHAR)) AS pka_size_cd,
  COALESCE(m.pka_size_desc, CAST('N/A' AS VARCHAR)) AS pka_size_desc,
  COALESCE(m.pka_character_cd, CAST('N/A' AS VARCHAR)) AS pka_character_cd,
  COALESCE(m.pka_character_desc, CAST('N/A' AS VARCHAR)) AS pka_character_desc,
  COALESCE(m.pka_package_cd, CAST('N/A' AS VARCHAR)) AS pka_package_cd,
  COALESCE(m.pka_package_desc, CAST('N/A' AS VARCHAR)) AS pka_package_desc,
  COALESCE(m.pka_attribute_13_cd, CAST('N/A' AS VARCHAR)) AS pka_attribute_13_cd,
  COALESCE(m.pka_attribute_13_desc, CAST('N/A' AS VARCHAR)) AS pka_attribute_13_desc,
  COALESCE(m.pka_attribute_14_cd, CAST('N/A' AS VARCHAR)) AS pka_attribute_14_cd,
  COALESCE(m.pka_attribute_14_desc, CAST('N/A' AS VARCHAR)) AS pka_attribute_14_desc,
  COALESCE(m.pka_sku_identification_cd, CAST('N/A' AS VARCHAR)) AS pka_sku_identification_cd,
  COALESCE(m.pka_sku_identification_desc, CAST('N/A' AS VARCHAR)) AS pka_sku_identification_desc,
  COALESCE(m.pka_one_time_relabeling_cd, CAST('N/A' AS VARCHAR)) AS pka_one_time_relabeling_cd,
  COALESCE(m.pka_one_time_relabeling_desc, CAST('N/A' AS VARCHAR)) AS pka_one_time_relabeling_desc,
  COALESCE(m.pka_product_key, CAST('N/A' AS VARCHAR)) AS pka_product_key,
  COALESCE(m.pka_product_key_description, CAST('N/A' AS VARCHAR)) AS pka_product_key_description,
  COALESCE(m.pka_product_key_description_2, CAST('N/A' AS VARCHAR)) AS pka_product_key_description_2,
  COALESCE(m.pka_root_code, CAST('N/A' AS VARCHAR)) AS pka_root_code,
  COALESCE(m.pka_root_code_desc_1, CAST('N/A' AS VARCHAR)) AS pka_root_code_desc_1,
  COALESCE(s.sls_org_nm, CAST('N/A' AS VARCHAR)) AS sls_org_nm,
  COALESCE(s.sls_org_co_cd, CAST('N/A' AS VARCHAR)) AS sls_org_co_cd,
  COALESCE(s.ctry_key, CAST('N/A' AS VARCHAR)) AS ctry_key,
  COALESCE(s.crncy_key, CAST('N/A' AS VARCHAR)) AS crncy_key,
  COALESCE(c.co_cd, CAST('N/A' AS VARCHAR)) AS co_cd,
  COALESCE(c.ctry_nm, CAST('N/A' AS VARCHAR)) AS ctry_nm,
  COALESCE(c.ctry_group, CAST('N/A' AS VARCHAR)) AS ctry_group,
  COALESCE(c."CLUSTER", CAST('N/A' AS VARCHAR)) AS "CLUSTER",
  COALESCE(g.market, CAST('N/A' AS VARCHAR)) AS market,
  COALESCE(g.material_description, CAST('N/A' AS VARCHAR)) AS material_description,
  COALESCE(g.product_key_description, CAST('N/A' AS VARCHAR)) AS product_key_description,
  COALESCE(g.brand_name, CAST('N/A' AS VARCHAR)) AS brand_name,
  COALESCE(g.package, CAST('N/A' AS VARCHAR)) AS package,
  COALESCE(g.product_key, CAST('N/A' AS VARCHAR)) AS product_key,
  COALESCE(g.greenlight_sku_flag, CAST('N/A' AS VARCHAR)) AS greenlight_sku_flag,
  COALESCE(g.red_sku_flag, CAST('N/A' AS VARCHAR)) AS red_sku_flag,
  COALESCE(g.root_code, CAST('N/A' AS VARCHAR)) AS root_code
FROM (
  (
    (
      (
        (
          SELECT DISTINCT
            edw_material_sales_dim.sls_org,
            edw_material_sales_dim.dstr_chnl,
            LTRIM(
              CAST((
                edw_material_sales_dim.matl_num
              ) AS TEXT),
              CAST((
                CAST((
                  0
                ) AS VARCHAR)
              ) AS TEXT)
            ) AS matl_num,
            edw_material_sales_dim.base_unit,
            edw_material_sales_dim.matl_grp_1,
            edw_material_sales_dim.prod_hierarchy,
            edw_material_sales_dim.matl_grp_2,
            edw_material_sales_dim.matl_grp_3,
            edw_material_sales_dim.matl_grp_4,
            edw_material_sales_dim.matl_grp_5,
            edw_material_sales_dim.ean_num,
            edw_material_sales_dim.delv_plnt,
            edw_material_sales_dim.itm_cat_grp,
            edw_material_sales_dim.lcl_matl_grp_1,
            edw_material_sales_dim.lcl_matl_grp_2,
            edw_material_sales_dim.lcl_matl_grp_3,
            edw_material_sales_dim.lcl_matl_grp_4,
            edw_material_sales_dim.lcl_matl_grp_5,
            edw_material_sales_dim.lcl_matl_grp_6,
            edw_material_sales_dim.mstr_cd,
            edw_material_sales_dim.med_desc
          FROM edw_material_sales_dim
          WHERE
            (
              TRIM(CAST((
                edw_material_sales_dim.matl_num
              ) AS TEXT)) <> CAST((
                CAST('' AS VARCHAR)
              ) AS TEXT)
            )
        ) AS ms
        LEFT JOIN edw_material_dim AS m
          ON (
            (
              LTRIM(CAST((
                m.matl_num
              ) AS TEXT), CAST((
                CAST((
                  0
                ) AS VARCHAR)
              ) AS TEXT)) = LTRIM(ms.matl_num, CAST((
                CAST((
                  0
                ) AS VARCHAR)
              ) AS TEXT))
            )
          )
      )
      LEFT JOIN edw_sales_org_dim AS s
        ON (
          (
            CAST((
              s.sls_org
            ) AS TEXT) = CAST((
              ms.sls_org
            ) AS TEXT)
          )
        )
    )
    LEFT JOIN edw_company_dim AS c
      ON (
        (
          CAST((
            c.co_cd
          ) AS TEXT) = CAST((
            s.sls_org_co_cd
          ) AS TEXT)
        )
      )
  )
  LEFT JOIN itg_mds_ap_greenlight_skus AS g
    ON (
      (
        (
          CASE
            WHEN (
              (
                UPPER(CAST((
                  g.market
                ) AS TEXT)) = CAST((
                  CAST('CHINA OTC' AS VARCHAR)
                ) AS TEXT)
              )
              OR (
                (
                  UPPER(CAST((
                    g.market
                  ) AS TEXT)) IS NULL
                ) AND (
                  'CHINA OTC' IS NULL
                )
              )
            )
            THEN CAST((
              CAST('OTC' AS VARCHAR)
            ) AS TEXT)
            WHEN (
              (
                UPPER(CAST((
                  g.market
                ) AS TEXT)) = CAST((
                  CAST('CHINA SKINCARE' AS VARCHAR)
                ) AS TEXT)
              )
              OR (
                (
                  UPPER(CAST((
                    g.market
                  ) AS TEXT)) IS NULL
                )
                AND (
                  'CHINA SKINCARE' IS NULL
                )
              )
            )
            THEN CAST((
              CAST('CHINA' AS VARCHAR)
            ) AS TEXT)
            ELSE UPPER(CAST((
              g.market
            ) AS TEXT))
          END = CASE
            WHEN (
              (
                UPPER(CAST((
                  c.ctry_group
                ) AS TEXT)) = CAST((
                  CAST('AUSTRALIA' AS VARCHAR)
                ) AS TEXT)
              )
              OR (
                (
                  UPPER(CAST((
                    c.ctry_group
                  ) AS TEXT)) IS NULL
                )
                AND (
                  'AUSTRALIA' IS NULL
                )
              )
            )
            THEN CAST((
              CAST('PACIFIC' AS VARCHAR)
            ) AS TEXT)
            WHEN (
              (
                UPPER(CAST((
                  c.ctry_group
                ) AS TEXT)) = CAST((
                  CAST('NEW ZEALAND' AS VARCHAR)
                ) AS TEXT)
              )
              OR (
                (
                  UPPER(CAST((
                    c.ctry_group
                  ) AS TEXT)) IS NULL
                )
                AND (
                  'NEW ZEALAND' IS NULL
                )
              )
            )
            THEN CAST((
              CAST('PACIFIC' AS VARCHAR)
            ) AS TEXT)
            ELSE UPPER(CAST((
              c.ctry_group
            ) AS TEXT))
          END
        )
        AND (
          LTRIM(
            CAST((
              g.material_number
            ) AS TEXT),
            CAST((
              CAST((
                0
              ) AS VARCHAR)
            ) AS TEXT)
          ) = LTRIM(ms.matl_num, CAST((
            CAST((
              0
            ) AS VARCHAR)
          ) AS TEXT))
        )
      )
    )
)
)

select * from final
