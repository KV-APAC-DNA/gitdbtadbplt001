
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
select
  ms.sls_org,
  ms.dstr_chnl,
  ltrim(ms.matl_num, cast((cast((0) as varchar)) as text)) as matl_num,
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
  coalesce(m.base_uom_cd, cast('N/A' as varchar)) as sap_base_uom_cd,
  coalesce(m.prch_uom_cd, cast('N/A' as varchar)) as sap_prchse_uom_cd,
  coalesce(m.matl_desc, cast('N/A' as varchar)) as matl_desc,
  coalesce(m.matl_grp_cd, cast('N/A' as varchar)) as matl_grp_cd,
  coalesce(m.prod_base, cast('N/A' as varchar)) as sap_base_prod_cd,
  coalesce(m.base_prod_desc, cast('N/A' as varchar)) as base_prod_desc,
  coalesce(m.mega_brnd_cd, cast('N/A' as varchar)) as sap_mega_brnd_cd,
  coalesce(m.mega_brnd_desc, cast('N/A' as varchar)) as mega_brnd_desc,
  coalesce(m.brnd_cd, cast('N/A' as varchar)) as sap_brnd_cd,
  coalesce(m.brnd_desc, cast('N/A' as varchar)) as brnd_desc,
  coalesce(m.vrnt, cast('N/A' as varchar)) as sap_vrnt_cd,
  coalesce(m.varnt_desc, cast('N/A' as varchar)) as varnt_desc,
  coalesce(m.put_up, cast('N/A' as varchar)) as sap_put_up_cd,
  coalesce(m.put_up_desc, cast('N/A' as varchar)) as put_up_desc,
  coalesce(m.prod_hier_cd, cast('N/A' as varchar)) as prod_hier_cd,
  coalesce(m.prodh1, cast('N/A' as varchar)) as prodh1,
  coalesce(m.prodh1_txtmd, cast('N/A' as varchar)) as prodh1_txtmd,
  coalesce(m.prodh2, cast('N/A' as varchar)) as prodh2,
  coalesce(m.prodh2_txtmd, cast('N/A' as varchar)) as prodh2_txtmd,
  coalesce(m.prodh3, cast('N/A' as varchar)) as prodh3,
  coalesce(m.prodh3_txtmd, cast('N/A' as varchar)) as prodh3_txtmd,
  coalesce(m.prodh4, cast('N/A' as varchar)) as prodh4,
  coalesce(m.prodh4_txtmd, cast('N/A' as varchar)) as prodh4_txtmd,
  coalesce(m.prodh5, cast('N/A' as varchar)) as prodh5,
  coalesce(m.prodh5_txtmd, cast('N/A' as varchar)) as prodh5_txtmd,
  coalesce(m.prodh6, cast('N/A' as varchar)) as prodh6,
  coalesce(m.prodh6_txtmd, cast('N/A' as varchar)) as prodh6_txtmd,
  coalesce(m.matl_type_cd, cast('N/A' as varchar)) as matl_type_cd,
  coalesce(m.matl_type_desc, cast('N/A' as varchar)) as matl_type_desc,
  coalesce(m.pka_franchise_cd, cast('N/A' as varchar)) as pka_franchise_cd,
  coalesce(m.pka_franchise_desc, cast('N/A' as varchar)) as pka_franchise_desc,
  coalesce(m.pka_brand_cd, cast('N/A' as varchar)) as pka_brand_cd,
  coalesce(m.pka_brand_desc, cast('N/A' as varchar)) as pka_brand_desc,
  coalesce(m.pka_sub_brand_cd, cast('N/A' as varchar)) as pka_sub_brand_cd,
  coalesce(m.pka_sub_brand_desc, cast('N/A' as varchar)) as pka_sub_brand_desc,
  coalesce(m.pka_variant_cd, cast('N/A' as varchar)) as pka_variant_cd,
  coalesce(m.pka_variant_desc, cast('N/A' as varchar)) as pka_variant_desc,
  coalesce(m.pka_sub_variant_cd, cast('N/A' as varchar)) as pka_sub_variant_cd,
  coalesce(m.pka_sub_variant_desc, cast('N/A' as varchar)) as pka_sub_variant_desc,
  coalesce(m.pka_flavor_cd, cast('N/A' as varchar)) as pka_flavor_cd,
  coalesce(m.pka_flavor_desc, cast('N/A' as varchar)) as pka_flavor_desc,
  coalesce(m.pka_ingredient_cd, cast('N/A' as varchar)) as pka_ingredient_cd,
  coalesce(m.pka_ingredient_desc, cast('N/A' as varchar)) as pka_ingredient_desc,
  coalesce(m.pka_application_cd, cast('N/A' as varchar)) as pka_application_cd,
  coalesce(m.pka_application_desc, cast('N/A' as varchar)) as pka_application_desc,
  coalesce(m.pka_length_cd, cast('N/A' as varchar)) as pka_length_cd,
  coalesce(m.pka_length_desc, cast('N/A' as varchar)) as pka_length_desc,
  coalesce(m.pka_shape_cd, cast('N/A' as varchar)) as pka_shape_cd,
  coalesce(m.pka_shape_desc, cast('N/A' as varchar)) as pka_shape_desc,
  coalesce(m.pka_spf_cd, cast('N/A' as varchar)) as pka_spf_cd,
  coalesce(m.pka_spf_desc, cast('N/A' as varchar)) as pka_spf_desc,
  coalesce(m.pka_cover_cd, cast('N/A' as varchar)) as pka_cover_cd,
  coalesce(m.pka_cover_desc, cast('N/A' as varchar)) as pka_cover_desc,
  coalesce(m.pka_form_cd, cast('N/A' as varchar)) as pka_form_cd,
  coalesce(m.pka_form_desc, cast('N/A' as varchar)) as pka_form_desc,
  coalesce(m.pka_size_cd, cast('N/A' as varchar)) as pka_size_cd,
  coalesce(m.pka_size_desc, cast('N/A' as varchar)) as pka_size_desc,
  coalesce(m.pka_character_cd, cast('N/A' as varchar)) as pka_character_cd,
  coalesce(m.pka_character_desc, cast('N/A' as varchar)) as pka_character_desc,
  coalesce(m.pka_package_cd, cast('N/A' as varchar)) as pka_package_cd,
  coalesce(m.pka_package_desc, cast('N/A' as varchar)) as pka_package_desc,
  coalesce(m.pka_attribute_13_cd, cast('N/A' as varchar)) as pka_attribute_13_cd,
  coalesce(m.pka_attribute_13_desc, cast('N/A' as varchar)) as pka_attribute_13_desc,
  coalesce(m.pka_attribute_14_cd, cast('N/A' as varchar)) as pka_attribute_14_cd,
  coalesce(m.pka_attribute_14_desc, cast('N/A' as varchar)) as pka_attribute_14_desc,
  coalesce(m.pka_sku_identification_cd, cast('N/A' as varchar)) as pka_sku_identification_cd,
  coalesce(m.pka_sku_identification_desc, cast('N/A' as varchar)) as pka_sku_identification_desc,
  coalesce(m.pka_one_time_relabeling_cd, cast('N/A' as varchar)) as pka_one_time_relabeling_cd,
  coalesce(m.pka_one_time_relabeling_desc, cast('N/A' as varchar)) as pka_one_time_relabeling_desc,
  coalesce(m.pka_product_key, cast('N/A' as varchar)) as pka_product_key,
  coalesce(m.pka_product_key_description, cast('N/A' as varchar)) as pka_product_key_description,
  coalesce(m.pka_product_key_description_2, cast('N/A' as varchar)) as pka_product_key_description_2,
  coalesce(m.pka_root_code, cast('N/A' as varchar)) as pka_root_code,
  coalesce(m.pka_root_code_desc_1, cast('N/A' as varchar)) as pka_root_code_desc_1,
  coalesce(s.sls_org_nm, cast('N/A' as varchar)) as sls_org_nm,
  coalesce(s.sls_org_co_cd, cast('N/A' as varchar)) as sls_org_co_cd,
  coalesce(s.ctry_key, cast('N/A' as varchar)) as ctry_key,
  coalesce(s.crncy_key, cast('N/A' as varchar)) as crncy_key,
  coalesce(c.co_cd, cast('N/A' as varchar)) as co_cd,
  coalesce(c.ctry_nm, cast('N/A' as varchar)) as ctry_nm,
  coalesce(c.ctry_group, cast('N/A' as varchar)) as ctry_group,
  coalesce(c."CLUSTER", cast('N/A' as varchar)) as "cluster",
  coalesce(g.market, cast('N/A' as varchar)) as market,
  coalesce(g.material_description, cast('N/A' as varchar)) as material_description,
  coalesce(g.product_key_description, cast('N/A' as varchar)) as product_key_description,
  coalesce(g.brand_name, cast('N/A' as varchar)) as brand_name,
  coalesce(g.package, cast('N/A' as varchar)) as package,
  coalesce(g.product_key, cast('N/A' as varchar)) as product_key,
  coalesce(g.greenlight_sku_flag, cast('N/A' as varchar)) as greenlight_sku_flag,
  coalesce(g.red_sku_flag, cast('N/A' as varchar)) as red_sku_flag,
  coalesce(g.root_code, cast('N/A' as varchar)) as root_code
from (
  (
    (
      (
        (
          select distinct
            edw_material_sales_dim.sls_org,
            edw_material_sales_dim.dstr_chnl,
            ltrim(
              cast((
                edw_material_sales_dim.matl_num
              ) as text),
              cast((
                cast((
                  0
                ) as varchar)
              ) as text)
            ) as matl_num,
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
          from edw_material_sales_dim
          where
            (
              trim(cast((
                edw_material_sales_dim.matl_num
              ) as text)) <> cast((
                cast('' as varchar)
              ) as text)
            )
        ) as ms
        left join edw_material_dim as m
          on (
            (
              ltrim(cast((
                m.matl_num
              ) as text), cast((
                cast((
                  0
                ) as varchar)
              ) as text)) = ltrim(ms.matl_num, cast((
                cast((
                  0
                ) as varchar)
              ) as text))
            )
          )
      )
      left join edw_sales_org_dim as s
        on (
          (
            cast((
              s.sls_org
            ) as text) = cast((
              ms.sls_org
            ) as text)
          )
        )
    )
    left join edw_company_dim as c
      on (
        (
          cast((
            c.co_cd
          ) as text) = cast((
            s.sls_org_co_cd
          ) as text)
        )
      )
  )
  left join itg_mds_ap_greenlight_skus as g
    on (
      (
        (
          case
            when (
              (
                upper(cast((
                  g.market
                ) as text)) = cast((
                  cast('CHINA OTC'  as varchar)
                ) as text)
              )
              or (
                (
                  upper(cast((
                    g.market
                  ) as text)) is null
                ) and (
                  'CHINA OTC' is null
                )
              )
            )
            then cast((
              cast('OTC' as varchar)
            ) as text)
            when (
              (
                upper(cast((
                  g.market
                ) as text)) = cast((
                  cast('CHINA SIKNCARE' as varchar)
                ) as text)
              )
              or (
                (
                  upper(cast((
                    g.market
                  ) as text)) is null
                )
                and (
                  'CHINA SIKNCARE' is null
                )
              )
            )
            then cast((
              cast('CHINA' as varchar)
            ) as text)
            else upper(cast((
              g.market
            ) as text))
          end = case
            when (
              (
                upper(cast((
                  c.ctry_group
                ) as text)) = cast((
                  cast('AUSTRALIA' as varchar)
                ) as text)
              )
              or (
                (
                  upper(cast((
                    c.ctry_group
                  ) as text)) is null
                )
                and (
                  'AUSTRALIA' is null
                )
              )
            )
            then cast((
              cast('PACIFIC' as varchar)
            ) as text)
            when (
              (
                upper(cast((
                  c.ctry_group
                ) as text)) = cast((
                  cast('NEW ZEALAND' as varchar)
                ) as text)
              )
              or (
                (
                  upper(cast((
                    c.ctry_group
                  ) as text)) is null
                )
                and (
                  'NEW ZEALAND' is null
                )
              )
            )
            then cast((
              cast('PACIFIC' as varchar)
            ) as text)
            else upper(cast((
              c.ctry_group
            ) as text))
          end
        )
        and (
          ltrim(
            cast((
              g.material_number
            ) as text),
            cast((
              cast((
                0
              ) as varchar)
            ) as text)
          ) = ltrim(ms.matl_num, cast((
            cast((
              0
            ) as varchar)
          ) as text))
        )
      )
    )
)
)

select * from final
