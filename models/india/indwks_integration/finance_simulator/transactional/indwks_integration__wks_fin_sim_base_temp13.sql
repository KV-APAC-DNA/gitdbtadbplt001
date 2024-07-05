{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook ="{% if is_incremental() %}
        DELETE FROM {{this}} nature = 'GDS' AND PLAN IS NOT NULL;
        {% endif %}"
    )
}}
with itg_fin_sim_plandata as (
    select * from {{ ref('inditg_integration__itg_fin_sim_plandata') }}
),
itg_mds_in_product_hierarchy as
(
    select * from {{ ref('inditg_integration__itg_mds_in_product_hierarchy') }}
),
final as
(
    SELECT
       matl_num,
      'NA' AS chrt_acct,
      'NA' AS acct_num,
      'NA' AS dstr_chnl,
      'NA' AS ctry_key,
      (gds.fisc_yr || gds.month)::INTEGER AS caln_yr_mo,
      gds.fisc_yr::INTEGER as fisc_yr,
      (gds.fisc_yr || 0 || gds.month)::INTEGER AS fisc_yr_per,
      gds.amt_obj_crncy,
      gds.qty,
      'NA' AS acct_hier_desc,
      'NA' AS acct_hier_shrt_desc,
      'NA' AS chnl_desc1,
      gds.chnl_desc2,
      'NA' AS bw_gl,
      gds.nature,
      'NA' AS sap_gl,
      'NA' AS descp,
      'NA' AS bravo_mapping,
      prod_h.name AS sku_desc,
      prod_h.brand_combi_code AS brand_combi,
      prod_h.franchise_code AS franchise,
      prod_h.group_code AS "group",
      CAST(NULL AS NUMERIC(38, 2)) as mrp,
      CAST(NULL AS NUMERIC(38, 2)) as cogs_per_unit,
      gds.PLAN,
      prod_h.brand_group_1_code AS brand_group_1,
      prod_h.brand_group_2_code AS brand_group_2,
      NULL AS co_cd,
      prod_h.brand_combi_var_code AS brand_combi_var
    FROM (
      SELECT *
      FROM itg_fin_sim_plandata
      WHERE nature = 'GDS'
        AND PLAN IS NOT NULL
      ) gds
    LEFT JOIN itg_mds_in_product_hierarchy prod_h ON gds.matl_num = prod_h.code
)
select * from final