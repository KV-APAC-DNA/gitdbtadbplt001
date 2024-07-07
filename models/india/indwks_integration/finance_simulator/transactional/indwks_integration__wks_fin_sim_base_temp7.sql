{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook ="{% if is_incremental() %}
        DELETE FROM {{this}} where nature = 'NR' AND chnl_desc2 = 'Govt';
        {% endif %}"
    )
}}
with itg_fin_sim_miscdata as (
    select * from {{ ref('inditg_integration__itg_fin_sim_miscdata') }}
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
      (nrgvt.fisc_yr || nrgvt.month)::INTEGER AS caln_yr_mo,
      nrgvt.fisc_yr::INTEGER as fisc_yr,
      (nrgvt.fisc_yr || 0 || nrgvt.month)::INTEGER AS fisc_yr_per,
      DECODE(TRIM(REPLACE(REPLACE(REPLACE(amt_obj_crncy, ',', ''), '-', ''), '#N/A', '')), '', 0, CAST(TRIM(REPLACE(REPLACE(REPLACE(amt_obj_crncy, ',', ''), '-', ''), '#N/A', '')) AS NUMERIC(38, 2))) as amt_obj_crncy,
      CAST(NULL AS NUMERIC(38, 2)) as qty,
      'NA' AS acct_hier_desc,
      'NA' AS acct_hier_shrt_desc,
      'NA' AS chnl_desc1,
      'Govt' AS chnl_desc2,
      'NA' AS bw_gl,
      'NR' AS nature,
      'NA' AS sap_gl,
      'NA' AS descp,
      'NA' AS bravo_mapping,
      prod_h.name AS sku_desc,
      nrgvt.brand_combi,
      prod_h.franchise_code AS franchise,
      prod_h.group_code AS "group",
      CAST(NULL AS NUMERIC(38, 2)) as mrp,
      CAST(NULL AS NUMERIC(38, 2)) as cogs_per_unit,
      NULL AS PLAN,
      prod_h.brand_group_1_code AS brand_group_1,
      prod_h.brand_group_2_code AS brand_group_2,
      NULL AS co_cd,
      prod_h.brand_combi_var_code AS brand_combi_var
    FROM (
      SELECT *
      FROM itg_fin_sim_miscdata
      WHERE nature = 'NR'
        AND chnl_desc2 = 'Govt'
      ) nrgvt
    LEFT JOIN itg_mds_in_product_hierarchy prod_h ON nrgvt.matl_num = prod_h.code
)
select * from final