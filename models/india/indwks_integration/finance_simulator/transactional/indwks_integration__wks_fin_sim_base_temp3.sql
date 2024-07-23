{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook ="{% if is_incremental() %}
        DELETE FROM {{this}} WHERE nature = 'PRN';
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
wks_fin_sim_base_temp2 as (
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp2') }}
),
final as
(
    SELECT
      prn.matl_num,
      prn_copa.chrt_acct,
      prn_copa.acct_num,
      'NA' AS dstr_chnl,
      'NA' AS ctry_key,
      (prn.fisc_yr || prn.month) AS caln_yr_mo,
      prn.fisc_yr as fisc_yr,
      (prn.fisc_yr || 0 || prn.month) AS fisc_yr_per,
      TRY_CAST(
          DECODE(
              TRIM(REPLACE(REPLACE(REPLACE(amt_obj_crncy, ',', ''), '-', ''), '#N/A', '')),
              '','0',
              TRIM(REPLACE(REPLACE(REPLACE(amt_obj_crncy, ',', ''), '-', ''), '#N/A', ''))
          ) AS NUMERIC(38, 2)) AS amt_obj_crncy,
      TRY_CAST(TRIM(REPLACE(REPLACE(qty, ',', ''), '-', '0')) AS NUMERIC(38, 2)) AS qty,
      prn_copa.acct_hier_desc,
      prn_copa.acct_hier_shrt_desc,
      'NA' AS chnl_desc1,
      'GT' AS chnl_desc2,
      'NA' AS bw_gl,
      'PRN' AS nature,
      'NA' AS sap_gl,
      'NA' AS descp,
      'NA' AS bravo_mapping,
      prod_h.name AS sku_desc,
      prn.brand_combi,
      prod_h.franchise_code AS franchise,
      prod_h.group_code AS "group",
      NULL as mrp,
      NULL as cogs_per_unit,
      NULL AS PLAN,
      prod_h.brand_group_1_code AS brand_group_1,
      prod_h.brand_group_2_code AS brand_group_2,
      NULL AS co_cd,
      prod_h.brand_combi_var_code AS brand_combi_var
    FROM (
      SELECT *
      FROM itg_fin_sim_miscdata
      WHERE nature = 'PRN'
      ) prn
    LEFT JOIN itg_mds_in_product_hierarchy prod_h ON prn.matl_num::varchar(20) = prod_h.code::varchar(20)
    LEFT JOIN (
      SELECT matl_num,
        chrt_acct,
        acct_num,
        acct_hier_desc,
        acct_hier_shrt_desc
      FROM wks_fin_sim_base_temp2
      WHERE bw_gl = 'CCOA/700516'
      GROUP BY 1,
        2,
        3,
        4,
        5
      ) prn_copa ON prn.matl_num::varchar(20) = prn_copa.matl_num::varchar(20)
)
select * from final