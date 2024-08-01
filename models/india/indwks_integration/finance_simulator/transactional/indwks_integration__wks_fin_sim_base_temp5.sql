{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook ="{% if is_incremental() %}
        DELETE FROM {{ ref('indwks_integration__wks_fin_sim_base_temp1') }} where nature = 'MRP ACT';
        DELETE FROM {{ ref('indwks_integration__wks_fin_sim_base_temp2') }} where nature = 'MRP ACT';
        DELETE FROM {{ ref('indwks_integration__wks_fin_sim_base_temp3') }} where nature = 'MRP ACT';
        DELETE FROM {{ ref('indwks_integration__wks_fin_sim_base_temp4') }} where nature = 'MRP ACT';
        {% endif %}"
    )
}}
with itg_fin_sim_miscdata as 
(
    select * from {{ ref('inditg_integration__itg_fin_sim_miscdata') }}
),
itg_mds_in_product_hierarchy as
(
    select * from {{ ref('inditg_integration__itg_mds_in_product_hierarchy') }}
),
wks_fin_sim_base_temp1 as 
(
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp1') }}
),
wks_fin_sim_base_temp2 as 
(
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp2') }}
),
wks_fin_sim_base_temp3 as 
(
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp3') }}
),
wks_fin_sim_base_temp4 as
(
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp4') }}
),
final as
(
    SELECT
       matl_num,
      'NA' AS chrt_acct,
      'NA' AS acct_num,
      'NA' AS dstr_chnl,
      'NA' AS ctry_key,
      (mrp.fisc_yr || mrp.month)::INTEGER AS caln_yr_mo,
      mrp.fisc_yr::INTEGER as fisc_yr,
      (mrp.fisc_yr || 0 || mrp.month)::INTEGER AS fisc_yr_per,
      TRY_CAST(
          DECODE(
              TRIM(REPLACE(REPLACE(REPLACE(amt_obj_crncy, ',', ''), '-', ''), '#N/A', '')),
              '','0',
              TRIM(REPLACE(REPLACE(REPLACE(amt_obj_crncy, ',', ''), '-', ''), '#N/A', ''))
          ) AS NUMERIC(38, 2)) AS amt_obj_crncy,
      TRY_CAST(
          TRIM(REPLACE(REPLACE(qty, ',', ''), '-', '0')) AS NUMERIC(38, 2)
          ) AS qty,
      'NA' AS acct_hier_desc,
      'NA' AS acct_hier_shrt_desc,
      'NA' AS chnl_desc1,
      'NA' AS chnl_desc2,
      'NA' AS bw_gl,
      'MRP ACT' AS nature,
      'NA' AS sap_gl,
      'NA' AS descp,
      'NA' AS bravo_mapping,
      prod_h.name AS sku_desc,
      mrp.brand_combi,
      prod_h.franchise_code AS franchise,
      prod_h.group_code AS "group",
      TRY_CAST(
          DECODE(
              TRIM(REPLACE(REPLACE(amt_obj_crncy, ',', ''), '-', '')),
              '','0',
              TRIM(REPLACE(REPLACE(amt_obj_crncy, ',', ''), '-', ''))
          ) AS NUMERIC(38, 2)) AS mrp,
      CAST(NULL AS NUMERIC(38,2)) AS cogs_per_unit,
      NULL AS PLAN,
      prod_h.brand_group_1_code AS brand_group_1,
      prod_h.brand_group_2_code AS brand_group_2,
      NULL AS co_cd,
      prod_h.brand_combi_var_code AS brand_combi_var
    FROM (
      SELECT *
      FROM itg_fin_sim_miscdata
      WHERE trim(nature) = 'MRP ACT'
      ) mrp
    LEFT JOIN itg_MDS_IN_Product_Hierarchy prod_h ON mrp.matl_num = prod_h.code
)
select * from final