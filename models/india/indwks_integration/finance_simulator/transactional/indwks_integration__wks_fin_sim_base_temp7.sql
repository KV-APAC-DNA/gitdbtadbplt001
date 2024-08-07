{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook ="{% if is_incremental() %}
        DELETE FROM {{ ref('indwks_integration__wks_fin_sim_base_temp1') }} where nature = 'NR' AND chnl_desc2 = 'Govt';
        DELETE FROM {{ ref('indwks_integration__wks_fin_sim_base_temp2') }} where nature = 'NR' AND chnl_desc2 = 'Govt';
        DELETE FROM {{ ref('indwks_integration__wks_fin_sim_base_temp3') }} where nature = 'NR' AND chnl_desc2 = 'Govt';
        DELETE FROM {{ ref('indwks_integration__wks_fin_sim_base_temp4') }} where nature = 'NR' AND chnl_desc2 = 'Govt';
        DELETE FROM {{ ref('indwks_integration__wks_fin_sim_base_temp5') }} where nature = 'NR' AND chnl_desc2 = 'Govt';
        DELETE FROM {{ ref('indwks_integration__wks_fin_sim_base_temp6') }} where nature = 'NR' AND chnl_desc2 = 'Govt';
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
wks_fin_sim_base_temp5 as
(
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp5') }}
),
wks_fin_sim_base_temp6 as
(
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp6') }}
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
      TRY_CAST(
          DECODE(
              TRIM(REPLACE(REPLACE(REPLACE(amt_obj_crncy, ',', ''), '-', ''), '#N/A', '')),
              '','0',
              TRIM(REPLACE(REPLACE(REPLACE(amt_obj_crncy, ',', ''), '-', ''), '#N/A', ''))
          ) AS NUMERIC(38, 2)) AS amt_obj_crncy,
      CAST(NULL AS NUMERIC(38, 2)) AS qty,
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