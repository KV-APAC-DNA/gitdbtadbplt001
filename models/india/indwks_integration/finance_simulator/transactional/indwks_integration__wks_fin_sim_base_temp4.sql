{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook ="{% if is_incremental() %}
        DELETE FROM {{ref('indwks_integration__wks_fin_sim_base_temp1')}} where nature = 'FREE GOODS';
        DELETE FROM {{ref('indwks_integration__wks_fin_sim_base_temp2')}} where nature = 'FREE GOODS';
        DELETE FROM {{ref('indwks_integration__wks_fin_sim_base_temp3')}} where nature = 'FREE GOODS';
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
final as
(
    SELECT
      fg.matl_num,
      fg_copa.chrt_acct,
      fg_copa.acct_num,
      'NA' AS dstr_chnl,
      'NA' AS ctry_key,
      (fg.fisc_yr || fg.month)::INTEGER AS caln_yr_mo,
      fg.fisc_yr::INTEGER as fisc_yr,
      (fg.fisc_yr || 0 || fg.month)::INTEGER AS fisc_yr_per,
       TRY_CAST(
           DECODE(
               TRIM(REPLACE(REPLACE(REPLACE(amt_obj_crncy, ',', ''), '-', ''), '#N/A', '')),
               '','0',
               TRIM(REPLACE(REPLACE(REPLACE(amt_obj_crncy, ',', ''), '-', ''), '#N/A', ''))
           ) AS NUMERIC(38, 2)) AS amt_obj_crncy,
       TRY_CAST(
           DECODE(
               TRIM(REPLACE(REPLACE(REPLACE(REPLACE(qty, ',', ''), '-', ''), '(', '-'), ')', '')),
               '','0',
               TRIM(REPLACE(REPLACE(REPLACE(REPLACE(qty, ',', ''), '-', ''), '(', '-'), ')', ''))
           ) AS NUMERIC(38, 2)) AS qty,
      fg_copa.acct_hier_desc,
      fg_copa.acct_hier_shrt_desc,
      'NA' AS chnl_desc1,
      'GT' AS chnl_desc2,
      'NA' AS bw_gl,
      'FREE GOODS' AS nature,
      'NA' AS sap_gl,
      'NA' AS descp,
      'NA' AS bravo_mapping,
      prod_h.name AS sku_desc,
      fg.brand_combi,
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
      WHERE nature = 'FREE GOODS'
      ) fg
    LEFT JOIN itg_mds_in_product_hierarchy prod_h ON fg.matl_num = prod_h.code
    LEFT JOIN (
      SELECT matl_num,
        chrt_acct,
        acct_num,
        acct_hier_desc,
        acct_hier_shrt_desc
      FROM (select * from wks_fin_sim_base_temp3 
            union all
            select * from wks_fin_sim_base_temp2 
            union all
            select * from wks_fin_sim_base_temp1)
      WHERE bw_gl = 'CCOA/700508'
        AND acct_hier_shrt_desc = 'FG'
      GROUP BY 1,
        2,
        3,
        4,
        5
      ) fg_copa ON fg.matl_num = fg_copa.matl_num
)
select * from final