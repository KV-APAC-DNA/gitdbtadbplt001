with wks_fin_sim_base_temp1 as
(
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp1') }}
),
final as
(
    SELECT
      matl_num,
      chrt_acct,
      acct_num,
      NULL as dstr_chnl,
      ctry_key,
      caln_yr_mo,
      fisc_yr,
      fisc_yr_per,
      SUM(amt_obj_crncy) AS amt_obj_crncy,
      SUM(qty) AS qty,
      acct_hier_desc,
      acct_hier_shrt_desc,
      NULL as chnl_desc1,
      'TOTAL' AS chnl_desc2,
      bw_gl,
      nature,
      sap_gl,
      descp,
      bravo_mapping,
      sku_desc,
      brand_combi,
      franchise,
      "group",
      mrp,
      cogs_per_unit,
      PLAN,
      brand_group_1,
      brand_group_2,
      co_cd,
      brand_combi_var
    FROM wks_fin_sim_base_temp1
    WHERE nature = 'CIW'
      AND chnl_desc2 IN ('GT', 'Exports', 'Govt')
      AND PLAN IS NULL
    --and fisc_yr_per ='2023006'
    GROUP BY matl_num,
      chrt_acct,
      acct_num,
      ctry_key,
      caln_yr_mo,
      fisc_yr,
      fisc_yr_per,
      acct_hier_desc,
      acct_hier_shrt_desc,
      bw_gl,
      nature,
      sap_gl,
      descp,
      bravo_mapping,
      sku_desc,
      brand_combi,
      franchise,
      "group",
      mrp,
      cogs_per_unit,
      PLAN,
      brand_group_1,
      brand_group_2,
      co_cd,
      brand_combi_var        
)
select * from final