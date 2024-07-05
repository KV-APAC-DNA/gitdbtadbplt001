with wks_fin_sim_copa_trans_fact as
(
    select * from {{ ref('indwks_integration__wks_fin_sim_copa_trans_fact') }}
),
itg_mds_in_product_hierarchy as
(
    select * from {{ ref('inditg_integration__itg_mds_in_product_hierarchy') }}
),
itg_mds_in_gl_account_master as
(
    select * from {{ ref('inditg_integration__itg_mds_in_gl_account_master') }}
),
itg_mds_in_sap_distribution_channel as
(
    select * from {{ ref('inditg_integration__itg_mds_in_sap_distribution_channel') }}
),
final as
(
    SELECT
    LTRIM(trans.matl_num, 0) AS matl_num,
    trans.chrt_acct,
    trans.acct_num,
    trans.dstr_chnl,
    trans.ctry_key,
    trans.caln_yr_mo,
    trans.fisc_yr,
    trans.fisc_yr_per,
    CASE 
      WHEN UPPER(acc_m.nature_code) = 'CIW'
        THEN trans.amt_obj_crncy * (- 1)
      WHEN UPPER(acc_m.nature_code) = 'GDS'
        AND UPPER(trans.acct_hier_shrt_desc) = 'RTN'
        THEN trans.amt_obj_crncy * (- 1)
      WHEN UPPER(acc_m.nature_code) = 'COGS'
        AND UPPER(trans.acct_hier_shrt_desc) = 'FGC'
        THEN 0
      ELSE trans.amt_obj_crncy
      END AS amt_obj_crncy,
    CASE 
      WHEN UPPER(acc_m.nature_code) = 'GDS'
        THEN (
            CASE 
              WHEN UPPER(trans.acct_hier_shrt_desc) = 'RTN'
                THEN trans.qty * (- 1)
              ELSE trans.qty
              END
            )
      ELSE 0
      END AS qty,
    trans.acct_hier_desc,
    trans.acct_hier_shrt_desc,
    ch_m.name AS chnl_desc1,
    CASE 
      WHEN ch_m.chnl_desc2_code IS NULL
        THEN 'NULL'
      ELSE ch_m.chnl_desc2_code
      END AS chnl_desc2,
    acc_m.bw_gl,
    acc_m.nature_code AS nature,
    acc_m.code AS sap_gl,
    acc_m.name AS descp,
    acc_m.bravo_mapping_code AS bravo_mapping,
    prod_h.name AS sku_desc,
    prod_h.brand_combi_code AS brand_combi,
    prod_h.franchise_code AS franchise,
    prod_h.group_code AS "group",
    CAST(NULL AS NUMERIC(38, 2)) AS mrp,
    CAST(NULL AS NUMERIC(38, 2)) AS cogs_per_unit,
    CAST(NULL AS VARCHAR(10)) AS PLAN,
    prod_h.brand_group_1_code AS brand_group_1,
    prod_h.brand_group_2_code AS brand_group_2,
    trans.co_cd,
    prod_h.brand_combi_var_code AS brand_combi_var FROM wks_fin_sim_copa_trans_fact trans LEFT JOIN itg_mds_in_product_hierarchy prod_h ON prod_h.code = LTRIM(trans.matl_num, 0) LEFT JOIN itg_mds_in_gl_account_master acc_m ON trans.chrt_acct || '\/' || LTRIM(trans.acct_num, 0) = acc_m.bw_gl LEFT JOIN (
    SELECT CASE 
        WHEN code = 'Not assigned'
          THEN NULL
        ELSE code
        END AS code,
      name,
      chnl_desc2_code
    FROM itg_mds_in_sap_distribution_channel
    ) ch_m ON NVL(ch_m.code, '') = NVL(trans.dstr_chnl, '') GROUP BY 1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16,
    17,
    18,
    19,
    20,
    21,
    22,
    23,
    24,
    25,
    26,
    27,
    28,
    29,
    30
)
select * from final