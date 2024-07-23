{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook ="{% if is_incremental() %}
        DELETE FROM {{this}} where nature = 'COGS' AND PLAN IS NOT NULL;
        {% endif %}"
    )
}}
with wks_fin_sim_base_temp1 as
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
wks_fin_sim_base_temp7 as
(
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp7') }}
),
wks_fin_sim_base_temp8 as
(
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp8') }}
),
wks_fin_sim_base_temp9 as
(
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp9') }}
),
wks_fin_sim_base_temp10 as
(
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp10') }}
),
wks_fin_sim_base_temp11 as
(
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp11') }}
),
wks_fin_sim_base_temp12 as
(
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp12') }}
),
wks_fin_sim_base_temp13 as
(
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp13') }}
),
itg_fin_sim_plandata as 
(
    select * from {{ ref('inditg_integration__itg_fin_sim_plandata') }}
),
itg_mds_in_product_hierarchy as
(
    select * from {{ ref('inditg_integration__itg_mds_in_product_hierarchy') }}
),
wks_fin_sim_base_temp14 as
(
    SELECT
       matl_num,
      'NA' AS chrt_acct,
      'NA' AS acct_num,
      'NA' AS dstr_chnl,
      'NA' AS ctry_key,
      (cogs.fisc_yr || cogs.month)::INTEGER AS caln_yr_mo,
      cogs.fisc_yr::INTEGER as fisc_yr,
      (cogs.fisc_yr || 0 || cogs.month)::INTEGER AS fisc_yr_per,
      cogs.amt_obj_crncy,
      cogs.qty,
      'NA' AS acct_hier_desc,
      'NA' AS acct_hier_shrt_desc,
      'NA' AS chnl_desc1,
      cogs.chnl_desc2,
      'NA' AS bw_gl,
      cogs.nature,
      'NA' AS sap_gl,
      'NA' AS descp,
      'NA' AS bravo_mapping,
      prod_h.name AS sku_desc,
      prod_h.brand_combi_code AS brand_combi,
      prod_h.franchise_code AS franchise,
      prod_h.group_code AS "group",
      CAST(NULL AS NUMERIC(38, 2)) as mrp,
      CAST(NULL AS NUMERIC(38, 2)) as cogs_per_unit,
      cogs.PLAN,
      prod_h.brand_group_1_code AS brand_group_1,
      prod_h.brand_group_2_code AS brand_group_2,
      NULL AS co_cd,
      prod_h.brand_combi_var_code AS brand_combi_var
    FROM (
      SELECT *
      FROM itg_fin_sim_plandata
      WHERE nature = 'COGS'
        AND PLAN IS NOT NULL
      ) cogs
    LEFT JOIN itg_mds_in_product_hierarchy prod_h ON COGS.matl_num = prod_h.code
),
transformed as 
(
    select * from wks_fin_sim_base_temp1
    union
    select * from wks_fin_sim_base_temp2
    union
    select * from wks_fin_sim_base_temp3
    union
    select * from wks_fin_sim_base_temp4
    union
    select * from wks_fin_sim_base_temp5
    union
    select * from wks_fin_sim_base_temp6
    union
    select * from wks_fin_sim_base_temp7
    union
    select * from wks_fin_sim_base_temp8
    union
    select * from wks_fin_sim_base_temp9
    union
    select * from wks_fin_sim_base_temp10
    union
    select * from wks_fin_sim_base_temp11
    union
    select * from wks_fin_sim_base_temp12
    union
    select * from wks_fin_sim_base_temp13
    union
    select * from wks_fin_sim_base_temp14
),
final as 
(
    select
        matl_num::varchar(100) as matl_num,
        chrt_acct::varchar(4) as chrt_acct,
        acct_num::varchar(10) as acct_num,
        dstr_chnl::varchar(2) as dstr_chnl,
        ctry_key::varchar(3) as ctry_key,
        caln_yr_mo::number(18,0) as caln_yr_mo,
        fisc_yr::number(18,0) as fisc_yr,
        fisc_yr_per::number(18,0) as fisc_yr_per,
        amt_obj_crncy::number(38,5) as amt_obj_crncy,
        qty::number(38,5) as qty,
        acct_hier_desc::varchar(100) as acct_hier_desc,
        acct_hier_shrt_desc::varchar(100) as acct_hier_shrt_desc,
        chnl_desc1::varchar(500) as chnl_desc1,
        chnl_desc2::varchar(500) as chnl_desc2,
        bw_gl::varchar(200) as bw_gl,
        nature::varchar(500) as nature,
        sap_gl::varchar(500) as sap_gl,
        descp::varchar(500) as descp,
        bravo_mapping::varchar(500) as bravo_mapping,
        sku_desc::varchar(500) as sku_desc,
        brand_combi::varchar(500) as brand_combi,
        franchise::varchar(500) as franchise,
        "group":: varchar(500) as "group",
        mrp::number(38,2) as mrp,
        cogs_per_unit::number(38,2) as cogs_per_unit,
        plan::varchar(10) as plan,
        brand_group_1::varchar(500) as brand_group_1,
        brand_group_2::varchar(500) as brand_group_2,
        co_cd::varchar(16777216) as co_cd,
        brand_combi_var::varchar(200) as brand_combi_var
    from transformed 
)
select * from final
