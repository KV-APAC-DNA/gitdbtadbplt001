with source as 
(
    select * from {{ ref('indedw_integration__edw_rpt_finance_simulator') }}
)
select
    matl_num as "matl_num",
    chrt_acct as "chrt_acct",
    acct_num as "acct_num",
    dstr_chnl as "dstr_chnl",
    ctry_key as "ctry_key",
    caln_yr_mo as "caln_yr_mo",
    fisc_yr as "fisc_yr",
    fisc_yr_per as "fisc_yr_per",
    amt_obj_crncy as "amt_obj_crncy",
    qty as "qty",
    acct_hier_desc as "acct_hier_desc",
    acct_hier_shrt_desc as "acct_hier_shrt_desc",
    chnl_desc1 as "chnl_desc1",
    chnl_desc2 as "chnl_desc2",
    bw_gl as "bw_gl",
    nature as "nature",
    sap_gl as "sap_gl",
    descp as "descp",
    bravo_mapping as "bravo_mapping",
    sku_desc as "sku_desc",
    brand_combi as "brand_combi",
    franchise as "franchise",
    "group" as "group",
    mrp as "mrp",
    cogs_per_unit as "cogs_per_unit",
    plan as "plan",
    brand_group_1 as "brand_group_1",
    brand_group_2 as "brand_group_2",
    co_cd as "co_cd",
    brand_combi_var as "brand_combi_var"
from source
