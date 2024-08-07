{{
    config(
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";"
    )
}}
with v_intrm_sls_pln_actl_tp as (
select * from {{ ref('ntaedw_integration__v_intrm_sls_pln_actl_tp') }}
),
v_intrm_sls_pln_actl_nts as (
select * from {{ ref('ntaedw_integration__v_intrm_sls_pln_actl_nts') }}
),
v_intrm_sls_pln_actl_gts as (
select * from {{ ref('ntaedw_integration__v_intrm_sls_pln_actl_gts') }}
),
final as( (
  SELECT 
    v_intrm_sls_pln_actl_gts.ctry_nm, 
    v_intrm_sls_pln_actl_gts.cust_num, 
    v_intrm_sls_pln_actl_gts.cust_nm, 
    v_intrm_sls_pln_actl_gts.matl_num, 
    v_intrm_sls_pln_actl_gts.matl_desc, 
    v_intrm_sls_pln_actl_gts.mega_brnd_desc, 
    v_intrm_sls_pln_actl_gts.brnd_desc, 
    v_intrm_sls_pln_actl_gts.prod_hier_l1, 
    v_intrm_sls_pln_actl_gts.prod_hier_l2, 
    v_intrm_sls_pln_actl_gts.prod_hier_l3, 
    v_intrm_sls_pln_actl_gts.prod_hier_l4, 
    v_intrm_sls_pln_actl_gts.prod_hier_l5, 
    v_intrm_sls_pln_actl_gts.prod_hier_l6, 
    v_intrm_sls_pln_actl_gts.prod_hier_l7, 
    v_intrm_sls_pln_actl_gts.prod_hier_l8, 
    v_intrm_sls_pln_actl_gts.prod_hier_l9, 
    v_intrm_sls_pln_actl_gts.cust_hier_l1, 
    v_intrm_sls_pln_actl_gts.cust_hier_l2, 
    v_intrm_sls_pln_actl_gts.cust_hier_l3, 
    v_intrm_sls_pln_actl_gts.cust_hier_l4, 
    v_intrm_sls_pln_actl_gts.cust_hier_l5, 
    v_intrm_sls_pln_actl_gts.sls_grp, 
    v_intrm_sls_pln_actl_gts.sls_ofc_desc, 
    v_intrm_sls_pln_actl_gts.channel, 
    v_intrm_sls_pln_actl_gts.store_type, 
    v_intrm_sls_pln_actl_gts.fisc_yr_per, 
    v_intrm_sls_pln_actl_gts.acct_hier_shrt_desc, 
    v_intrm_sls_pln_actl_gts.from_crncy, 
    v_intrm_sls_pln_actl_gts.to_crncy, 
    v_intrm_sls_pln_actl_gts.ex_rt_typ, 
    v_intrm_sls_pln_actl_gts.ex_rt, 
    v_intrm_sls_pln_actl_gts.copa_val, 
    v_intrm_sls_pln_actl_gts.net_bill_val, 
    v_intrm_sls_pln_actl_gts.ord_qty_pc, 
    v_intrm_sls_pln_actl_gts.timegone 
  FROM 
    v_intrm_sls_pln_actl_gts v_intrm_sls_pln_actl_gts 
  UNION ALL 
  SELECT 
    v_intrm_sls_pln_actl_nts.ctry_nm, 
    v_intrm_sls_pln_actl_nts.cust_num, 
    v_intrm_sls_pln_actl_nts.cust_nm, 
    v_intrm_sls_pln_actl_nts.matl_num, 
    v_intrm_sls_pln_actl_nts.matl_desc, 
    v_intrm_sls_pln_actl_nts.mega_brnd_desc, 
    v_intrm_sls_pln_actl_nts.brnd_desc, 
    v_intrm_sls_pln_actl_nts.prod_hier_l1, 
    v_intrm_sls_pln_actl_nts.prod_hier_l2, 
    v_intrm_sls_pln_actl_nts.prod_hier_l3, 
    v_intrm_sls_pln_actl_nts.prod_hier_l4, 
    v_intrm_sls_pln_actl_nts.prod_hier_l5, 
    v_intrm_sls_pln_actl_nts.prod_hier_l6, 
    v_intrm_sls_pln_actl_nts.prod_hier_l7, 
    v_intrm_sls_pln_actl_nts.prod_hier_l8, 
    v_intrm_sls_pln_actl_nts.prod_hier_l9, 
    v_intrm_sls_pln_actl_nts.cust_hier_l1, 
    v_intrm_sls_pln_actl_nts.cust_hier_l2, 
    v_intrm_sls_pln_actl_nts.cust_hier_l3, 
    v_intrm_sls_pln_actl_nts.cust_hier_l4, 
    v_intrm_sls_pln_actl_nts.cust_hier_l5, 
    v_intrm_sls_pln_actl_nts.sls_grp, 
    v_intrm_sls_pln_actl_nts.sls_ofc_desc, 
    v_intrm_sls_pln_actl_nts.channel, 
    v_intrm_sls_pln_actl_nts.store_type, 
    v_intrm_sls_pln_actl_nts.fisc_yr_per, 
    v_intrm_sls_pln_actl_nts.acct_hier_shrt_desc, 
    v_intrm_sls_pln_actl_nts.from_crncy, 
    v_intrm_sls_pln_actl_nts.to_crncy, 
    v_intrm_sls_pln_actl_nts.ex_rt_typ, 
    v_intrm_sls_pln_actl_nts.ex_rt, 
    v_intrm_sls_pln_actl_nts.copa_val, 
    v_intrm_sls_pln_actl_nts.net_bill_val, 
    v_intrm_sls_pln_actl_nts.ord_qty_pc, 
    v_intrm_sls_pln_actl_nts.timegone 
  FROM 
    v_intrm_sls_pln_actl_nts v_intrm_sls_pln_actl_nts
) 
UNION ALL 
SELECT 
  v_intrm_sls_pln_actl_tp.ctry_nm, 
  v_intrm_sls_pln_actl_tp.cust_num, 
  v_intrm_sls_pln_actl_tp.cust_nm, 
  v_intrm_sls_pln_actl_tp.matl_num, 
  v_intrm_sls_pln_actl_tp.matl_desc, 
  v_intrm_sls_pln_actl_tp.mega_brnd_desc, 
  v_intrm_sls_pln_actl_tp.brnd_desc, 
  v_intrm_sls_pln_actl_tp.prod_hier_l1, 
  v_intrm_sls_pln_actl_tp.prod_hier_l2, 
  v_intrm_sls_pln_actl_tp.prod_hier_l3, 
  v_intrm_sls_pln_actl_tp.prod_hier_l4, 
  v_intrm_sls_pln_actl_tp.prod_hier_l5, 
  v_intrm_sls_pln_actl_tp.prod_hier_l6, 
  v_intrm_sls_pln_actl_tp.prod_hier_l7, 
  v_intrm_sls_pln_actl_tp.prod_hier_l8, 
  v_intrm_sls_pln_actl_tp.prod_hier_l9, 
  v_intrm_sls_pln_actl_tp.cust_hier_l1, 
  v_intrm_sls_pln_actl_tp.cust_hier_l2, 
  v_intrm_sls_pln_actl_tp.cust_hier_l3, 
  v_intrm_sls_pln_actl_tp.cust_hier_l4, 
  v_intrm_sls_pln_actl_tp.cust_hier_l5, 
  v_intrm_sls_pln_actl_tp.sls_grp, 
  v_intrm_sls_pln_actl_tp.sls_ofc_desc, 
  v_intrm_sls_pln_actl_tp.channel, 
  v_intrm_sls_pln_actl_tp.store_type, 
  v_intrm_sls_pln_actl_tp.fisc_yr_per, 
  v_intrm_sls_pln_actl_tp.acct_hier_shrt_desc, 
  v_intrm_sls_pln_actl_tp.from_crncy, 
  v_intrm_sls_pln_actl_tp.to_crncy, 
  v_intrm_sls_pln_actl_tp.ex_rt_typ, 
  v_intrm_sls_pln_actl_tp.ex_rt, 
  v_intrm_sls_pln_actl_tp.copa_val, 
  v_intrm_sls_pln_actl_tp.net_bill_val, 
  v_intrm_sls_pln_actl_tp.ord_qty_pc, 
  v_intrm_sls_pln_actl_tp.timegone 
FROM 
  v_intrm_sls_pln_actl_tp v_intrm_sls_pln_actl_tp
)
select * from final 
