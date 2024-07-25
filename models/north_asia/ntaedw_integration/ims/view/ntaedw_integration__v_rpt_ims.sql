with 
v_rpt_ims_inventory_analysis as (
select * from {{ ref('ntaedw_integration__v_rpt_ims_inventory_analysis') }}
),
final as (
SELECT 
  v_rpt_ims_inventory_analysis.dstr_cd, 
  v_rpt_ims_inventory_analysis.dstr_nm, 
  v_rpt_ims_inventory_analysis.cust_cd, 
  v_rpt_ims_inventory_analysis.cust_nm, 
  v_rpt_ims_inventory_analysis.channel1, 
  v_rpt_ims_inventory_analysis.channel2, 
  v_rpt_ims_inventory_analysis.ims_txn_dt, 
  v_rpt_ims_inventory_analysis.fisc_per, 
  v_rpt_ims_inventory_analysis.fisc_wk, 
  v_rpt_ims_inventory_analysis.no_of_wks, 
  v_rpt_ims_inventory_analysis.fisc_wk_num, 
  v_rpt_ims_inventory_analysis.prod_cd, 
  v_rpt_ims_inventory_analysis.prod_nm, 
  v_rpt_ims_inventory_analysis.ean_num, 
  v_rpt_ims_inventory_analysis.sls_amt, 
  v_rpt_ims_inventory_analysis.sls_qty, 
  v_rpt_ims_inventory_analysis.rtrn_qty, 
  v_rpt_ims_inventory_analysis.rtrn_amt, 
  v_rpt_ims_inventory_analysis.sls_rep_nm, 
  v_rpt_ims_inventory_analysis.sls_rep_cd, 
  v_rpt_ims_inventory_analysis.ctry_nm, 
  v_rpt_ims_inventory_analysis.from_crncy, 
  v_rpt_ims_inventory_analysis.to_crncy, 
  v_rpt_ims_inventory_analysis.ex_rt, 
  v_rpt_ims_inventory_analysis.prod_hier_l1, 
  v_rpt_ims_inventory_analysis.prod_hier_l2, 
  v_rpt_ims_inventory_analysis.prod_hier_l3, 
  v_rpt_ims_inventory_analysis.prod_hier_l4, 
  v_rpt_ims_inventory_analysis.prod_hier_l5, 
  v_rpt_ims_inventory_analysis.prod_hier_l6, 
  v_rpt_ims_inventory_analysis.prod_hier_l7, 
  v_rpt_ims_inventory_analysis.prod_hier_l8, 
  v_rpt_ims_inventory_analysis.prod_hier_l9, 
  v_rpt_ims_inventory_analysis.sap_matl_num, 
  v_rpt_ims_inventory_analysis.lcl_prod_nm, 
  v_rpt_ims_inventory_analysis.sls_grp, 
  v_rpt_ims_inventory_analysis.store_typ, 
  v_rpt_ims_inventory_analysis.non_sellable_product 
FROM 
  v_rpt_ims_inventory_analysis 
WHERE 
  (
    "date_part"(
      year, 
      v_rpt_ims_inventory_analysis.ims_txn_dt
    ) > (
      "date_part"(
        year, 
        convert_timezone('UTC',current_timestamp()):: timestamp without time zone
      ) -3
    )
  )
)
select * from final