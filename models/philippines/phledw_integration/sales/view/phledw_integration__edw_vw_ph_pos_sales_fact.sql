with
itg_ph_pos_sales_fact as
(
    select * from {{ ref('phlitg_integration__itg_ph_pos_sales_fact') }}
),
final as
(
SELECT 
    'PH' AS cntry_cd,
    'Philippines' AS cntry_nm,
    NULL AS pos_dt,
    NULL AS jj_yr_week_no,
    itg_ph_pos_sales_fact.jj_mnth_id,
    itg_ph_pos_sales_fact.cust_cd,
    itg_ph_pos_sales_fact.item_cd,
    NULL AS item_desc,
    NULL AS sap_matl_num,
    NULL AS bar_cd,
    NULL AS master_code,
    itg_ph_pos_sales_fact.brnch_cd AS cust_brnch_cd,
    itg_ph_pos_sales_fact.pos_qty,
    itg_ph_pos_sales_fact.pos_gts,
    itg_ph_pos_sales_fact.pos_item_prc,
    itg_ph_pos_sales_fact.pos_tax,
    itg_ph_pos_sales_fact.pos_nts,
    itg_ph_pos_sales_fact.conv_factor,
    itg_ph_pos_sales_fact.jj_qty_pc,
    itg_ph_pos_sales_fact.jj_item_prc_per_pc,
    itg_ph_pos_sales_fact.jj_gts,
    itg_ph_pos_sales_fact.jj_vat_amt,
    itg_ph_pos_sales_fact.jj_nts,
    NULL AS dept_cd
FROM itg_ph_pos_sales_fact
)
select * from final