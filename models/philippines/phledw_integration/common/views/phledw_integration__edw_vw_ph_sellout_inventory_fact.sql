with itg_ph_dms_sellout_stock_fact as(
    select * from {{ ref('phlitg_integration__itg_ph_dms_sellout_stock_fact') }} 
),
transformed as(
    SELECT 'PH' AS cntry_cd
        ,'Philippines' AS cntry_nm
        ,itg_ph_dms_sellout_stock_fact.wh_cd AS warehse_cd
        ,itg_ph_dms_sellout_stock_fact.dstrbtr_grp_cd
        ,NULL AS dstrbtr_soldto_code
        ,itg_ph_dms_sellout_stock_fact.dstrbtr_prod_id AS dstrbtr_matl_num
        ,NULL AS sap_matl_num
        ,NULL AS bar_cd
        ,itg_ph_dms_sellout_stock_fact.inv_dt
        ,itg_ph_dms_sellout_stock_fact.qty AS soh
        ,itg_ph_dms_sellout_stock_fact.amt AS soh_val
        ,itg_ph_dms_sellout_stock_fact.amt AS jj_soh_val
        ,0 AS beg_stock_qty
        ,itg_ph_dms_sellout_stock_fact.qty AS end_stock_qty
        ,0 AS beg_stock_val
        ,itg_ph_dms_sellout_stock_fact.amt AS end_stock_val
        ,0 AS jj_beg_stock_qty
        ,0 AS jj_end_stock_qty
        ,0 AS jj_beg_stock_val
        ,0 AS jj_end_stock_val
	FROM itg_ph_dms_sellout_stock_fact
)
select * from transformed