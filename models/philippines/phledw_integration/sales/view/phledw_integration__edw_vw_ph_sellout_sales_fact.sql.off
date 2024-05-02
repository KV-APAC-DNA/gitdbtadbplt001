with 
itg_ph_dms_sellout_sales_fact as
(
    select * from {{ ref('phlitg_integration__itg_ph_dms_sellout_sales_fact') }}
),
final as
(
    SELECT 'PH' AS cntry_cd,
        'Philippines' AS cntry_nm,
        itg_ph_dms_sellout_sales_fact.dstrbtr_grp_cd,
        NULL AS dstrbtr_soldto_code,
        itg_ph_dms_sellout_sales_fact.trnsfrm_cust_id AS cust_cd,
        itg_ph_dms_sellout_sales_fact.dstrbtr_prod_id AS dstrbtr_matl_num,
        NULL AS sap_matl_num,
        NULL AS bar_cd,
        itg_ph_dms_sellout_sales_fact.invoice_dt AS bill_date,
        itg_ph_dms_sellout_sales_fact.invoice_no AS bill_doc,
        itg_ph_dms_sellout_sales_fact.sls_rep_id AS slsmn_cd,
        itg_ph_dms_sellout_sales_fact.sls_rep_nm AS slsmn_nm,
        itg_ph_dms_sellout_sales_fact.wh_id,
        NULL AS doc_type,
        NULL AS doc_type_desc,
        0 AS base_sls,
        CASE
            WHEN (
                itg_ph_dms_sellout_sales_fact.qty >= (((0)::numeric)::numeric(18, 0))::numeric(15, 4)
            ) THEN itg_ph_dms_sellout_sales_fact.qty
            ELSE ((0)::numeric)::numeric(18, 0)
        END AS sls_qty,
        CASE
            WHEN (
                itg_ph_dms_sellout_sales_fact.qty < (((0)::numeric)::numeric(18, 0))::numeric(15, 4)
            ) THEN itg_ph_dms_sellout_sales_fact.qty
            ELSE ((0)::numeric)::numeric(18, 0)
        END AS ret_qty,
        itg_ph_dms_sellout_sales_fact.uom,
        CASE
            WHEN (
                itg_ph_dms_sellout_sales_fact.qty >= (((0)::numeric)::numeric(18, 0))::numeric(15, 4)
            ) THEN itg_ph_dms_sellout_sales_fact.qty
            ELSE ((0)::numeric)::numeric(18, 0)
        END AS sls_qty_pc,
        CASE
            WHEN (
                itg_ph_dms_sellout_sales_fact.qty < (((0)::numeric)::numeric(18, 0))::numeric(15, 4)
            ) THEN itg_ph_dms_sellout_sales_fact.qty
            ELSE ((0)::numeric)::numeric(18, 0)
        END AS ret_qty_pc,
        CASE
            WHEN (
                itg_ph_dms_sellout_sales_fact.gts_val >= (((0)::numeric)::numeric(18, 0))::numeric(15, 4)
            ) THEN itg_ph_dms_sellout_sales_fact.gts_val
            ELSE ((0)::numeric)::numeric(18, 0)
        END AS grs_trd_sls,
        CASE
            WHEN (
                itg_ph_dms_sellout_sales_fact.gts_val < (((0)::numeric)::numeric(18, 0))::numeric(15, 4)
            ) THEN itg_ph_dms_sellout_sales_fact.gts_val
            ELSE ((0)::numeric)::numeric(18, 0)
        END AS ret_val,
        itg_ph_dms_sellout_sales_fact.dscnt AS trd_discnt,
        NULL::integer AS trd_discnt_item_lvl,
        NULL::integer AS trd_discnt_bill_lvl,
        NULL::integer AS trd_sls,
        itg_ph_dms_sellout_sales_fact.nts_val AS net_trd_sls,
        NULL AS cn_reason_cd,
        NULL AS cn_reason_desc,
        CASE
            WHEN (
                itg_ph_dms_sellout_sales_fact.gts_val >= (((0)::numeric)::numeric(18, 0))::numeric(15, 4)
            ) THEN itg_ph_dms_sellout_sales_fact.gts_val
            ELSE ((0)::numeric)::numeric(18, 0)
        END AS jj_grs_trd_sls,
        CASE
            WHEN (
                itg_ph_dms_sellout_sales_fact.gts_val < (((0)::numeric)::numeric(18, 0))::numeric(15, 4)
            ) THEN itg_ph_dms_sellout_sales_fact.gts_val
            ELSE ((0)::numeric)::numeric(18, 0)
        END AS jj_ret_val,
        NULL::integer AS jj_trd_sls,
        itg_ph_dms_sellout_sales_fact.nts_val AS jj_net_trd_sls,
        itg_ph_dms_sellout_sales_fact.sls_rep_type,
        NULL AS store,
        NULL AS re_nm
    FROM itg_ph_dms_sellout_sales_fact
)
select * from final