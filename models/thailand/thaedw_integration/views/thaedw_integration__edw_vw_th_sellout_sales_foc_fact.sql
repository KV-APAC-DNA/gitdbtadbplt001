with itg_th_dstrbtr_customer_dim as (
select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_TH_DSTRBTR_CUSTOMER_DIM
),
itg_th_sellout_sales_foc_fact as (
select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_TH_SELLOUT_SALES_FOC_FACT
),
itg_th_dstrbtr_material_dim as (
select * from DEV_DNA_CORE.SNAPOSEITG_INTEGRATION.ITG_TH_DSTRBTR_material_DIM
),
final as (
SELECT 'TH' AS cntry_cd,
    'Thailand' AS cntry_nm,
    s.dstrbtr_id AS dstrbtr_grp_cd,
    NULL AS dstrbtr_soldto_code,
    (
        ltrim((s.ar_cd)::text, ((0)::character varying)::text)
    )::character varying AS cust_cd,
    (
        ltrim(
            (s.prod_cd)::text,
            ((0)::character varying)::text
        )
    )::character varying AS dstrbtr_matl_num,
    s.product_name1,
    s.product_name2,
    NULL AS sap_matl_num,
    NULL AS bar_cd,
    s.order_dt AS bill_date,
    s.order_no,
    s.iscancel,
    s.grp_cd,
    s.prom_cd1,
    s.prom_cd2,
    s.prom_cd3,
    (
        ltrim(
            (s.order_no)::text,
            ((0)::character varying)::text
        )
    )::character varying AS bill_doc,
    c.sls_emp AS slsmn_cd,
    c.sls_nm AS slsmn_nm,
    NULL AS wh_id,
    NULL AS doc_type,
    NULL AS doc_type_desc,
    0 AS base_sls,
    CASE
        WHEN (
            s.qty >= (((0)::numeric)::numeric(18, 0))::numeric(19, 6)
        ) THEN s.qty
        ELSE ((0)::numeric)::numeric(18, 0)
    END AS sls_qty,
    CASE
        WHEN (
            s.qty < (((0)::numeric)::numeric(18, 0))::numeric(19, 6)
        ) THEN s.qty
        ELSE ((0)::numeric)::numeric(18, 0)
    END AS ret_qty,
    'DZ' AS uom,
    CASE
        WHEN (
            s.qty >= (((0)::numeric)::numeric(18, 0))::numeric(19, 6)
        ) THEN (s.qty * ((12)::numeric)::numeric(18, 0))
        ELSE ((0)::numeric)::numeric(18, 0)
    END AS sls_qty_pc,
    CASE
        WHEN (
            s.qty < (((0)::numeric)::numeric(18, 0))::numeric(19, 6)
        ) THEN (s.qty * ((12)::numeric)::numeric(18, 0))
        ELSE ((0)::numeric)::numeric(18, 0)
    END AS ret_qty_pc,
    CASE
        WHEN (
            s.qty >= (((0)::numeric)::numeric(18, 0))::numeric(19, 6)
        ) THEN (s.qty * m.sls_prc_credit)
        ELSE ((0)::numeric)::numeric(18, 0)
    END AS grs_trd_sls,
    CASE
        WHEN (
            s.qty < (((0)::numeric)::numeric(18, 0))::numeric(19, 6)
        ) THEN (s.qty * m.sls_prc_credit)
        ELSE ((0)::numeric)::numeric(18, 0)
    END AS ret_val,
    (s.discnt + s.discnt_bt_ln) AS trd_discnt,
    s.discnt AS trd_discnt_item_lvl,
    s.grs_prc,
    s.discnt_bt_ln AS trd_discnt_bill_lvl,
    NULL::integer AS trd_sls,
    s.total_bfr_vat AS net_trd_sls,
    s.subamt1 AS tot_bf_discount,
    s.cn_reason_cd,
    s.cn_reason_en_desc AS cn_reason_desc,
    CASE
        WHEN (
            s.qty >= (((0)::numeric)::numeric(18, 0))::numeric(19, 6)
        ) THEN (s.qty * m.sls_prc_credit)
        ELSE ((0)::numeric)::numeric(18, 0)
    END AS jj_grs_trd_sls,
    CASE
        WHEN (
            s.qty < (((0)::numeric)::numeric(18, 0))::numeric(19, 6)
        ) THEN (s.qty * m.sls_prc_credit)
        ELSE ((0)::numeric)::numeric(18, 0)
    END AS jj_ret_val,
    NULL::integer AS jj_trd_sls,
    s.total_bfr_vat AS jj_net_trd_sls,
    s.subamt1 AS gross_sales
FROM (
        (
            itg_th_sellout_sales_foc_fact s
            LEFT JOIN itg_th_dstrbtr_material_dim m ON (
                (
                    ltrim(
                        (s.prod_cd)::text,
                        ((0)::character varying)::text
                    ) = ltrim(
                        (m.item_cd)::text,
                        ((0)::character varying)::text
                    )
                )
            )
        )
        LEFT JOIN (
            SELECT itg_th_dstrbtr_customer_dim.old_cust_id,
                itg_th_dstrbtr_customer_dim.dstrbtr_id,
                itg_th_dstrbtr_customer_dim.sls_emp,
                itg_th_dstrbtr_customer_dim.sls_nm,
                row_number() OVER(
                    PARTITION BY itg_th_dstrbtr_customer_dim.old_cust_id,
                    itg_th_dstrbtr_customer_dim.dstrbtr_id
                    ORDER BY itg_th_dstrbtr_customer_dim.old_cust_id,
                        itg_th_dstrbtr_customer_dim.dstrbtr_id
                ) AS rn
            FROM itg_th_dstrbtr_customer_dim
        ) c ON (
            (
                (
                    (c.rn = (1)::bigint)
                    AND (
                        trim((c.old_cust_id)::text) = trim((s.ar_cd)::text)
                    )
                )
                AND (
                    trim((c.dstrbtr_id)::text) = trim((s.dstrbtr_id)::text)
                )
            )
        )
    )
)
select * from final
