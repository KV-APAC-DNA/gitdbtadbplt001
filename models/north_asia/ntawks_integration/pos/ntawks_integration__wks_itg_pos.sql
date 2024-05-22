with wks_itg_pos_rank as (
    select * from snapntawks_integration.wks_itg_pos_rank
),
itg_pos_temp as 
(
    select * from {{ source('ntaitg_integration', 'itg_pos_temp') }}
),
final as
(
    select 
        pos_date AS pos_dt,
        customer_code_vend_cd AS vend_cd,
        NULL AS vend_nm,
        NULL AS prod_nm,
        product_code AS vend_prod_cd,
        product_name AS vend_prod_nm,
        NULL AS brnd_nm,
        barcode AS ean_num,
        store_code AS str_cd,
        store_name AS str_nm,
        number_of_sales AS sls_qty,
        sales_revenue AS sls_amt,
        unit_price AS unit_prc_amt,
        sales_rvenue_excl_vat AS sls_excl_vat_amt,
        0 AS stk_rtrn_amt,
        0 AS stk_recv_amt,
        0 AS avg_sell_qty,
        0 AS cum_ship_qty,
        0 AS cum_rtrn_qty,
        0 AS web_ordr_takn_qty,
        0 AS web_ordr_acpt_qty,
        0 AS dc_invnt_qty,
        0 AS invnt_qty,
        NULL AS invnt_amt,
        date_of_preparation AS invnt_dt,
        serial_num AS serial_num,
        NULL AS prod_delv_type,
        NULL AS prod_type,
        NULL AS dept_cd,
        NULL AS dept_nm,
        product_volume AS spec_1_desc,
        NULL AS spec_2_desc,
        NULL AS cat_big,
        NULL AS cat_mid,
        NULL AS cat_small,
        NULL AS dc_prod_cd,
        customer_code AS cust_dtls,
        distribution_code AS dist_cd,
        currency AS crncy_cd,
        product_status AS src_txn_sts,
        sl_no AS src_seq_num,
        SRC.src_sys_cd AS src_sys_cd,
        'KR' AS ctry_cd,
        NULL AS src_mesg_no,
        NULL AS src_mesg_code,
        NULL AS src_mesg_func_code,
        NULL AS src_mesg_date,
        NULL AS src_sale_date_form,
        NULL AS src_send_code,
        NULL AS src_send_ean_code,
        NULL AS src_send_name,
        NULL AS src_recv_qual,
        NULL AS src_recv_ean_code,
        NULL AS src_recv_name,
        NULL AS src_part_qual,
        NULL AS src_part_ean_code,
        NULL AS src_part_id,
        NULL AS src_part_name,
        NULL AS src_sender_id,
        NULL AS src_recv_date,
        NULL AS src_recv_time,
        NULL AS src_file_size,
        NULL AS src_file_path,
        NULL AS src_lega_tran,
        NULL AS src_regi_date,
        NULL AS src_line_no,
        NULL AS src_instore_code,
        NULL AS src_mnth_sale_amnt,
        NULL AS src_qty_unit,
        0 AS src_mnth_sale_qty,
        NULL as unit_of_pkg_sales,
        NULL as doc_send_date,
        NULL as unit_of_pkg_invt,
        NULL as doc_fun,
        NULL as doc_no,
        NULL as doc_fun_cd,
        NULL as buye_loc_cd,
        NULL as vend_loc_cd,
        NULL as provider_loc_cd,
        0 as comp_qty,
        NULL as unit_of_pkg_comp,
        0 as order_qty,
        NULL as unit_of_pkg_order,
        TGT.CRT_DTTM AS TGT_CRT_DTTM,
        upd_dttm as UPDT_DTTM,
        CASE
            WHEN TGT.CRT_DTTM IS NULL THEN 'I'
            ELSE 'U'
        END AS CHNG_FLG
    from 
    wks_itg_pos_rank src
    left outer join 
    (
        select pos_dt,
            ean_num,
            str_cd,
            src_sys_cd,
            ctry_cd,
            crt_dttm
        from itg_pos_temp
    ) tgt on src.pos_date = tgt.pos_dt
    and src.barcode = tgt.ean_num
    and src.store_code = tgt.str_cd
    and src.src_sys_cd = tgt.src_sys_cd
    AND 'KR' = tgt.ctry_cd
    where src.rnk = 1
)
select * from final