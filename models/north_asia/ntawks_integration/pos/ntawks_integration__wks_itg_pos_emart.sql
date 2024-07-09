with sdl_kr_pos_emart as 
(
    select * from {{ ref('ntawks_integration__wks_kr_pos_emart') }}  
),
itg_pos_temp as 
(
    select * from {{ source('ntaitg_integration','itg_pos_temp') }}
),
final as 
(
    SELECT 
        sale_date AS pos_dt,
        NULL AS vend_cd,
        NULL AS vend_nm,
        NULL AS prod_nm,
        NULL AS vend_prod_cd,
        (sku_name1 || '' || sku_name2) AS vend_prod_nm,
        NULL AS brnd_nm,
        sku_code AS ean_num,
        store_code AS str_cd,
        store_name AS str_nm,
        cast (day_sale_qty as integer) AS sls_qty,
        cast (day_sale_amnt as numeric(16, 5)) AS sls_amt,
        cast (unit_prce as numeric(16, 5)) AS unit_prc_amt,
        0 AS sls_excl_vat_amt,
        0 AS stk_rtrn_amt,
        0 AS stk_recv_amt,
        0 AS avg_sell_qty,
        0 AS cum_ship_qty,
        0 AS cum_rtrn_qty,
        0 AS web_ordr_takn_qty,
        0 AS web_ordr_acpt_qty,
        0 AS dc_invnt_qty,
        0 AS invnt_qty,
        0 AS invnt_amt,
        NULL AS invnt_dt,
        NULL AS serial_num,
        NULL AS prod_delv_type,
        NULL AS prod_type,
        NULL AS dept_cd,
        NULL AS dept_nm,
        NULL AS spec_1_desc,
        NULL AS spec_2_desc,
        NULL AS cat_big,
        NULL AS cat_mid,
        NULL AS cat_small,
        NULL AS dc_prod_cd,
        NULL AS cust_dtls,
        NULL AS dist_cd,
        'KRW' AS crncy_cd,
        NULL AS src_txn_sts,
        NULL AS src_seq_num,
        'Emart' AS src_sys_cd,
        'KR' AS ctry_cd,
        mesg_no as src_mesg_no,
        mesg_code as src_mesg_code,
        mesg_func_code as src_mesg_func_code,
        mesg_date as src_mesg_date,
        sale_date_form as src_sale_date_form,
        send_code as src_send_code,
        send_ean_code as src_send_ean_code,
        send_name as src_send_name,
        recv_qual as src_recv_qual,
        recv_ean_code as src_recv_ean_code,
        recv_name as src_recv_name,
        part_qual as src_part_qual,
        part_ean_code as src_part_ean_code,
        part_id as src_part_id,
        part_name as src_part_name,
        sender_id as src_sender_id,
        recv_date as src_recv_date,
        recv_time as src_recv_time,
        file_size as src_file_size,
        file_path as src_file_path,
        lega_tran as src_lega_tran,
        regi_date as src_regi_date,
        line_no as src_line_no,
        instore_code as src_instore_code,
        mnth_sale_amnt as src_mnth_sale_amnt,
        qty_unit as src_qty_unit,
        mnth_sale_qty as src_mnth_sale_qty,
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
        UPDT_DTTM,
        CASE
            WHEN TGT.CRT_DTTM IS NULL THEN 'I'
            ELSE 'U'
        END AS CHNG_FLG
    FROM sdl_kr_pos_emart SRC
        LEFT OUTER JOIN (
            SELECT pos_dt,
                ean_num,
                str_cd,
                src_sys_cd,
                ctry_cd,
                CRT_DTTM
            FROM itg_pos_temp
        ) TGT ON SRC.sale_date = TGT.pos_dt
        AND SRC.sku_code = TGT.ean_num
        AND SRC.store_code = TGT.str_cd
        AND 'Emart' = TGT.src_sys_cd
        AND 'KR' = TGT.ctry_cd
)
select * from final