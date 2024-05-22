with sdl_kr_pos_costco_invrpt as(
    select *, dense_rank() over(partition by null order by filename desc) as rnk from {{ source('ntasdl_raw', 'sdl_kr_pos_costco_invrpt') }}
),
itg_pos_temp as 
(
    select * from {{ source('ntaitg_integration', 'itg_pos_temp') }}
),
final as
(
    SELECT 
        src.pos_dt AS pos_dt,
        NULL AS vend_cd,
        NULL AS vend_nm,
        NULL AS prod_nm,
        NULL AS vend_prod_cd,
        NULL AS vend_prod_nm,
        NULL AS brnd_nm,
        EAN_CD AS ean_num,
        STORE_CD AS str_cd,
        NULL AS str_nm,
        SALES_QTY AS sls_qty,
        NULL AS sls_amt,
        NULL AS unit_prc_amt,
        NULL AS sls_excl_vat_amt,
        NULL AS stk_rtrn_amt,
        NULL AS stk_recv_amt,
        NULL AS avg_sell_qty,
        NULL AS cum_ship_qty,
        NULL AS cum_rtrn_qty,
        NULL AS web_ordr_takn_qty,
        NULL AS web_ordr_acpt_qty,
        NULL AS dc_invnt_qty,
        INVT_QTY AS invnt_qty,
        NULL AS invnt_amt,
        src.pos_dt AS invnt_dt,
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
        'Costco' AS src_sys_cd,
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
        NULL AS src_mnth_sale_qty,
        UNIT_OF_PKG_SALES as SRC_UNIT_OF_PKG_SALES,
        DOC_SEND_DATE AS SRC_DOC_SEND_DATE,
        UNIT_OF_PKG_INVT AS SRC_UNIT_OF_PKG_INVT,
        DOC_FUN AS SRC_DOC_FUN,
        DOC_NO AS SRC_DOC_NO,
        DOC_FUN_CD AS SRC_DOC_FUN_CD,
        BUYE_LOC_CD AS SRC_BUYE_LOC_CD,
        VEND_LOC_CD AS SRC_VEND_LOC_CD,
        PROVIDER_LOC_CD AS SRC_PROVIDER_LOC_CD,
        COMP_QTY AS SRC_COMP_QTY,
        UNIT_OF_PKG_COMP AS SRC_UNIT_OF_PKG_COMP,
        ORDER_QTY AS SRC_ORDER_QTY,
        UNIT_OF_PKG_ORDER AS SRC_UNIT_OF_PKG_ORDER,
        TGT.CRT_DTTM AS TGT_CRT_DTTM,
        UPDT_DTTM,
        CASE
            WHEN TGT.CRT_DTTM IS NULL THEN 'I'
            ELSE 'U'
        END AS CHNG_FLG
    FROM --SDL_KR_POS_COSTCO_INVRPT SRC
    (
        select * from SDL_KR_POS_COSTCO_INVRPT where rnk = 1
    ) SRC
    LEFT OUTER JOIN 
    (
        SELECT pos_dt,
            ean_num,
            str_cd,
            src_sys_cd,
            ctry_cd,
            CRT_DTTM
        FROM itg_pos_temp
    ) TGT ON CAST(src.pos_dt AS DATE) = TGT.pos_dt
    AND SRC.EAN_CD = TGT.ean_num
    AND SRC.STORE_CD = TGT.str_cd
    AND 'Costco' = TGT.src_sys_cd
    AND 'KR' = TGT.ctry_cd
)
select * from final