with sdl_kr_pos_emart_evydy as 
(
    select * from {{ ref('ntawks_integration__wks_kr_pos_emart_evydy') }}
),
itg_pos_temp as 
(
    select * from {{ source('ntaitg_integration', 'itg_pos_temp') }}
),
final as
(
    SELECT 
        CAST(SALE_DATE AS DATE) AS pos_dt,
        ---new 
        mesg_from as mesg_from,
        NULL AS vend_cd,
        NULL AS vend_nm,
        NULL AS prod_nm,
        NULL AS vend_prod_cd,
        (prod_name1 || '' || prod_name2) AS vend_prod_nm,
        --new
        NULL AS brnd_nm,
        prod_code AS ean_num,
        ---new
        sale_id AS str_cd,
        --new
        sale_name AS str_nm,
        --new
        cast (sale_qty as integer) AS sls_qty,
        --new
        cast (sale_amnt as numeric(16, 5)) AS sls_amt,
        --new
        cast (unit_price as numeric(16, 5)) AS unit_prc_amt,
        --new 
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
        --new
        NULL as src_mesg_code,
        ---passing as null
        NULL as src_mesg_func_code,
        ---passing as null
        CAST(NULL as DATE) as src_mesg_date,
        ---passing as null
        NULL as src_sale_date_form,
        ---passing as null
        NULL as src_send_code,
        ---passing as null
        send_id as src_send_ean_code,
        --new
        send_name as src_send_name,
        --new
        NULL as src_recv_qual,
        ---passing as null
        recv_id as src_recv_ean_code,
        --new
        NULL as src_recv_name,
        ---passing as null
        NULL as src_part_qual,
        ---passing as null
        NULL as src_part_ean_code,
        ---passing as null
        NULL as src_part_id,
        ---passing as null
        NULL as src_part_name,
        ---passing as null
        NULL as src_sender_id,
        ---passing as null
        NULL as src_recv_date,
        ---passing as null
        NULL as src_recv_time,
        ---passing as null
        NULL as src_file_size,
        ---passing as null
        NULL as src_file_path,
        ---passing as null
        NULL as src_lega_tran,
        ---passing as null
        NULL as src_regi_date,
        ---passing as null
        NULL as src_line_no,
        ---passing as null
        NULL as src_instore_code,
        ---passing as null
        NULL as src_mnth_sale_amnt,
        ---passing as null
        NULL as src_qty_unit,
        ---passing as null
        NULL as src_mnth_sale_qty,
        ---passing as null
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
    FROM sdl_kr_pos_emart_evydy SRC
        LEFT OUTER JOIN 
        (
            SELECT pos_dt,
                ean_num,
                str_cd,
                src_sys_cd,
                ctry_cd,
                CRT_DTTM
            FROM itg_pos_temp
        ) TGT ON SRC.sale_date = TGT.pos_dt
        AND SRC.prod_code = TGT.ean_num
        AND SRC.sale_id = TGT.str_cd
        AND 'Emart' = TGT.src_sys_cd
        AND 'KR' = TGT.ctry_cd
    where mesg_from = 'RSHIN20'
)
select * from final