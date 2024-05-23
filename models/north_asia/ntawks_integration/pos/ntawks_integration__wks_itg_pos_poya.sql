with sdl_tw_pos_poya as (
    select * from {{ source('ntasdl_raw', 'sdl_tw_pos_poya') }}
),
itg_pos as (
    select * from {{ source('ntaitg_integration', 'itg_pos_temp') }}
),
final AS 
(
    SELECT 
        start_date AS pos_dt,
        vendor_code AS vend_cd,
        NULL AS vend_nm,
        NULL AS prod_nm,
        customer_product_code AS vend_prod_cd,
        product_description AS vend_prod_nm,
        NULL AS brnd_nm,
        ean_code AS ean_num,
        NULL AS str_cd,
        NULL AS str_nm,
        selling_qty AS sls_qty,
        selling_amt AS sls_amt,
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
        inventory AS invnt_qty,
        NULL AS invnt_amt,
        NULL AS invnt_dt,
        NULL AS serial_num,
        NULL AS prod_delv_type,
        NULL AS prod_type,
        department AS dept_cd,
        NULL AS dept_nm,
        NULL AS spec_1_desc,
        NULL AS spec_2_desc,
        NULL AS cat_big,
        NULL AS cat_mid,
        category_small AS cat_small,
        NULL AS dc_prod_cd,
        NULL AS cust_dtls,
        Change_code AS dist_cd,
        'TWD' AS crncy_cd,
        NULL AS src_txn_sts,
        NULL AS src_seq_num,
        'Poya 寶雅' AS src_sys_cd,
        'TW' AS ctry_cd,
        TGT.CRT_DTTM AS TGT_CRT_DTTM,
        UPD_DTTM,
        CASE 
        WHEN TGT.CRT_DTTM IS NULL
            THEN 'I'
        ELSE 'U'
        END AS CHNG_FLG 
    FROM sdl_tw_pos_poya SRC 
    LEFT OUTER JOIN 
    (
        SELECT pos_dt,
        vend_prod_cd,
        ean_num,
        src_sys_cd,
        ctry_cd,
        CRT_DTTM
    FROM ITG_POS
    WHERE src_sys_cd = 'Poya 寶雅'
      AND ctry_cd = 'TW'
      AND pos_dt IS NOT NULL
    ) TGT ON SRC.start_date = TGT.pos_dt
    AND SRC.customer_product_code = TGT.vend_prod_cd
    ---AND SRC.ean_code=TGT. ean_num
)    
select * from final