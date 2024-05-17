with sdl_tw_pos_px_civila as (
    select * from {{ source('ntasdl_raw', 'sdl_tw_pos_px_civila') }}
),
itg_pos as (
    select * from {{ source('ntaitg_integration', 'itg_pos_temp') }}
),
final as
(
    select 
        pos_date AS pos_dt,
        null AS vend_cd,
        null AS vend_nm,
        null AS prod_nm,
        civilian_product_code AS vend_prod_cd,
        product_name AS vend_prod_nm,
        brand AS brnd_nm,
        ean_code AS ean_num,
        store_code AS str_cd,
        store_name_chinese AS str_nm,
        Stock_selling_qty_by_store AS sls_qty,
        Stock_selling_amt_by_store AS sls_amt,
        unit_price AS unit_prc_amt,
        null AS sls_excl_vat_amt,
        Stock_return_amt_by_store AS stk_rtrn_amt,
        Stock_receive_amt_by_store AS stk_recv_amt,
        null AS avg_sell_qty,
        Stock_receive_qty_by_store AS cum_ship_qty,
        Stock_return_qty_by_store AS cum_rtrn_qty,
        null AS web_ordr_takn_qty,
        null AS web_ordr_acpt_qty,
        null AS dc_invnt_qty,
        Stock_inventory_qty AS invnt_qty,
        Stock_inventory_amt AS invnt_amt,
        null AS invnt_dt,
        null AS serial_num,
        null AS prod_delv_type,
        null AS prod_type,
        null AS dept_cd,
        null AS dept_nm,
        null AS spec_1_desc,
        null AS spec_2_desc,
        null AS cat_big,
        null AS cat_mid,
        null AS cat_small,
        dc AS dc_prod_cd,
        null AS cust_dtls,
        null AS dist_cd,
        'TWD' AS crncy_cd,
        null AS src_txn_sts,
        null AS src_seq_num,
        'PX 全聯' AS src_sys_cd,
        'TW' AS ctry_cd,
        TGT.CRT_DTTM AS TGT_CRT_DTTM,
        UPD_DTTM,
        CASE
            WHEN TGT.CRT_DTTM IS NULL THEN 'I'
            ELSE 'U'
        END AS CHNG_FLG
    FROM sdl_tw_pos_px_civila src
    LEFT OUTER JOIN 
    (
        SELECT pos_dt,
            vend_prod_cd,
            str_cd,
            src_sys_cd,
            ctry_cd,
            CRT_DTTM
        FROM ITG_POS
        where src_sys_cd = 'PX 全聯'
            and ctry_cd = 'TW'
            and pos_dt IS NOT NULL
    ) TGT ON SRC.pos_date = TGT.pos_dt
    AND SRC.civilian_product_code = TGT.vend_prod_cd
    AND SRC.store_code = TGT.str_cd
)
select * from final