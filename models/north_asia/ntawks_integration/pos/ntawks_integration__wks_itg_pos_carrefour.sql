with source as (
    select * from {{ source('ntasdl_raw', 'sdl_tw_pos_carrefour') }}
     where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_tw_pos_carrefour__null_test') }}
        union all
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_tw_pos_carrefour__test_date_format_odd_eve_leap') }}
    )
    
),
itg_pos as (
    select * from {{ source('ntaitg_integration', 'itg_pos_temp') }}
),
final as
(
    select 
        pos_date AS pos_dt,
        vendor AS vend_cd,
        null AS vend_nm,
        product_name_english AS prod_nm,
        product_code AS vend_prod_cd,
        product_name_chinese AS vend_prod_nm,
        null AS brnd_nm,
        ean_code AS ean_num,
        store_no AS str_cd,
        store_name AS str_nm,
        qty AS sls_qty,
        amount AS sls_amt,
        null AS unit_prc_amt,
        null AS sls_excl_vat_amt,
        null AS stk_rtrn_amt,
        null AS stk_recv_amt,
        null AS avg_sell_qty,
        null AS cum_ship_qty,
        null AS cum_rtrn_qty,
        null AS web_ordr_takn_qty,
        null AS web_ordr_acpt_qty,
        null AS dc_invnt_qty,
        null AS invnt_qty,
        null AS invnt_amt,
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
        null AS dc_prod_cd,
        null AS cust_dtls,
        null AS dist_cd,
        'TWD' AS crncy_cd,
        null AS src_txn_sts,
        null AS src_seq_num,
        'Carrefour 家樂福' AS src_sys_cd,
        'TW' AS ctry_cd,
        TGT.CRT_DTTM AS TGT_CRT_DTTM,
        UPD_DTTM,
        CASE
            WHEN TGT.CRT_DTTM IS NULL THEN 'I'
            ELSE 'U'
        END AS CHNG_FLG
    FROM source SRC
    LEFT OUTER JOIN 
    (
        SELECT pos_dt,
            vend_prod_cd,
            ean_num,
            str_cd,
            src_sys_cd,
            ctry_cd,
            CRT_DTTM
        FROM ITG_POS
        where src_sys_cd = 'Carrefour 家樂福'
            and ctry_cd = 'TW'
    ) TGT ON SRC.pos_date = TGT.pos_dt
    AND SRC.product_code = TGT.vend_prod_cd
    AND SRC.store_no = TGT.str_cd
        
)
select * from final