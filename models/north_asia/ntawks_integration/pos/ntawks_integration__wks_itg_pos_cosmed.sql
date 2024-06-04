with sdl_tw_pos_cosmed as (
    select * from {{ source('ntasdl_raw', 'sdl_tw_pos_cosmed') }}
),
itg_pos as (
    select * from {{ source('ntaitg_integration', 'itg_pos_temp') }}
),
final as
(
    select 
        SRC.strt_dt AS pos_dt,
        null AS vend_cd,
        null AS vend_nm,
        null AS prod_nm,
        SRC.product_code AS vend_prod_cd,
        SRC.product_name AS vend_prod_nm,
        null AS brnd_nm,
        null AS ean_num,
        null AS str_cd,
        null AS str_nm,
        sales_quantity AS sls_qty,
        null AS sls_amt,
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
        'Cosmed 康是美' AS src_sys_cd,
        'TW' AS ctry_cd,
        TGT.CRT_DTTM AS TGT_CRT_DTTM,
        null as UPD_DTTM,
        CASE
            WHEN TGT.CRT_DTTM IS NULL THEN 'I'
            ELSE 'U'
        END AS CHNG_FLG
    FROM ----NA_SDL.sdl_tw_pos_cosmed SRC
        (
            select product_code,
                product_name,
                wk1_strt_dt as strt_dt,
                wk1_qty as sales_quantity
            from sdl_tw_pos_cosmed
            union all
            select product_code,
                product_name,
                wk2_strt_dt as strt_dt,
                wk2_qty as sales_quantity
            from sdl_tw_pos_cosmed
            union all
            select product_code,
                product_name,
                wk3_strt_dt as strt_dt,
                wk3_qty as sales_quantity
            from sdl_tw_pos_cosmed
            union all
            select product_code,
                product_name,
                wk4_strt_dt as strt_dt,
                wk4_qty as sales_quantity
            from sdl_tw_pos_cosmed
        ) SRC
        left outer join 
        (
            SELECT pos_dt,
                vend_prod_cd,
                src_sys_cd,
                ctry_cd,
                CRT_DTTM
            FROM itg_pos
            where src_sys_cd = 'Cosmed 康是美'
                and ctry_cd = 'TW'
                and pos_dt IS NOT NULL
        ) tgt on src.strt_dt = tgt.pos_dt
        and src.product_code = tgt.vend_prod_cd 
)
select * from final