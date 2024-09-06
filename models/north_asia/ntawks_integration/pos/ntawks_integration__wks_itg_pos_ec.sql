with source as (
    select * from {{ source('ntasdl_raw', 'sdl_tw_pos_ec') }}
),
itg_pos as (
    select * from {{ source('ntaitg_integration', 'itg_pos_temp') }}
),
final as
(
    SELECT 
        src.pos_date AS pos_dt,
        src.customer_ec_platfom AS vend_cd,
        NULL AS vend_nm,
        NULL AS prod_nm,
        src.product_code AS vend_prod_cd,
        src.product_name AS vend_prod_nm,
        src.brand AS brnd_nm,
        NULL AS ean_num,
        NULL AS str_cd,
        NULL AS str_nm,
        cast((src.qty + COALESCE(tgt.sls_qty, 0)) as int) AS sls_qty,
        NULL AS sls_amt,
        NULL AS unit_prc_amt,
        cast(
            (
                src.selling_amt_before_tax + COALESCE(tgt.sls_excl_vat_amt, 0)
            ) as decimal(15, 2)
        ) AS sls_excl_vat_amt,
        NULL AS stk_rtrn_amt,
        NULL AS stk_recv_amt,
        NULL AS avg_sell_qty,
        NULL AS cum_ship_qty,
        NULL AS cum_rtrn_qty,
        NULL AS web_ordr_takn_qty,
        NULL AS web_ordr_acpt_qty,
        NULL AS dc_invnt_qty,
        NULL AS invnt_qty,
        NULL AS invnt_amt,
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
        'TWD' AS crncy_cd,
        NULL AS src_txn_sts,
        NULL AS src_seq_num,
        'EC' AS src_sys_cd,
        'TW' AS ctry_cd,
        TGT.CRT_DTTM AS TGT_CRT_DTTM,
        current_timestamp as UPD_DTTM,
        CASE
            WHEN TGT.CRT_DTTM IS NULL THEN 'I'
            ELSE 'U'
        END AS CHNG_FLG
    FROM 
        (
            SELECT MAX(customer_ec_platfom) AS customer_ec_platfom,
                pos_date,
                product_code,
                MAX(product_name) AS product_name,
                COALESCE(SUM(qty), 0) AS qty,
                COALESCE(SUM(selling_amt_before_tax), 0) AS selling_amt_before_tax,
                MAX(brand) AS brand,
                max(crt_dttm) as crt_dttm
            FROM source
            GROUP BY pos_date,
                product_code
        ) SRC
        LEFT OUTER JOIN 
        (
            SELECT pos_dt,
                vend_prod_cd,
                CRT_DTTM,
                upd_dttm,
                COALESCE(sls_excl_vat_amt, 0) AS sls_excl_vat_amt,
                COALESCE(sls_qty, 0) AS sls_qty
            FROM ITG_POS
            WHERE src_sys_cd = 'EC'
                AND ctry_cd = 'TW'
        ) TGT ON SRC.pos_date = TGT.pos_dt
        AND SRC.product_code = TGT.vend_prod_cd
       where src.pos_date not in (
        select distinct pos_dt from itg_pos
        WHERE src_sys_cd = 'EC' and ctry_cd = 'TW'
        )
)
select * from final