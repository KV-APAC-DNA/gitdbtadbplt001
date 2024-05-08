with source as (
    select * from {{ source('ntasdl_raw', 'sdl_tw_pos_ec') }}
),
final as
(
    select
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
        current_timestamp as UPD_DTTM,
    FROM
    (
            SELECT
                max(customer_ec_platfom) as customer_ec_platfom,
                pos_date,
                product_code,
                max(product_name) as product_name,
                coalesce(sum(qty), 0) as qty,
                coalesce(sum(selling_amt_before_tax), 0) as selling_amt_before_tax,
                max(brand) as brand
            from source
            GROUP BY pos_date,
                product_code
        ) SRC
)