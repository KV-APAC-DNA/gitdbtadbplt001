with sdl_tw_pos_rt_mart as (
    select * from {{ source('ntasdl_raw', 'sdl_tw_pos_rt_mart') }}
),
itg_pos as (
    select * from {{ source('ntaitg_integration', 'itg_pos_temp') }}
),
final AS 
(
    select 
        pos_date AS pos_dt,
        vendor_code AS vend_cd,
        vendor_name AS vend_nm,
        null::varchar(100) AS prod_nm,
        product_code AS vend_prod_cd,
        product_name AS vend_prod_nm,
        null::varchar(40) AS brnd_nm,
        ean_code AS ean_num,
        store_no AS str_cd,
        store_name AS str_nm,
        selling_qty AS sls_qty,
        null::numeric(16, 5) AS sls_amt,
        null::numeric(16, 5) AS unit_prc_amt,
        null::numeric(16, 5) AS sls_excl_vat_amt,
        null::numeric(16, 5) AS stk_rtrn_amt,
        null::numeric(16, 5) AS stk_recv_amt,
        average_selling_qty AS avg_sell_qty,
        stock_receive_qty AS cum_ship_qty,
        null::integer AS cum_rtrn_qty,
        null::integer AS web_ordr_takn_qty,
        null::integer AS web_ordr_acpt_qty,
        null::integer AS dc_invnt_qty,
        inventory_qty AS invnt_qty,
        null::numeric(16, 5) AS invnt_amt,
        null::date AS invnt_dt,
        null::varchar(40) AS serial_num,
        null::varchar(40) AS prod_delv_type,
        null::varchar(40) AS prod_type,
        department AS dept_cd,
        department_name AS dept_nm,
        null::varchar(100) AS spec_1_desc,
        null::varchar(100) AS spec_2_desc,
        null::varchar(100) AS cat_big,
        null::varchar(40) AS cat_mid,
        null::varchar(40) AS cat_small,
        null::varchar(40) AS dc_prod_cd,
        null::varchar(100) AS cust_dtls,
        null::varchar(40) AS dist_cd,
        'TWD' AS crncy_cd,
        null::varchar(40) AS src_txn_sts,
        null::integer AS src_seq_num,
        'RT-Mart 大潤發' AS src_sys_cd,
        'TW' AS ctry_cd,
        TGT.CRT_DTTM AS TGT_CRT_DTTM,
        UPD_DTTM,
        CASE
            WHEN TGT.CRT_DTTM IS NULL THEN 'I'
            ELSE 'U'
        END AS CHNG_FLG
    FROM sdl_tw_pos_rt_mart SRC
    LEFT OUTER JOIN 
    (
        SELECT pos_dt,
            vend_prod_cd,
            str_cd,
            src_sys_cd,
            ctry_cd,
            CRT_DTTM
        FROM ITG_POS
        where src_sys_cd = 'RT-Mart 大潤發'
            and ctry_cd = 'TW'
            and pos_dt IS NOT NULL
    ) TGT ON SRC.pos_date = TGT.pos_dt
    AND SRC.product_code = TGT.vend_prod_cd
    AND SRC.store_no = TGT.str_cd
)    
select * from final