with sdl_tw_pos_rt_mart as (
    select * from {{ source('ntasdl_raw', 'sdl_tw_pos_rt_mart') }}
),
itg_pos_invnt as (
    select * from {{ source('ntaitg_integration', 'itg_pos_invnt_temp') }}
),
final as 
(
    SELECT 
        SRC.pos_date AS invnt_dt,
        SRC.vendor_code AS vend_cd,
        SRC.vendor_name AS vend_nm,
        SRC.product_code AS vend_prod_cd,
        SRC.product_name AS vend_prod_nm,
        trim(SRC.ean_code)::varchar(60) AS ean_num,
        SRC.store_no AS str_cd,
        SRC.store_name AS str_nm,
        SRC.Inventory_qty AS invnt_qty,
        NULL::numeric(16, 5) AS invnt_amt,
        NULL::numeric(16, 5) AS unit_prc_amt,
        NULL::numeric(16, 5) AS per_box_qty,
        NULL::numeric(16, 5) AS cust_invnt_qty,
        NULL::numeric(16, 5) AS box_invnt_qty,
        NULL::numeric(16, 5) AS wk_hold_sls,
        NULL::numeric(16, 5) AS wk_hold,
        NULL::varchar(10) AS fst_recv_dt,
        NULL::varchar(10) AS dsct_dt,
        NULL::varchar(40) AS DC,
        NULL::varchar(40) AS stk_cls,
        'TWD' AS crncy_cd,
        'RT-Mart 大潤發' AS src_sys_cd,
        'TW' AS ctry_cd,
        TGT.CRT_DTTM AS TGT_CRT_DTTM,
        SRC.UPD_DTTM,
        CASE
            WHEN TGT.CRT_DTTM IS NULL THEN 'I'
            ELSE 'U'
        END AS CHNG_FLG
    FROM 
    sdl_tw_pos_rt_mart SRC
    LEFT OUTER JOIN
    (
        SELECT invnt_dt,
            vend_prod_cd,
            str_cd,
            src_sys_cd,
            ctry_cd,
            CRT_DTTM
        FROM itg_pos_invnt
    ) TGT ON SRC.pos_date = TGT.invnt_dt
    AND SRC.product_code = TGT.vend_prod_cd
    AND SRC.store_no = TGT.str_cd
    AND 'RT-Mart 大潤發' = TGT.src_sys_cd
    AND 'TW' = TGT.ctry_cd
)
select * from final