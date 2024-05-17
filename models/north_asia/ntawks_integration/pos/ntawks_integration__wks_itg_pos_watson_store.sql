with itg_tw_pos_watson_store as (
    select * from {{ ref('ntaitg_integration__itg_tw_pos_watson_store') }}
),
itg_query_parameters as (
    select * from {{ source('ntaitg_integration', 'itg_query_parameters') }}
),
itg_pos as (
    select * from {{ source('ntaitg_integration', 'itg_pos_temp') }}
),
final as
(
    SELECT 
        start_date AS pos_dt,
        NULL AS vend_cd,
        NULL AS vend_nm,
        NULL AS prod_nm,
        product_code AS vend_prod_cd,
        product_description AS vend_prod_nm,
        NULL AS brnd_nm,
        NULL AS ean_num,
        store_no AS str_cd,
        store_name AS str_nm,
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
        NULL AS invnt_qty,
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
        NULL AS cat_small,
        NULL AS dc_prod_cd,
        NULL AS cust_dtls,
        NULL AS dist_cd,
        'TWD' AS crncy_cd,
        NULL AS src_txn_sts,
        NULL AS src_seq_num,
        para.parameter_value AS src_sys_cd,
        para.country_code AS ctry_cd,
        TGT.CRT_DTTM AS TGT_CRT_DTTM,
        NULL AS UPD_DTTM,
        CASE
            WHEN TGT.CRT_DTTM IS NULL THEN 'I'
            ELSE 'U'
        END AS CHNG_FLG
    FROM itg_tw_pos_watson_store SRC
    LEFT OUTER JOIN 
    (
        select * from itg_query_parameters
        where country_code = 'TW'
        and parameter_type = 'cust_nm'
    ) para
    ON SPLIT_PART(SRC.filename,'_',3) = para.parameter_name
    AND SPLIT_PART(SRC.filename,'_',1) = para.country_code
    LEFT OUTER JOIN 
    (
        SELECT distinct pos_dt,
            vend_prod_cd,
            str_cd,
            src_sys_cd,
            ctry_cd,
            CRT_DTTM
        FROM ITG_POS pos
    ) TGT
    ON NVL(SRC.product_code,'#') = NVL(TGT.vend_prod_cd,'#')
    AND SRC.start_date = TGT.pos_dt
    AND SRC.store_no = TGT.str_cd
    AND SPLIT_PART(SRC.filename,'_',3) = SPLIT_PART(TGT.src_sys_cd,' ',1)
    AND SPLIT_PART(SRC.filename,'_',1) = TGT.ctry_cd
    AND TGT.pos_dt IS NOT NULL
)
select * from final