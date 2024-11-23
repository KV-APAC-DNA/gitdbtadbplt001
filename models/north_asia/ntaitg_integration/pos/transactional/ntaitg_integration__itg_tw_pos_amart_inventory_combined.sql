WITH 
SDL_TW_POS_AMART_INVENTORY as (
    select * from {{ ref('ntawks_integration__wks_itg_pos_amart_inventory') }}
),
SDL_TW_POS_AMART_PURCHASE as (
    select * from {{ ref('ntawks_integration__wks_itg_pos_amart_purchase') }}
),
inventory AS (
    SELECT mnth_id::numeric as mnth_id,
           item_code,
           item_desc,
           customer_code,
           SUM(inventory_qty) as inventory_qty
    FROM SDL_TW_POS_AMART_INVENTORY
    GROUP BY 1,2,3,4
),
purchases AS (
    SELECT product_code,
           TO_CHAR(purchase_date::DATE, 'YYYYMM')::numeric AS mnth_id,
           SUM(purchase_qty) as purchase_qty
    FROM SDL_TW_POS_AMART_PURCHASE
    GROUP BY 1,2
),

source as (
SELECT 
    curr.mnth_id,
    curr.item_code,
    curr.item_desc,
    curr.customer_code,
    curr.inventory_qty,
    p.purchase_qty,
    (prev.inventory_qty + COALESCE(p.purchase_qty,0) - curr.inventory_qty) as offtake_qty,
    CURRENT_TIMESTAMP()::timestamp_ntz(9) AS crtd_dttm,
        CURRENT_TIMESTAMP()::timestamp_ntz(9) AS UPD_DTTM

FROM inventory curr
LEFT JOIN inventory prev ON curr.item_code = prev.item_code 
    AND curr.mnth_id = prev.mnth_id + 1
LEFT JOIN purchases p ON curr.item_code = p.product_code 
    AND curr.mnth_id = p.mnth_id),


--------------


itg_pos as (
    select * from {{ source('ntaitg_integration', 'itg_pos_temp') }}
),

final as
(
    select 
        LAST_DAY(TO_DATE(CONCAT(mnth_id,'01'),'YYYYMMDD')) AS pos_dt,
        '12087' AS vend_cd,
        null AS vend_nm,
        null AS prod_nm,
        item_code AS vend_prod_cd,
        item_desc AS vend_prod_nm,
        null AS brnd_nm,
        null AS ean_num,
        null AS str_cd,
        null AS str_nm,
        purchase_qty AS sls_qty,
        NULL AS sls_amt,
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
        inventory_qty AS invnt_qty,
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
        'A-Mart 愛買' AS src_sys_cd,
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
            -- str_cd,
            src_sys_cd,
            ctry_cd,
            CRT_DTTM
        FROM ITG_POS
        where src_sys_cd = 'A-Mart 愛買'
            and ctry_cd = 'TW'
    ) TGT ON LAST_DAY(TO_DATE(CONCAT(SRC.mnth_id,'01'),'YYYYMMDD')) = TGT.pos_dt
    AND SRC.item_code = TGT.vend_prod_cd
    -- AND SRC.store_no = TGT.str_cd
    
        
)
select * from final
