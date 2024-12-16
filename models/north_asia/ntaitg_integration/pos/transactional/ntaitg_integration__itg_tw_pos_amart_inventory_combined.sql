{{
    config(
        post_hook="INSERT INTO prod_dna_core.dbt_cloud_pr_5458_1580.ntaitg_integration__itg_pos 
(    
    SELECT 
        pos_dt::date as pos_dt,
        vend_cd::varchar(40) as vend_cd,
        vend_nm::varchar(100) as vend_nm,
        prod_nm::varchar(100) as prod_nm,
        vend_prod_cd::varchar(40) as vend_prod_cd,
        vend_prod_nm::varchar(600) as vend_prod_nm,
        brnd_nm::varchar(40) as brnd_nm,
        ean_num::varchar(100) as ean_num,
        str_cd::varchar(40) as str_cd,
        str_nm::varchar(100) as str_nm,
        sls_qty::number(18,0) as sls_qty,
        sls_amt::number(16,5) as sls_amt,
        unit_prc_amt::number(16,5) as unit_prc_amt,
        sls_excl_vat_amt::number(16,5) as sls_excl_vat_amt,
        stk_rtrn_amt::number(16,5) as stk_rtrn_amt,
        stk_recv_amt::number(16,5) as stk_recv_amt,
        avg_sell_qty::number(16,5) as avg_sell_qty,
        cast(cum_ship_qty as integer) as cum_ship_qty,
        cast(cum_rtrn_qty as integer) as cum_rtrn_qty,
        cast(web_ordr_takn_qty as integer) as web_ordr_takn_qty,
        cast(web_ordr_acpt_qty as integer) as web_ordr_acpt_qty,
        cast(dc_invnt_qty as integer) as dc_invnt_qty,
        cast(invnt_qty as integer) as invnt_qty,
        invnt_amt::number(16,5) as invnt_amt,
        cast(invnt_dt as date) as invnt_dt,
        serial_num::varchar(40) as serial_num,
        prod_delv_type::varchar(40) as prod_delv_type,
        prod_type::varchar(40) as prod_type,
        dept_cd::varchar(40) as dept_cd,
        dept_nm::varchar(100) as dept_nm,
        spec_1_desc::varchar(100) as spec_1_desc,
        spec_2_desc::varchar(100) as spec_2_desc,
        cat_big::varchar(100) as cat_big,
        cat_mid::varchar(40) as cat_mid,
        cat_small::varchar(40) as cat_small,
        dc_prod_cd::varchar(40) as dc_prod_cd,
        cust_dtls::varchar(100) as cust_dtls,
        dist_cd::varchar(40) as dist_cd,
        crncy_cd::varchar(10) as crncy_cd,
        src_txn_sts::varchar(40) as src_txn_sts,
        cast(src_seq_num as integer) as src_seq_num,
        src_sys_cd::varchar(30) as src_sys_cd,
        ctry_cd::varchar(10) as ctry_cd,
        null::varchar(35) as src_mesg_no,
        null::varchar(3) as src_mesg_code,
        null::varchar(3) as src_mesg_func_code,
        null::date as src_mesg_date,
        null::varchar(3) as src_sale_date_form,
        null::varchar(10) as src_send_code,
        null::varchar(13) as src_send_ean_code,
        null::varchar(30) as src_send_name,
        null::varchar(13) as src_recv_qual,
        null::varchar(10) as src_recv_ean_code,
        null::varchar(35) as src_recv_name,
        null::varchar(3) as src_part_qual,
        null::varchar(13) as src_part_ean_code,
        null::varchar(10) as src_part_id,
        null::varchar(30) as src_part_name,
        null::varchar(35) as src_sender_id,
        null::varchar(10) as src_recv_date,
        null::varchar(6) as src_recv_time,
        null::number(8,0) as src_file_size,
        null::varchar(128) as src_file_path,
        null::varchar(1) as src_lega_tran,
        null::varchar(10) as src_regi_date,
        null::number(6,0) as src_line_no,
        null::varchar(20) as src_instore_code,
        null::number(15,0) as src_mnth_sale_amnt,
        null::varchar(3) as src_qty_unit,
        null::number(10,0) as src_mnth_sale_qty,
        null::varchar(5) as unit_of_pkg_sales,
        null::date as doc_send_date,
        null::varchar(5) as unit_of_pkg_invt,
        null::varchar(6) as doc_fun,
        null::varchar(40) as doc_no,
        null::varchar(6) as doc_fun_cd,
        null::varchar(40) as buye_loc_cd,
        null::varchar(40) as vend_loc_cd,
        null::varchar(40) as provider_loc_cd,
        null::number(18,0) as comp_qty,
        null::varchar(5) as unit_of_pkg_comp,
        null::number(18,0) as order_qty,
        null::varchar(5) as unit_of_pkg_order,
        case
                when chng_flg = 'I' then current_timestamp
                else tgt_crt_dttm
            end as crt_dttm,
        current_timestamp AS UPD_DTTM
    FROM 
    (
              select * from prod_dna_core.dbt_cloud_pr_5458_1580.ntaitg_integration__itg_tw_pos_amart_inventory_combined
    )a
);"
    )
}}

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
           SUM(TOTAL_QTY) as inventory_qty
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
        customer_code AS vend_cd,
        null AS vend_nm,
        null AS prod_nm,
        item_code AS vend_prod_cd,
        item_desc AS vend_prod_nm,
        null AS brnd_nm,
        null AS ean_num,
        null AS str_cd,
        null AS str_nm,
        offtake_qty AS sls_qty,
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
