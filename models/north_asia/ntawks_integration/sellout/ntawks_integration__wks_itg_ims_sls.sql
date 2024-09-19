{{
    config(
        pre_hook='{{build_itg_ims_temp()}}'
    )
}}
with sdl_hk_ims_viva_sel_out as (
    select * from {{ source('ntasdl_raw', 'sdl_hk_ims_viva_sel_out') }}
),
sdl_hk_ims_wingkeung_sel_out as (
    select * from {{ source('ntasdl_raw', 'sdl_hk_ims_wingkeung_sel_out') }}
),
itg_ims as (
    select * from {{ source('ntaitg_integration','itg_ims_temp') }}
),
viva as (
    SELECT calendar_sid AS ims_txn_dt,
        COALESCE(customer_number, '#') AS cust_cd,
        customer_name AS cust_nm,
        COALESCE(product_number, '#') AS prod_cd,
        product_description AS prod_nm,
        CAST(NULL AS DATE) AS rpt_per_strt_dt,
        CAST(NULL AS DATE) AS rpt_per_end_dt,
        '#' AS ean_num,
        NULL AS uom,
        NULL AS unit_prc,
        net_trade_sales AS sls_amt,
        sales_order_quantity AS sls_qty,
        CAST(NULL AS INT) AS rtrn_qty,
        NULL AS rtrn_amt,
        NULL AS ship_cust_nm,
        NULL AS cust_cls_grp,
        NULL AS cust_sub_cls,
        NULL AS prod_spec,
        NULL AS itm_agn_nm,
        NULL AS ordr_co,
        return_reason AS rtrn_rsn,
        sales_office AS sls_ofc_cd,
        sales_group AS sls_grp_cd,
        sales_office_name AS sls_ofc_nm,
        sales_group_name AS sls_grp_nm,
        account_types AS acc_type,
        NULL AS co_cd,
        employee AS sls_rep_cd,
        employee_name AS sls_rep_nm,
        CAST(NULL AS DATE) AS doc_dt,
        document_type AS doc_type,
        NULL AS doc_num,
        NULL AS invc_num,
        NULL AS invc_prc,
        NULL AS invc_amt,
        NULL AS channel,
        NULL AS remark_desc,
        NULL AS prom_type,
        NULL AS ship_to_party,
        NULL AS prom_prc,
        NULL AS ordr_num,
        NULL AS ordr_box,
        NULL AS ordr_pc,
        CAST(NULL AS INT) AS gift_qty,
        NULL AS sls_bfr_tax_amt,
        NULL AS sls_bal_amt,
        NULL AS sku_per_box,
        country_code AS ctry_cd,
        currency AS crncy_cd,
        '100681' AS dstr_cd,
        'VIVA (GROUP) TRADING CO., LTD.' AS dstr_nm,
        TGT.CRT_DTTM AS TGT_CRT_DTTM,
        convert_timezone('UTC', current_timestamp()) AS UPDT_DTTM,
        CASE
            WHEN TGT.CRT_DTTM IS NULL THEN 'I'
            ELSE 'U'
        END AS CHNG_FLG
    FROM (
            SELECT calendar_sid,
                customer_number AS customer_number,
                MAX(customer_name) AS customer_name,
                product_number,
                MAX(product_description) AS product_description,
                SUM(net_trade_sales) AS net_trade_sales,
                SUM(sales_order_quantity) AS sales_order_quantity,
                MAX(return_reason) AS return_reason,
                MAX(sales_office) AS sales_office,
                MAX(sales_group) AS sales_group,
                MAX(sales_office_name) AS sales_office_name,
                MAX(sales_group_name) AS sales_group_name,
                MAX(account_types) AS account_types,
                MAX(employee) AS employee,
                MAX(employee_name) AS employee_name,
                MAX(country_code) AS country_code,
                MAX(currency) AS currency,
                CASE
                    WHEN UPPER(transactiontype) = 'INVOICE' THEN 'Invoice'
                    WHEN UPPER(transactiontype) IN ('RETURN', 'RETURENS') THEN 'Return'
                    ELSE '#'
                END document_type
            FROM sdl_hk_ims_viva_sel_out
            WHERE calendar_sid IS NOT NULL
            GROUP BY calendar_sid,
                customer_number,
                product_number,
                transactiontype
        ) src
        LEFT JOIN (
            SELECT ims_txn_dt,
                cust_cd,
                prod_cd,
                doc_type,
                crt_dttm
            FROM itg_ims
            WHERE dstr_cd = '100681'
        ) TGT ON src.calendar_sid = tgt.ims_txn_dt
        AND COALESCE (src.customer_number, '#') = COALESCE (tgt.cust_cd, '#')
        AND COALESCE (src.product_number, '#') = COALESCE (tgt.prod_cd, '#')
        AND COALESCE (src.document_type, '#') = COALESCE (tgt.doc_type, '#')
),
wing as (
    SELECT calendar_sid AS ims_txn_dt,
        COALESCE(customer_number, '#') AS cust_cd,
        customer_name AS cust_nm,
        COALESCE(product_number, '#') AS prod_cd,
        product_description AS prod_nm,
        CAST(NULL AS DATE) AS rpt_per_strt_dt,
        CAST(NULL AS DATE) AS rpt_per_end_dt,
        '#' AS ean_num,
        NULL AS uom,
        NULL AS unit_prc,
        net_trade_sales AS sls_amt,
        sales_order_quantity AS sls_qty,
        CAST(NULL AS INT) AS rtrn_qty,
        NULL AS rtrn_amt,
        NULL AS ship_cust_nm,
        NULL AS cust_cls_grp,
        NULL AS cust_sub_cls,
        NULL AS prod_spec,
        NULL AS itm_agn_nm,
        NULL AS ordr_co,
        return_reason AS rtrn_rsn,
        sales_office AS sls_ofc_cd,
        sales_group AS sls_grp_cd,
        sales_office_name AS sls_ofc_nm,
        sales_group_name AS sls_grp_nm,
        account_types AS acc_type,
        NULL AS co_cd,
        employee AS sls_rep_cd,
        employee_name AS sls_rep_nm,
        CAST(NULL AS DATE) AS doc_dt,
        COALESCE(transactiontype, '#') AS doc_type,
        NULL AS doc_num,
        NULL AS invc_num,
        NULL AS invc_prc,
        NULL AS invc_amt,
        NULL AS channel,
        NULL AS remark_desc,
        NULL AS prom_type,
        NULL AS ship_to_party,
        NULL AS prom_prc,
        NULL AS ordr_num,
        NULL AS ordr_box,
        NULL AS ordr_pc,
        CAST(NULL AS INT) AS gift_qty,
        NULL AS sls_bfr_tax_amt,
        NULL AS sls_bal_amt,
        NULL AS sku_per_box,
        country_code AS ctry_cd,
        currency AS crncy_cd,
        '110256' AS dstr_cd,
        'WING KEUNG MEDICINE COMPANY LTD.' AS dstr_nm,
        TGT.CRT_DTTM AS TGT_CRT_DTTM,
        convert_timezone('UTC', current_timestamp()) AS UPDT_DTTM,
        CASE
            WHEN TGT.CRT_DTTM IS NULL THEN 'I'
            ELSE 'U'
        END AS CHNG_FLG
    FROM (
            SELECT calendar_sid,
                customer_number,
                MAX(customer_name) AS customer_name,
                product_number,
                MAX(product_description) AS product_description,
                SUM(net_trade_sales) AS net_trade_sales,
                SUM(sales_order_quantity) AS sales_order_quantity,
                MAX(return_reason) AS return_reason,
                MAX(sales_office) AS sales_office,
                MAX(sales_group) AS sales_group,
                MAX(sales_office_name) AS sales_office_name,
                MAX(sales_group_name) AS sales_group_name,
                MAX(account_types) AS account_types,
                MAX(employee) AS employee,
                MAX(employee_name) AS employee_name,
                CASE
                    WHEN UPPER(transactiontype) = 'INVOICE' THEN 'Invoice'
                    WHEN UPPER(transactiontype) = 'RETURNS' THEN 'Return'
                    ELSE '#'
                END transactiontype,
                MAX(country_code) AS country_code,
                MAX(currency) AS currency
            FROM sdl_hk_ims_wingkeung_sel_out
            WHERE calendar_sid IS NOT NULL
            GROUP BY calendar_sid,
                customer_number,
                product_number,
                transactiontype
        ) src
        LEFT JOIN (
            SELECT ims_txn_dt,
                cust_cd,
                prod_cd,
                doc_type,
                sls_qty,
                sls_amt,
                crt_dttm
            FROM itg_ims
            WHERE dstr_cd = '110256'
        ) TGT ON src.calendar_sid = tgt.ims_txn_dt
        AND COALESCE (src.customer_number, '#') = COALESCE (tgt.cust_cd, '#')
        AND COALESCE (src.product_number, '#') = COALESCE (tgt.prod_cd, '#')
        AND COALESCE (src.transactiontype, '#') = COALESCE (tgt.doc_type, '#')
),
final as (
    select * from viva
    union all
    select * from wing
)
select * from final