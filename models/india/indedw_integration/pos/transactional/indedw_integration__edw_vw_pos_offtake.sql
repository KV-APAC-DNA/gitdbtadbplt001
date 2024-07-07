{{
    config(
        materialized='view'
    )
}}

with edw_retailer_calendar_dim as
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
itg_pos_historical_btl as
(
    select * from {{ ref('inditg_integration__itg_pos_historical_btl') }}
),
itg_pos_offtake_fact as
(
    select * from {{ ref('inditg_integration__itg_pos_offtake_fact') }}
),
itg_pos_re_mapping as
(
    select * from {{ ref('inditg_integration__itg_pos_re_mapping') }}
),
itg_pos_category_mapping as
(
    select * from {{ ref('inditg_integration__itg_pos_category_mapping') }}
),
final as
(
    SELECT 
    COALESCE(txn.store_cd, '#N/A' :: character varying) AS store_cd, 
    COALESCE(txn.article_cd, '#N/A' :: character varying) AS article_cd, 
    cal.mth_mm AS period, 
    left(((cal.mth_mm):: character varying):: text,4) AS fisc_year, 
    right(((cal.mth_mm):: character varying):: text,2) AS fisc_month, 
    txn.level, 
    sum(txn.sls_qty) AS sls_qty, 
    sum(txn.sls_val_lcy) AS sls_val_lcy, 
    COALESCE(txn.key_account_name, '#N/A' :: character varying) AS account_name, 
    COALESCE(re.store_name, '#N/A' :: character varying) AS store_name, 
    COALESCE(re.region, '#N/A' :: character varying) AS "region", 
    COALESCE(re.zone, '#N/A' :: character varying) AS zone, 
    COALESCE(re.re, '#N/A' :: character varying) AS re, 
    COALESCE(re.promotor, '#N/A' :: character varying) AS promotor, 
    COALESCE(acm.mother_sku_name, '#N/A' :: character varying) AS mother_sku_name, 
    COALESCE(acm.sap_cd, '#N/A' :: character varying) AS sap_cd, 
    COALESCE(acm.brand_name, '#N/A' :: character varying) AS brand_name, 
    COALESCE(acm.franchise_name, '#N/A' :: character varying) AS franchise_name, 
    COALESCE(acm.internal_category, '#N/A' :: character varying) AS internal_category, 
    COALESCE(acm.internal_sub_category, '#N/A' :: character varying) AS internal_subcategory, 
    COALESCE(acm.external_category, '#N/A' :: character varying) AS external_category, 
    COALESCE(acm.external_sub_category, '#N/A' :: character varying) AS external_subcategory, 
    sum(btl.promos) AS promos, 
    txn.file_upload_date, 
    acm.article_desc AS article_name, 
    acm.product_name, 
    acm.product_category_name, 
    acm.variant_name, 
    cal.qtr AS quarter 
    FROM(((((SELECT itg_pos_offtake_fact.key_account_name, 
                itg_pos_offtake_fact.pos_dt, 
                CASE WHEN (((itg_pos_offtake_fact.store_cd):: text = ('' :: character varying):: text) OR ((itg_pos_offtake_fact.store_cd IS NULL) AND ('' IS NULL))) THEN '#N/A' :: character varying 
                    WHEN (((itg_pos_offtake_fact.store_cd):: text = (NULL :: character varying):: text) OR ((itg_pos_offtake_fact.store_cd IS NULL) /*AND (NULL :: "unknown" IS NULL)*/)) THEN '#N/A' :: character varying ELSE itg_pos_offtake_fact.store_cd END AS store_cd, 
                itg_pos_offtake_fact.subcategory, 
                ITG_POS_OFFTAKE_FACT.level, 
                CASE WHEN (((itg_pos_offtake_fact.article_cd):: text = ('' :: character varying):: text) OR ((itg_pos_offtake_fact.article_cd IS NULL) AND ('' IS NULL))) THEN '#N/A' :: character varying 
                    WHEN (((itg_pos_offtake_fact.article_cd):: text = (NULL :: character varying):: text) OR ((itg_pos_offtake_fact.article_cd IS NULL) /*AND (NULL :: "unknown" IS NULL)*/)) THEN '#N/A' :: character varying ELSE itg_pos_offtake_fact.article_cd END AS article_cd, 
                itg_pos_offtake_fact.file_upload_date, 
                sum(itg_pos_offtake_fact.sls_qty) AS sls_qty, 
                sum(itg_pos_offtake_fact.sls_val_lcy) AS sls_val_lcy 
                FROM 
                itg_pos_offtake_fact 
                GROUP BY 
                itg_pos_offtake_fact.key_account_name, 
                itg_pos_offtake_fact.pos_dt, 
                itg_pos_offtake_fact.store_cd, 
                itg_pos_offtake_fact.subcategory, 
                ITG_POS_OFFTAKE_FACT.level, 
                itg_pos_offtake_fact.article_cd, 
                itg_pos_offtake_fact.file_upload_date
            ) txn 
            LEFT JOIN (
                SELECT 
                itg_pos_re_mapping.store_cd, 
                itg_pos_re_mapping.account_name, 
                itg_pos_re_mapping.store_name, 
                itg_pos_re_mapping.region, 
                itg_pos_re_mapping.zone, 
                itg_pos_re_mapping.re, 
                itg_pos_re_mapping.promotor, 
                itg_pos_re_mapping.crt_dttm, 
                itg_pos_re_mapping.filename 
                FROM 
                itg_pos_re_mapping) re 
                ON (((upper((txn.key_account_name):: text) = upper((re.account_name):: text)) 
                AND (upper(ltrim((txn.store_cd):: text,('0' :: character varying):: text)) = upper(ltrim((re.store_cd):: text,('0' :: character varying):: text)))
                ))) 
            LEFT JOIN (
            SELECT 
                'J&J' :: character varying AS "level", 
                NULL :: bigint AS row_number, 
                itg_pos_category_mapping.account_name, 
                itg_pos_category_mapping.article_cd, 
                itg_pos_category_mapping.article_desc, 
                itg_pos_category_mapping.ean, 
                itg_pos_category_mapping.sap_cd, 
                itg_pos_category_mapping.mother_sku_name, 
                itg_pos_category_mapping.brand_name, 
                itg_pos_category_mapping.franchise_name, 
                itg_pos_category_mapping.product_category_name, 
                itg_pos_category_mapping.variant_name, 
                itg_pos_category_mapping.product_name, 
                itg_pos_category_mapping.internal_category, 
                itg_pos_category_mapping.internal_sub_category, 
                itg_pos_category_mapping.external_category, 
                itg_pos_category_mapping.external_sub_category 
            FROM 
                itg_pos_category_mapping 
            UNION ALL 
            SELECT 
                t1."level", 
                t1.row_number, 
                t1.account_name, 
                t1.article_cd, 
                t1.article_desc, 
                t1.ean, 
                t1.sap_cd, 
                t1.mother_sku_name, 
                t1.brand_name, 
                t1.franchise_name, 
                t1.product_category_name, 
                t1.variant_name, 
                t1.product_name, 
                t1.internal_category, 
                t1.internal_sub_category, 
                t1.external_category, 
                t1.external_sub_category 
            FROM 
                (
                SELECT 
                    'Category' :: character varying AS "level", 
                    row_number() OVER(PARTITION BY itg_pos_category_mapping.account_name,itg_pos_category_mapping.external_sub_category order by (select 1)) AS row_number, 
                    itg_pos_category_mapping.account_name, 
                    NULL :: character varying AS article_cd, 
                    NULL :: character varying AS article_desc, 
                    NULL :: character varying AS ean, 
                    NULL :: character varying AS sap_cd, 
                    NULL :: character varying AS mother_sku_name, 
                    NULL :: character varying AS brand_name, 
                    NULL :: character varying AS franchise_name, 
                    NULL :: character varying AS product_category_name, 
                    NULL :: character varying AS variant_name, 
                    NULL :: character varying AS product_name, 
                    itg_pos_category_mapping.internal_category, 
                    itg_pos_category_mapping.internal_sub_category, 
                    itg_pos_category_mapping.external_category, 
                    itg_pos_category_mapping.external_sub_category 
                FROM 
                    itg_pos_category_mapping
                ) t1 
            WHERE 
                (t1.row_number = 1)) acm 
                ON (((upper((txn.key_account_name):: text) = upper((acm.account_name):: text)) 
                AND (CASE WHEN (((upper((txn.level):: text) = ('J&J' :: character varying):: text)
                                AND (upper(ltrim((txn.article_cd):: text,('0' :: character varying):: text)) = upper(ltrim((acm.article_cd):: text,('0' :: character varying):: text))))
                                AND (upper((acm."level"):: text) = ('J&J' :: character varying):: text)) THEN 1 
                        WHEN (((upper((txn.level):: text) = ('CATEGORY' :: character varying):: text)
                                AND (upper((txn.subcategory):: text) = upper((acm.external_sub_category):: text))) 
                                AND (upper((acm."level"):: text) = ('CATEGORY' :: character varying):: text)) THEN 1 ELSE 0 END = 1
                )))) 
        LEFT JOIN (
            SELECT 
            edw_retailer_calendar_dim.caldate, 
            edw_retailer_calendar_dim.day, 
            edw_retailer_calendar_dim.week, 
            edw_retailer_calendar_dim.mth_mm, 
            edw_retailer_calendar_dim.mth_yyyymm, 
            edw_retailer_calendar_dim.qtr, 
            edw_retailer_calendar_dim.yyyyqtr, 
            edw_retailer_calendar_dim.cal_yr, 
            edw_retailer_calendar_dim.fisc_yr, 
            edw_retailer_calendar_dim.crt_dttm, 
            edw_retailer_calendar_dim.updt_dttm, 
            edw_retailer_calendar_dim.month_nm, 
            edw_retailer_calendar_dim.month_nm_shrt 
            FROM 
            edw_retailer_calendar_dim) cal 
            ON ((to_date((txn.pos_dt):: date) = to_date(cal.caldate)
            ))) 
        LEFT JOIN (
        SELECT 
            itg_pos_historical_btl.mother_sku_name, 
            itg_pos_historical_btl.account_name, 
            itg_pos_historical_btl.re, 
            substring(((itg_pos_historical_btl.pos_dt):: character varying):: text,1,4) AS "year", 
            substring(((itg_pos_historical_btl.pos_dt):: character varying):: text,6,2) AS "month", 
            itg_pos_historical_btl.pos_dt, 
            itg_pos_historical_btl.promos, 
            itg_pos_historical_btl.crt_dttm, 
            itg_pos_historical_btl.filename 
        FROM 
            itg_pos_historical_btl
        ) btl ON (((((upper((acm.account_name):: text) = upper((btl.account_name):: text)) 
            AND (upper((acm.mother_sku_name):: text) = upper((btl.mother_sku_name):: text))) 
            AND (to_char((txn.pos_dt):: date,('YYYYMM' :: character varying):: text) = to_char((btl.pos_dt):: date,('YYYYMM' :: character varying):: text))) 
            AND (upper((re.re):: text) = upper((btl.re):: text))))) 
    GROUP BY 
    txn.store_cd, 
    txn.article_cd, 
    cal.mth_mm, 
    txn.level, 
    txn.key_account_name, 
    re.store_name, 
    re.region, 
    re.zone, 
    re.re, 
    re.promotor, 
    acm.mother_sku_name, 
    acm.sap_cd, 
    acm.brand_name, 
    acm.franchise_name, 
    acm.internal_category, 
    acm.internal_sub_category, 
    acm.external_category, 
    acm.external_sub_category, 
    txn.file_upload_date, 
    acm.article_desc, 
    acm.product_name, 
    acm.product_category_name, 
    acm.variant_name, 
    cal.qtr

)
select * from final