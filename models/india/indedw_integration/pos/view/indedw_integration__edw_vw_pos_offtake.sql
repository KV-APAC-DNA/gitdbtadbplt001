{{
    config
    (
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
SELECT COALESCE(txn.store_cd, '#N/A'::CHARACTER VARYING) AS store_cd,
    COALESCE(txn.article_cd, '#N/A'::CHARACTER VARYING) AS article_cd,
    cal.mth_mm AS period,
    left(((cal.mth_mm)::CHARACTER VARYING)::TEXT, 4) AS fisc_year,
    right(((cal.mth_mm)::CHARACTER VARYING)::TEXT, 2) AS fisc_month,
    txn.LEVEL,
    sum(txn.sls_qty) AS sls_qty,
    sum(txn.sls_val_lcy) AS sls_val_lcy,
    COALESCE(txn.key_account_name, '#N/A'::CHARACTER VARYING) AS account_name,
    COALESCE(re.store_name, '#N/A'::CHARACTER VARYING) AS store_name,
    COALESCE(re.region, '#N/A'::CHARACTER VARYING) AS "region",
    COALESCE(re.zone, '#N/A'::CHARACTER VARYING) AS zone,
    COALESCE(re.re, '#N/A'::CHARACTER VARYING) AS re,
    COALESCE(re.promotor, '#N/A'::CHARACTER VARYING) AS promotor,
    COALESCE(acm.mother_sku_name, '#N/A'::CHARACTER VARYING) AS mother_sku_name,
    COALESCE(acm.sap_cd, '#N/A'::CHARACTER VARYING) AS sap_cd,
    COALESCE(acm.brand_name, '#N/A'::CHARACTER VARYING) AS brand_name,
    COALESCE(acm.franchise_name, '#N/A'::CHARACTER VARYING) AS franchise_name,
    COALESCE(acm.internal_category, '#N/A'::CHARACTER VARYING) AS internal_category,
    COALESCE(acm.internal_sub_category, '#N/A'::CHARACTER VARYING) AS internal_subcategory,
    COALESCE(acm.external_category, '#N/A'::CHARACTER VARYING) AS external_category,
    COALESCE(acm.external_sub_category, '#N/A'::CHARACTER VARYING) AS external_subcategory,
    sum(btl.promos) AS promos,
    txn.file_upload_date,
    acm.article_desc AS article_name,
    acm.product_name,
    acm.product_category_name,
    acm.variant_name,
    cal.qtr AS quarter
FROM (
    (
        (
            (
                (
                    SELECT itg_pos_offtake_fact.key_account_name,
                        itg_pos_offtake_fact.pos_dt,
                        CASE 
                            WHEN (
                                    ((itg_pos_offtake_fact.store_cd)::TEXT = (''::CHARACTER VARYING)::TEXT)
                                    OR (
                                        (itg_pos_offtake_fact.store_cd IS NULL)
                                        AND ('' IS NULL)
                                        )
                                    )
                                THEN '#N/A'::CHARACTER VARYING
                            WHEN (
                                    ((itg_pos_offtake_fact.store_cd)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
                                    OR ((itg_pos_offtake_fact.store_cd IS NULL) /*AND (NULL :: "unknown" IS NULL)*/)
                                    )
                                THEN '#N/A'::CHARACTER VARYING
                            ELSE itg_pos_offtake_fact.store_cd
                            END AS store_cd,
                        itg_pos_offtake_fact.subcategory,
                        ITG_POS_OFFTAKE_FACT.LEVEL,
                        CASE 
                            WHEN (
                                    ((itg_pos_offtake_fact.article_cd)::TEXT = (''::CHARACTER VARYING)::TEXT)
                                    OR (
                                        (itg_pos_offtake_fact.article_cd IS NULL)
                                        AND ('' IS NULL)
                                        )
                                    )
                                THEN '#N/A'::CHARACTER VARYING
                            WHEN (
                                    ((itg_pos_offtake_fact.article_cd)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
                                    OR ((itg_pos_offtake_fact.article_cd IS NULL) /*AND (NULL :: "unknown" IS NULL)*/)
                                    )
                                THEN '#N/A'::CHARACTER VARYING
                            ELSE itg_pos_offtake_fact.article_cd
                            END AS article_cd,
                        itg_pos_offtake_fact.file_upload_date,
                        sum(itg_pos_offtake_fact.sls_qty) AS sls_qty,
                        sum(itg_pos_offtake_fact.sls_val_lcy) AS sls_val_lcy
                    FROM itg_pos_offtake_fact
                    GROUP BY itg_pos_offtake_fact.key_account_name,
                        itg_pos_offtake_fact.pos_dt,
                        itg_pos_offtake_fact.store_cd,
                        itg_pos_offtake_fact.subcategory,
                        ITG_POS_OFFTAKE_FACT.LEVEL,
                        itg_pos_offtake_fact.article_cd,
                        itg_pos_offtake_fact.file_upload_date
                ) txn LEFT JOIN (
                    SELECT itg_pos_re_mapping.store_cd,
                        itg_pos_re_mapping.account_name,
                        itg_pos_re_mapping.store_name,
                        itg_pos_re_mapping.region,
                        itg_pos_re_mapping.zone,
                        itg_pos_re_mapping.re,
                        itg_pos_re_mapping.promotor,
                        itg_pos_re_mapping.crt_dttm,
                        itg_pos_re_mapping.filename
                    FROM itg_pos_re_mapping
                    ) re ON (
                        (
                            (upper((txn.key_account_name)::TEXT) = upper((re.account_name)::TEXT))
                            AND (upper(ltrim((txn.store_cd)::TEXT, ('0'::CHARACTER VARYING)::TEXT)) = upper(ltrim((re.store_cd)::TEXT, ('0'::CHARACTER VARYING)::TEXT)))
                            )
                        )
            ) LEFT JOIN (
                SELECT 'J&J'::CHARACTER VARYING AS "level",
                    NULL::BIGINT AS row_number,
                    UPPER(itg_pos_category_mapping.account_name) AS account_name,
                    itg_pos_category_mapping.article_cd,
                    LTRIM(itg_pos_category_mapping.article_desc) AS article_desc,
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
                FROM itg_pos_category_mapping
                
                UNION ALL
                
                SELECT t1."level",
                    t1.row_number,
                    UPPER(t1.account_name) AS account_name,
                    t1.article_cd,
                    LTRIM(t1.article_desc) AS article_desc,
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
                FROM (
                    SELECT 'Category'::CHARACTER VARYING AS "level",
                        row_number() OVER (
                            PARTITION BY UPPER(itg_pos_category_mapping.account_name),
                            itg_pos_category_mapping.external_sub_category ORDER BY (
                                    SELECT 1
                                    )
                            ) AS row_number,
                        itg_pos_category_mapping.account_name,
                        NULL::CHARACTER VARYING AS article_cd,
                        NULL::CHARACTER VARYING AS article_desc,
                        NULL::CHARACTER VARYING AS ean,
                        NULL::CHARACTER VARYING AS sap_cd,
                        NULL::CHARACTER VARYING AS mother_sku_name,
                        NULL::CHARACTER VARYING AS brand_name,
                        NULL::CHARACTER VARYING AS franchise_name,
                        NULL::CHARACTER VARYING AS product_category_name,
                        NULL::CHARACTER VARYING AS variant_name,
                        NULL::CHARACTER VARYING AS product_name,
                        itg_pos_category_mapping.internal_category,
                        LTRIM(itg_pos_category_mapping.internal_sub_category) AS internal_sub_category,
                        itg_pos_category_mapping.external_category,
                        itg_pos_category_mapping.external_sub_category
                    FROM itg_pos_category_mapping
                    ) t1
                WHERE (t1.row_number = 1)
                ) acm ON (
                    (
                        (upper((txn.key_account_name)::TEXT) = upper((acm.account_name)::TEXT))
                        AND (
                            CASE 
                                WHEN (
                                        (
                                            (upper((txn.LEVEL)::TEXT) = ('J&J'::CHARACTER VARYING)::TEXT)
                                            AND (upper(ltrim((txn.article_cd)::TEXT, ('0'::CHARACTER VARYING)::TEXT)) = upper(ltrim((acm.article_cd)::TEXT, ('0'::CHARACTER VARYING)::TEXT)))
                                            )
                                        AND (upper((acm."level")::TEXT) = ('J&J'::CHARACTER VARYING)::TEXT)
                                        )
                                    THEN 1
                                WHEN (
                                        (
                                            (upper((txn.LEVEL)::TEXT) = ('CATEGORY'::CHARACTER VARYING)::TEXT)
                                            AND (upper((txn.subcategory)::TEXT) = upper((acm.external_sub_category)::TEXT))
                                            )
                                        AND (upper((acm."level")::TEXT) = ('CATEGORY'::CHARACTER VARYING)::TEXT)
                                        )
                                    THEN 1
                                ELSE 0
                                END = 1
                            )
                        )
                    )
        ) LEFT JOIN (
            SELECT edw_retailer_calendar_dim.caldate,
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
            FROM edw_retailer_calendar_dim
            ) cal ON ((to_date((txn.pos_dt)::DATE) = to_date(cal.caldate)))
    ) LEFT JOIN (
        SELECT itg_pos_historical_btl.mother_sku_name,
            itg_pos_historical_btl.account_name,
            itg_pos_historical_btl.re,
            substring(((itg_pos_historical_btl.pos_dt)::CHARACTER VARYING)::TEXT, 1, 4) AS "year",
            substring(((itg_pos_historical_btl.pos_dt)::CHARACTER VARYING)::TEXT, 6, 2) AS "month",
            itg_pos_historical_btl.pos_dt,
            itg_pos_historical_btl.promos,
            itg_pos_historical_btl.crt_dttm,
            itg_pos_historical_btl.filename
        FROM itg_pos_historical_btl
        ) btl ON (
            (
                (
                    (
                        (upper((acm.account_name)::TEXT) = upper((btl.account_name)::TEXT))
                        AND (upper((acm.mother_sku_name)::TEXT) = upper((btl.mother_sku_name)::TEXT))
                        )
                    AND (to_char((txn.pos_dt)::DATE, ('YYYYMM'::CHARACTER VARYING)::TEXT) = to_char((btl.pos_dt)::DATE, ('YYYYMM'::CHARACTER VARYING)::TEXT))
                    )
                AND (upper((re.re)::TEXT) = upper((btl.re)::TEXT))
                )
            )
    )
GROUP BY txn.store_cd,
    txn.article_cd,
    cal.mth_mm,
    txn.LEVEL,
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
