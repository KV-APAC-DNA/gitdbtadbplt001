with edw_perfect_store as 
(
    select * from {{ ref('aspedw_integration__edw_perfect_store') }}
),
itg_mds_rg_ps_channel_weights as 
(
    select * from {{ source('aspitg_integration', 'itg_mds_rg_ps_channel_weights') }} -- currently used as source, but need a confirmation from Kenvue team
),
itg_mds_rg_ps_market_coverage as 
(
    select * from {{ ref('aspitg_integration__itg_mds_rg_ps_market_coverage') }}
),
cte1 as (
    SELECT DATASET AS DATASET,
        CUSTOMERID AS CUSTOMERID,
        SALESPERSONID AS SALESPERSONID,
        VISITID AS VISITID,
        QUESTIONTEXT AS QUESTIONTEXT,
        PRODUCTID AS PRODUCTID,
        KPI AS KPI,
        SCHEDULEDDATE AS SCHEDULEDDATE,
        FISC_YR AS FISC_YR,
        FISC_PER AS FISC_PER,
        CASE
            WHEN NULLIF(TRIM(LASTNAME), '') IS NOT NULL THEN FIRSTNAME || ',' || LASTNAME
            ELSE FIRSTNAME
        END AS MERCHANDISER_NAME,
        CUSTOMERNAME AS CUSTOMERNAME,
        COUNTRY AS COUNTRY,
        STATE AS STATE,
        STOREREFERENCE AS PARENT_CUSTOMER,
        STORETYPE AS RETAIL_ENVIRONMENT,
        CHANNEL AS CHANNEL,
        SALESGROUP AS RETAILER,
        BU AS BUSINESS_UNIT,
        EANNUMBER AS EANNUMBER,
        MATL_NUM AS MATL_NUM,
        PROD_HIER_L1 AS PROD_HIER_L1,
        PROD_HIER_L2 AS PROD_HIER_L2,
        PROD_HIER_L3 AS PROD_HIER_L3,
        PROD_HIER_L4 AS PROD_HIER_L4,
        PROD_HIER_L5 AS PROD_HIER_L5,
        PROD_HIER_L6 AS PROD_HIER_L6,
        PROD_HIER_L7 AS PROD_HIER_L7,
        PROD_HIER_L8 AS PROD_HIER_L8,
        PRODUCTNAME AS PROD_HIER_L9,
        KPI_CHNL_WT AS KPI_CHNL_WT,
        MKT_SHARE AS MKT_SHARE,
        QUES_DESC AS QUES_TYPE,
        "y/n_flag" AS "y/n_flag",
        PRIORITY_STORE_FLAG AS PRIORITY_STORE_FLAG,
        RESPONSE,
        RESPONSE_SCORE,
        NULL AS ADD_INFO,
        CASE
            WHEN UPPER(PRESENCE) = 'TRUE' THEN 1
            ELSE 0
        END AS ACTUAL_VALUE,
        1 AS REF_VALUE,
        NULL AS KPI_ACTUAL_WT_VAL,
        NULL AS KPI_REF_VAL,
        NULL AS KPI_REF_WT_VAL,
        PHOTO_URL,
        STORE_GRADE --gcph_category as gcph_category,
        --gcph_subcategory as gcph_subcategory
    FROM EDW_PERFECT_STORE
    WHERE UPPER(KPI) = 'MSL COMPLIANCE'
        AND UPPER(VST_STATUS) = 'COMPLETED'
        AND UPPER(MUSTCARRYITEM) = 'TRUE'
        AND UPPER(COUNTRY) <> 'INDIA'
),
cte2 as (
    SELECT DATASET AS DATASET,
        CUSTOMERID AS CUSTOMERID,
        SALESPERSONID AS SALESPERSONID,
        VISITID AS VISITID,
        QUESTIONTEXT AS QUESTIONTEXT,
        PRODUCTID AS PRODUCTID,
        KPI AS KPI,
        SCHEDULEDDATE AS SCHEDULEDDATE,
        FISC_YR AS FISC_YR,
        FISC_PER AS FISC_PER,
        CASE
            WHEN NULLIF(TRIM(LASTNAME), '') IS NOT NULL THEN FIRSTNAME || ',' || LASTNAME
            ELSE FIRSTNAME
        END AS MERCHANDISER_NAME,
        CUSTOMERNAME AS CUSTOMERNAME,
        COUNTRY AS COUNTRY,
        STATE AS STATE,
        STOREREFERENCE AS PARENT_CUSTOMER,
        STORETYPE AS RETAIL_ENVIRONMENT,
        CHANNEL AS CHANNEL,
        SALESGROUP AS RETAILER,
        BU AS BUSINESS_UNIT,
        EANNUMBER AS EANNUMBER,
        MATL_NUM AS MATL_NUM,
        PROD_HIER_L1 AS PROD_HIER_L1,
        PROD_HIER_L2 AS PROD_HIER_L2,
        PROD_HIER_L3 AS PROD_HIER_L3,
        PROD_HIER_L4 AS PROD_HIER_L4,
        PROD_HIER_L5 AS PROD_HIER_L5,
        PROD_HIER_L6 AS PROD_HIER_L6,
        PROD_HIER_L7 AS PROD_HIER_L7,
        PROD_HIER_L8 AS PROD_HIER_L8,
        PROD_HIER_L9 AS PROD_HIER_L9,
        KPI_CHNL_WT AS KPI_CHNL_WT,
        MKT_SHARE AS MKT_SHARE,
        QUES_DESC AS QUES_TYPE,
        "y/n_flag" AS "y/n_flag",
        PRIORITY_STORE_FLAG AS PRIORITY_STORE_FLAG,
        RESPONSE,
        RESPONSE_SCORE,
        NULL AS ADD_INFO,
        CASE
            WHEN UPPER(PRESENCE) = 'TRUE' THEN 1
            ELSE 0
        END AS ACTUAL_VALUE,
        cast(target as numeric) AS REF_VALUE,
        actual AS KPI_ACTUAL_WT_VAL,
        target AS KPI_REF_VAL,
        "y/n_flag" AS KPI_REF_WT_VAL,
        PHOTO_URL,
        STORE_GRADE --gcph_category as gcph_category,
        --gcph_subcategory as gcph_subcategory
    FROM EDW_PERFECT_STORE
    WHERE UPPER(KPI) = 'MSL COMPLIANCE'
        AND UPPER(VST_STATUS) = 'COMPLETED'
        AND UPPER(MUSTCARRYITEM) = 'TRUE'
        AND UPPER(COUNTRY || CHANNEL) <> 'INDIAMT'
        AND UPPER(COUNTRY) = 'INDIA'
),
cte3 as (
    SELECT DATASET AS DATASET,
        CUSTOMERID AS CUSTOMERID,
        SALESPERSONID AS SALESPERSONID,
        VISITID AS VISITID,
        QUESTIONTEXT AS QUESTIONTEXT,
        PRODUCTID AS PRODUCTID,
        KPI AS KPI,
        SCHEDULEDDATE AS SCHEDULEDDATE,
        FISC_YR AS FISC_YR,
        FISC_PER AS FISC_PER,
        CASE
            WHEN NULLIF(TRIM(LASTNAME), '') IS NOT NULL THEN FIRSTNAME || ',' || LASTNAME
            ELSE FIRSTNAME
        END AS MERCHANDISER_NAME,
        CUSTOMERNAME AS CUSTOMERNAME,
        COUNTRY AS COUNTRY,
        STATE AS STATE,
        STOREREFERENCE AS PARENT_CUSTOMER,
        STORETYPE AS RETAIL_ENVIRONMENT,
        CHANNEL AS CHANNEL,
        SALESGROUP AS RETAILER,
        BU AS BUSINESS_UNIT,
        EANNUMBER AS EANNUMBER,
        MATL_NUM AS MATL_NUM,
        PROD_HIER_L1 AS PROD_HIER_L1,
        PROD_HIER_L2 AS PROD_HIER_L2,
        PROD_HIER_L3 AS PROD_HIER_L3,
        PROD_HIER_L4 AS PROD_HIER_L4,
        PROD_HIER_L5 AS PROD_HIER_L5,
        PROD_HIER_L6 AS PROD_HIER_L6,
        PROD_HIER_L7 AS PROD_HIER_L7,
        PROD_HIER_L8 AS PROD_HIER_L8,
        PRODUCTNAME AS PROD_HIER_L9,
        KPI_CHNL_WT AS KPI_CHNL_WT,
        MKT_SHARE AS MKT_SHARE,
        QUES_DESC AS QUES_TYPE,
        "y/n_flag" AS "y/n_flag",
        PRIORITY_STORE_FLAG AS PRIORITY_STORE_FLAG,
        RESPONSE,
        RESPONSE_SCORE,
        NULL AS ADD_INFO,
        CASE
            WHEN UPPER(PRESENCE) = 'TRUE' THEN 1
            ELSE 0
        END AS ACTUAL_VALUE,
        1 AS REF_VALUE,
        NULL AS KPI_ACTUAL_WT_VAL,
        NULL AS KPI_REF_VAL,
        NULL AS KPI_REF_WT_VAL,
        PHOTO_URL,
        STORE_GRADE --gcph_category as gcph_category,
        --gcph_subcategory as gcph_subcategory
    FROM EDW_PERFECT_STORE
    WHERE UPPER(KPI) = 'MSL COMPLIANCE'
        AND UPPER(VST_STATUS) = 'COMPLETED'
        AND UPPER(MUSTCARRYITEM) = 'TRUE'
        AND UPPER(COUNTRY || CHANNEL) = 'INDIAMT'
),
cte4 as (
    SELECT --'PS_DATA' AS IDENTIFIER,
        DATASET AS DATASET,
        CUSTOMERID AS CUSTOMERID,
        SALESPERSONID AS SALESPERSONID,
        VISITID AS VISITID,
        QUESTIONTEXT AS QUESTIONTEXT,
        PRODUCTID AS PRODUCTID,
        KPI AS KPI,
        SCHEDULEDDATE AS SCHEDULEDDATE,
        FISC_YR AS FISC_YR,
        FISC_PER AS FISC_PER,
        CASE
            WHEN NULLIF(TRIM(LASTNAME), '') IS NOT NULL THEN FIRSTNAME || ',' || LASTNAME
            ELSE FIRSTNAME
        END AS MERCHANDISER_NAME,
        CUSTOMERNAME AS CUSTOMERNAME,
        COUNTRY AS COUNTRY,
        STATE AS STATE,
        STOREREFERENCE AS PARENT_CUSTOMER,
        STORETYPE AS RETAIL_ENVIRONMENT,
        CHANNEL AS CHANNEL,
        SALESGROUP AS RETAILER,
        BU AS BUSINESS_UNIT,
        EANNUMBER AS EANNUMBER,
        MATL_NUM AS MATL_NUM,
        PROD_HIER_L1 AS PROD_HIER_L1,
        PROD_HIER_L2 AS PROD_HIER_L2,
        PROD_HIER_L3 AS PROD_HIER_L3,
        PROD_HIER_L4 AS PROD_HIER_L4,
        PROD_HIER_L5 AS PROD_HIER_L5,
        PROD_HIER_L6 AS PROD_HIER_L6,
        PROD_HIER_L7 AS PROD_HIER_L7,
        PROD_HIER_L8 AS PROD_HIER_L8,
        PRODUCTNAME AS PROD_HIER_L9,
        KPI_CHNL_WT AS KPI_CHNL_WT,
        MKT_SHARE AS MKT_SHARE,
        QUES_DESC AS QUES_TYPE,
        "y/n_flag" AS "y/n_flag",
        PRIORITY_STORE_FLAG AS PRIORITY_STORE_FLAG,
        RESPONSE,
        RESPONSE_SCORE,
        OUTOFSTOCK AS ADD_INFO,
        CASE
            WHEN UPPER(OUTOFSTOCK) <> '' THEN 1 ---need to check
            ELSE 0
        END AS ACTUAL_VALUE,
        1 AS REF_VALUE,
        NULL AS KPI_ACTUAL_WT_VAL,
        NULL AS KPI_REF_VAL,
        NULL AS KPI_REF_WT_VAL,
        PHOTO_URL,
        STORE_GRADE --gcph_category as gcph_category,
        --gcph_subcategory as gcph_subcategory
    FROM EDW_PERFECT_STORE
    WHERE UPPER(KPI) = 'OOS COMPLIANCE'
        AND UPPER(VST_STATUS) = 'COMPLETED'
        AND UPPER(MUSTCARRYITEM) = 'TRUE'
        AND UPPER(PRESENCE) = 'TRUE'
),
cte5 as (
    SELECT --'PS_DATA' AS IDENTIFIER,
        DATASET AS DATASET,
        CUSTOMERID AS CUSTOMERID,
        SALESPERSONID AS SALESPERSONID,
        VISITID AS VISITID,
        QUESTIONTEXT AS QUESTIONTEXT,
        PRODUCTID AS PRODUCTID,
        CASE
            WHEN UPPER(KPI) = 'SHARE OF SHELF' THEN 'SOS COMPLIANCE'
            WHEN UPPER(KPI) = 'SHARE OF ASSORTMENT' THEN 'SOA COMPLIANCE'
            ELSE KPI
        END AS KPI,
        SCHEDULEDDATE AS SCHEDULEDDATE,
        FISC_YR AS FISC_YR,
        FISC_PER AS FISC_PER,
        CASE
            WHEN NULLIF(TRIM(LASTNAME), '') IS NOT NULL THEN FIRSTNAME || ',' || LASTNAME
            ELSE FIRSTNAME
        END AS MERCHANDISER_NAME,
        CUSTOMERNAME AS CUSTOMERNAME,
        COUNTRY AS COUNTRY,
        STATE AS STATE,
        STOREREFERENCE AS PARENT_CUSTOMER,
        STORETYPE AS RETAIL_ENVIRONMENT,
        CHANNEL AS CHANNEL,
        SALESGROUP AS RETAILER,
        BU AS BUSINESS_UNIT,
        EANNUMBER AS EANNUMBER,
        MATL_NUM AS MATL_NUM,
        PROD_HIER_L1 AS PROD_HIER_L1,
        PROD_HIER_L2 AS PROD_HIER_L2,
        PROD_HIER_L3 AS PROD_HIER_L3,
        --CATEGORY AS PROD_HIER_L4,
        --SEGMENT AS PROD_HIER_L5,
        --BRAND AS PROD_HIER_L6,
        CASE
            WHEN UPPER(COUNTRY) = 'AUSTRALIA'
            OR UPPER(COUNTRY) = 'NEW ZEALAND' THEN PROD_HIER_L4
            ELSE CATEGORY
        END AS PROD_HIER_L4,
        CASE
            WHEN UPPER(COUNTRY) = 'AUSTRALIA'
            OR UPPER(COUNTRY) = 'NEW ZEALAND' THEN PROD_HIER_L5
            ELSE SEGMENT
        END AS PROD_HIER_L5,
        CASE
            WHEN UPPER(COUNTRY) = 'AUSTRALIA'
            OR UPPER(COUNTRY) = 'NEW ZEALAND' THEN PROD_HIER_L6
            ELSE BRAND
        END AS PROD_HIER_L6,
        PROD_HIER_L7 AS PROD_HIER_L7,
        PROD_HIER_L8 AS PROD_HIER_L8,
        PRODUCTNAME AS PROD_HIER_L9,
        KPI_CHNL_WT AS KPI_CHNL_WT,
        MKT_SHARE AS MKT_SHARE,
        QUES_DESC AS QUES_TYPE,
        "y/n_flag" AS "y/n_flag",
        PRIORITY_STORE_FLAG AS PRIORITY_STORE_FLAG,
        RESPONSE,
        RESPONSE_SCORE,
        CASE
            WHEN UPPER(COUNTRY) = 'TAIWAN' THEN ACC_REJ_REASON
            ELSE REJ_REASON
        END AS ADD_INFO,
        CASE
            WHEN UPPER(KPI) IN ('SHARE OF SHELF', 'SHARE OF ASSORTMENT') THEN (
                CASE
                    WHEN regexp_like(LTRIM(VALUE), '^[0-9]+\\.[0-9]+$')
                    OR regexp_like(LTRIM(VALUE), '^[0-9]+$') THEN CAST(VALUE AS NUMERIC(14, 4))
                    ELSE NULL
                END
            )
            WHEN UPPER(KPI) = 'PROMO COMPLIANCE' THEN (
                CASE
                    WHEN UPPER(COUNTRY) = 'INDONESIA'
                    AND UPPER("y/n_flag") = 'YES'
                    AND UPPER(POSM_EXECUTION_FLAG) = 'Y' THEN 1
                    WHEN UPPER(COUNTRY) = 'INDONESIA'
                    AND UPPER("y/n_flag") = 'YES'
                    AND UPPER(POSM_EXECUTION_FLAG) = 'N' THEN 0.5
                    WHEN UPPER(COUNTRY) = 'MALAYSIA'
                    AND UPPER("y/n_flag") = 'YES'
                    AND UPPER(POSM_EXECUTION_FLAG) = 'N' THEN 1
                    WHEN UPPER(COUNTRY) = 'MALAYSIA'
                    AND UPPER("y/n_flag") = 'YES'
                    AND UPPER(POSM_EXECUTION_FLAG) = 'Y' THEN 0.5
                    WHEN UPPER(COUNTRY) NOT IN ('INDONESIA', 'MALAYSIA')
                    AND UPPER("y/n_flag") = 'YES' THEN 1
                    ELSE 0
                END
            )
            WHEN UPPER(KPI) = 'DISPLAY COMPLIANCE' THEN (
                CASE
                    WHEN UPPER("y/n_flag") = 'YES' THEN 1
                    ELSE 0
                END
            )
            WHEN UPPER(KPI) = 'PLANOGRAM COMPLIANCE' THEN (
                CASE
                    WHEN UPPER(COUNTRY) = 'AUSTRALIA'
                    OR UPPER(COUNTRY) = 'NEW ZEALAND'
                    OR UPPER(COUNTRY) = 'CHINA' THEN CAST(ACTUAL AS NUMERIC(14, 4))
                    ELSE (
                        CASE
                            WHEN UPPER("y/n_flag") = 'YES' THEN 1
                            ELSE 0
                        END
                    )
                END
            )
            ELSE 0
        END AS ACTUAL_VALUE,
        CASE
            WHEN UPPER(KPI) IN ('SHARE OF SHELF', 'SHARE OF ASSORTMENT') THEN (
                CASE
                    WHEN regexp_like(LTRIM(VALUE), '^[0-9]+\\.[0-9]+$')
                    OR regexp_like(LTRIM(VALUE), '^[0-9]+$') THEN CAST(VALUE AS NUMERIC(14, 4))
                    ELSE NULL
                END
            )
            WHEN UPPER(KPI) = 'PROMO COMPLIANCE' THEN (
                CASE
                    WHEN UPPER("y/n_flag") = 'NO'
                    OR UPPER("y/n_flag") = 'YES' THEN 1
                    ELSE 0
                END
            )
            WHEN UPPER(KPI) = 'DISPLAY COMPLIANCE' THEN (
                CASE
                    WHEN UPPER("y/n_flag") = 'NO'
                    OR UPPER("y/n_flag") = 'YES' THEN 1
                    ELSE 0
                END
            )
            WHEN UPPER(KPI) = 'PLANOGRAM COMPLIANCE' THEN (
                CASE
                    WHEN UPPER(COUNTRY) = 'AUSTRALIA'
                    OR UPPER(COUNTRY) = 'NEW ZEALAND'
                    OR UPPER(COUNTRY) = 'CHINA' THEN CAST(TARGET AS NUMERIC(14, 4))
                    ELSE (
                        CASE
                            WHEN UPPER("y/n_flag") = 'YES'
                            OR UPPER("y/n_flag") = 'NO' THEN 1
                            ELSE 0
                        END
                    )
                END
            )
            ELSE 0
        END AS REF_VALUE,
        NULL AS KPI_ACTUAL_WT_VAL,
        NULL AS KPI_REF_VAL,
        NULL AS KPI_REF_WT_VAL,
        PHOTO_URL,
        STORE_GRADE --gcph_category as gcph_category,
        --gcph_subcategory as gcph_subcategory
    FROM EDW_PERFECT_STORE
    WHERE UPPER(KPI) NOT IN ('MSL COMPLIANCE', 'OOS COMPLIANCE')
),
ps_data as (
    select *
    from cte1
    union all
    select *
    from cte2
    union all
    select *
    from cte3
    union all
    select *
    from cte4
    union all
    select *
    from cte5
),
final as (
    SELECT PS_DATA.DATASET,
        PS_DATA.CUSTOMERID,
        PS_DATA.SALESPERSONID,
        PS_DATA.VISITID,
        PS_DATA.QUESTIONTEXT,
        PS_DATA.PRODUCTID,
        PS_DATA.KPI,
        PS_DATA.SCHEDULEDDATE,
        CASE
            WHEN UPPER(PS_DATA.COUNTRY) IN ('AUSTRALIA', 'NEW ZEALAND') THEN to_date(
                ADD_MONTHS (
                    TO_DATE(
                        SUBSTRING(
                            to_date(convert_timezone('UTC', current_timestamp())),
                            1,
                            7
                        ) || '-15',
                        'YYYY-MM-DD'
                    ),
                    -2
                )
            )
            ELSE to_date(
                ADD_MONTHS (
                    TO_DATE(
                        SUBSTRING(
                            to_date(convert_timezone('UTC', current_timestamp())),
                            1,
                            7
                        ) || '-15',
                        'YYYY-MM-DD'
                    ),
                    -1
                )
            )
        END AS LATESTDATE,
        PS_DATA.FISC_YR,
        PS_DATA.FISC_PER,
        PS_DATA.MERCHANDISER_NAME,
        PS_DATA.CUSTOMERNAME,
        PS_DATA.COUNTRY,
        PS_DATA.STATE,
        PS_DATA.PARENT_CUSTOMER,
        PS_DATA.RETAIL_ENVIRONMENT,
        PS_DATA.CHANNEL,
        PS_DATA.RETAILER,
        PS_DATA.BUSINESS_UNIT,
        PS_DATA.EANNUMBER,
        PS_DATA.MATL_NUM,
        PS_DATA.PROD_HIER_L1,
        PS_DATA.PROD_HIER_L2,
        PS_DATA.PROD_HIER_L3,
        PS_DATA.PROD_HIER_L4,
        PS_DATA.PROD_HIER_L5,
        PS_DATA.PROD_HIER_L6,
        PS_DATA.PROD_HIER_L7,
        PS_DATA.PROD_HIER_L8,
        PS_DATA.PROD_HIER_L9,
        PS_DATA.QUES_TYPE,
        PS_DATA."y/n_flag",
        PS_DATA.PRIORITY_STORE_FLAG,
        PS_DATA.ADD_INFO,
        PS_DATA.RESPONSE,
        PS_DATA.RESPONSE_SCORE,
        PS_DATA.KPI_CHNL_WT,
        PS_DATA.MKT_SHARE,
        SALIENCE.COVERAGE AS SALIENCE_VAL,
        CHNL_WEIGHT.WEIGHT AS CHANNEL_WEIGHTAGE,
        --- need to confirm
        PS_DATA.ACTUAL_VALUE,
        PS_DATA.REF_VALUE,
        PS_DATA.KPI_ACTUAL_WT_VAL,
        --- new column addition as part of India GT
        PS_DATA.KPI_REF_VAL,
        --- new column addition as part of India GT
        PS_DATA.KPI_REF_WT_VAL,
        --- new column addition as part of India GT
        PS_DATA.PHOTO_URL,
        --PS_DATA.gcph_category, -- new column addition as part of AU size of prize
        --PS_DATA.gcph_subcategory -- new column addition as part of AU size of prize 
        PS_DATA.STORE_GRADE -- new column addition as part od Pacific MSL Change
    FROM ps_data 
    --JOIN (SELECT 'PS_DATA' AS IDENTIFIER, TO_DATE(DATE_PART('YEAR',convert_timezone('UTC', current_timestamp())) ||DATE_PART ('MONTH',convert_timezone('UTC', current_timestamp())) ||15,'YYYYMMDD') AS LATEST_DATE FROM EDW_PERFECT_STORE) MAX_DATE ON MAX_DATE.IDENTIFIER = PS_DATA.IDENTIFIER
        LEFT JOIN itg_mds_rg_ps_channel_weights CHNL_WEIGHT ON UPPER (PS_DATA.COUNTRY) = UPPER (CHNL_WEIGHT.COUNTRY)
        AND (
            CASE
                WHEN UPPER (CHNL_WEIGHT.channel_re) = 'RE' THEN UPPER (PS_DATA.RETAIL_ENVIRONMENT)
                ELSE UPPER (PS_DATA.CHANNEL)
            END
        ) = UPPER (CHNL_WEIGHT.CHANNEL_RE_VALUE)
        LEFT JOIN ITG_MDS_RG_PS_MARKET_COVERAGE SALIENCE ON UPPER (PS_DATA.COUNTRY) = UPPER (SALIENCE.CNTY_NM)
        AND UPPER (PS_DATA.CHANNEL) = UPPER (SALIENCE.CHANNEL)
    WHERE DATE_PART('YEAR', SCHEDULEDDATE) >= (
            DATE_PART(
                'year',
                convert_timezone('UTC', current_timestamp())
            ) -1
        )
)
select * from final