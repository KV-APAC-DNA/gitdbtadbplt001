with edw_perfect_store_kpi_data as 
(
    select * from aspedw_integration.edw_perfect_store_kpi_data
),
edw_vw_os_time_dim as 
(
    select * from sgpedw_integration__edw_vw_os_time_dim
),
msl as
(   
    SELECT 
        DATASET,
        --CUSTOMERID,
        --SALESPERSONID,
        KPI,
        TO_DATE(YM || '15', 'YYYYMMDD') AS SCHEDULEDDATE,
        LATESTDATE,
        FISC_YR,
        FISC_PER,
        --MERCHANDISER_NAME,
        --CUSTOMERNAME,
        COUNTRY,
        --STATE,
        PARENT_CUSTOMER,
        RETAIL_ENVIRONMENT,
        CHANNEL,
        --RETAILER,
        --BUSINESS_UNIT,
        prod_hier_l4,
        PRIORITY_STORE_FLAG,
        KPI_CHNL_WT,
        CHANNEL_WEIGHTAGE,
        mkt_share,
        SALIENCE_VAL,
        ACTUAL_VALUE,
        REF_VALUE,
        KPI_ACTUAL_WT_VAL,
        KPI_REF_VAL,
        KPI_REF_WT_VAL,
        --PHOTO_URL,
        --gcph_category,
        --gcph_subcategory,
        compliance,
        gap_to_target
    FROM 
        (
            SELECT DATASET,
                --CUSTOMERID,
                --SALESPERSONID,
                KPI,
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM') AS YM,
                LATESTDATE,
                FISC_YR,
                FISC_PER,
                --MERCHANDISER_NAME,
                --CUSTOMERNAME,
                COUNTRY,
                --STATE,
                PARENT_CUSTOMER,
                RETAIL_ENVIRONMENT,
                CHANNEL,
                --RETAILER,
                --BUSINESS_UNIT,
                prod_hier_l4,
                PRIORITY_STORE_FLAG,
                MAX(KPI_CHNL_WT) AS KPI_CHNL_WT,
                MAX(CHANNEL_WEIGHTAGE) AS CHANNEL_WEIGHTAGE,
                max(mkt_share) as mkt_share,
                MAX(SALIENCE_VAL) AS SALIENCE_VAL,
                SUM(ACTUAL_VALUE) AS ACTUAL_VALUE,
                SUM(REF_VALUE) AS REF_VALUE,
                SUM(KPI_ACTUAL_WT_VAL) AS KPI_ACTUAL_WT_VAL,
                SUM(KPI_REF_VAL) AS KPI_REF_VAL,
                SUM(KPI_REF_WT_VAL) AS KPI_REF_WT_VAL,
                --PHOTO_URL,
                --gcph_category,
                --gcph_subcategory,
                trunc((sum(ACTUAL_VALUE) / sum(REF_VALUE)),3) as compliance,
                case
                    when (
                        round((max(mkt_share)) -trunc((sum(ACTUAL_VALUE) / sum(REF_VALUE)),4),3)
                    ) <= '0' THEN '0'
                    else (
                        round((max(mkt_share)) -trunc((sum(ACTUAL_VALUE) / sum(REF_VALUE)),4),3)
                    )
                END as gap_to_target
            FROM 
                (
                    SELECT 'MSL COMPLIANCE' AS DATASET,
                        --CUSTOMERID,
                        --SALESPERSONID,
                        'SIZE OF THE PRIZE' AS KPI,
                        SCHEDULEDDATE,
                        LATESTDATE,
                        FISC_YR,
                        FISC_PER,
                        --MERCHANDISER_NAME,
                        --CUSTOMERNAME,
                        COUNTRY,
                        --STATE,
                        PARENT_CUSTOMER,
                        RETAIL_ENVIRONMENT,
                        CHANNEL,
                        --RETAILER,
                        --BUSINESS_UNIT,
                        prod_hier_l4,
                        PRIORITY_STORE_FLAG,
                        KPI_CHNL_WT,
                        mkt_share,
                        CHANNEL_WEIGHTAGE,
                        SALIENCE_VAL,
                        ACTUAL_VALUE,
                        REF_VALUE,
                        ROUND(ACTUAL_VALUE * WEIGHT_MSL, 4) AS KPI_ACTUAL_WT_VAL,
                        REF_VALUE AS KPI_REF_VAL,
                        ROUND(REF_VALUE * WEIGHT_MSL, 4) AS KPI_REF_WT_VAL --gcph_category,
                        --gcph_subcategory
                    FROM EDW_PERFECT_STORE_KPI_DATA
                    WHERE UPPER(KPI) = 'MSL COMPLIANCE'
                        AND UPPER(PRIORITY_STORE_FLAG) = 'Y'
                        AND UPPER(COUNTRY) in ('AUSTRALIA', 'NEW ZEALAND')
                )
            GROUP BY DATASET,
                --CUSTOMERID,
                --SALESPERSONID,
                KPI,
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM'),
                LATESTDATE,
                FISC_YR,
                FISC_PER,
                --MERCHANDISER_NAME,
                --CUSTOMERNAME,
                COUNTRY,
                --STATE,
                PARENT_CUSTOMER,
                RETAIL_ENVIRONMENT,
                CHANNEL,
                --RETAILER,
                --BUSINESS_UNIT,
                prod_hier_l4,
                PRIORITY_STORE_FLAG --PHOTO_URL
                --gcph_category,
                --gcph_subcategory
        ) msl
),
osa as
(   
    SELECT 
        DATASET,
        --CUSTOMERID,
        --SALESPERSONID,
        KPI,
        TO_DATE(YM || '15', 'YYYYMMDD') AS SCHEDULEDDATE,
        LATESTDATE,
        FISC_YR,
        FISC_PER,
        --MERCHANDISER_NAME,
        --CUSTOMERNAME,
        COUNTRY,
        --STATE,
        PARENT_CUSTOMER,
        RETAIL_ENVIRONMENT,
        CHANNEL,
        --RETAILER,
        --BUSINESS_UNIT,
        prod_hier_l4,
        PRIORITY_STORE_FLAG,
        KPI_CHNL_WT,
        CHANNEL_WEIGHTAGE,
        mkt_share,
        SALIENCE_VAL,
        round(ACTUAL_VALUE,4) as ACTUAL_VALUE,
        round(REF_VALUE,4) as REF_VALUE,
        round(KPI_ACTUAL_WT_VAL,4) as KPI_ACTUAL_WT_VAL,
        round(KPI_REF_VAL,4) as KPI_REF_VAL,
        round(KPI_REF_WT_VAL,4) as KPI_REF_WT_VAL,
        --PHOTO_URL,
        --gcph_category,
        --gcph_subcategory,
        compliance,
        gap_to_target
    FROM 
        (
            SELECT 
                DATASET,
                --CUSTOMERID,
                --SALESPERSONID,
                KPI,
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM') AS YM,
                LATESTDATE,
                FISC_YR,
                FISC_PER,
                --MERCHANDISER_NAME,
                --CUSTOMERNAME,
                COUNTRY,
                --STATE,
                PARENT_CUSTOMER,
                RETAIL_ENVIRONMENT,
                CHANNEL,
                --RETAILER,
                --BUSINESS_UNIT,
                prod_hier_l4,
                PRIORITY_STORE_FLAG,
                MAX(KPI_CHNL_WT) AS KPI_CHNL_WT,
                MAX(CHANNEL_WEIGHTAGE) AS CHANNEL_WEIGHTAGE,
                max(mkt_share) as mkt_share,
                MAX(SALIENCE_VAL) AS SALIENCE_VAL,
                SUM(ACTUAL_VALUE) AS ACTUAL_VALUE,
                SUM(REF_VALUE) AS REF_VALUE,
                SUM(KPI_ACTUAL_WT_VAL) AS KPI_ACTUAL_WT_VAL,
                SUM(KPI_REF_VAL) AS KPI_REF_VAL,
                SUM(KPI_REF_WT_VAL) AS KPI_REF_WT_VAL,
                --PHOTO_URL,
                --gcph_category,
                --gcph_subcategory,
                TRUNC((sum(ACTUAL_VALUE)) /(sum(REF_VALUE)),3) as compliance,
                case
                    when (
                        round((max(mkt_share)) -(sum(ACTUAL_VALUE) / sum(REF_VALUE)),3)
                    ) <= '0' THEN '0'
                    ELSE (
                        round((max(mkt_share)) -(sum(ACTUAL_VALUE) / sum(REF_VALUE)),3)
                    )
                END as gap_to_target
            FROM 
                (
                    SELECT 'OOS COMPLIANCE' AS DATASET,
                        --CUSTOMERID,
                        --SALESPERSONID,
                        'SIZE OF THE PRIZE' AS KPI,
                        SCHEDULEDDATE,
                        LATESTDATE,
                        FISC_YR,
                        FISC_PER,
                        --MERCHANDISER_NAME,
                        --CUSTOMERNAME,
                        COUNTRY,
                        --STATE,
                        PARENT_CUSTOMER,
                        RETAIL_ENVIRONMENT,
                        CHANNEL,
                        --RETAILER,
                        --BUSINESS_UNIT,
                        prod_hier_l4,
                        PRIORITY_STORE_FLAG,
                        KPI_CHNL_WT,
                        mkt_share,
                        CHANNEL_WEIGHTAGE,
                        SALIENCE_VAL,
                        CASE
                            WHEN ACTUAL_VALUE = 0 THEN 1
                            ELSE 0
                        END AS ACTUAL_VALUE,
                        REF_VALUE,
                        ROUND(
                            (
                                CASE
                                    WHEN ACTUAL_VALUE = 0 THEN 1
                                    ELSE 0
                                END
                            ) * WEIGHT_OOS,
                            4
                        ) AS KPI_ACTUAL_WT_VAL,
                        REF_VALUE AS KPI_REF_VAL,
                        ROUND(REF_VALUE * WEIGHT_OOS, 4) AS KPI_REF_WT_VAL --PHOTO_URL,
                        --gcph_category,
                        --gcph_subcategory
                    FROM EDW_PERFECT_STORE_KPI_DATA
                    WHERE UPPER(KPI) = 'OOS COMPLIANCE'
                        AND upper(PRIORITY_STORE_FLAG) = 'Y'
                        AND UPPER(COUNTRY) in ('AUSTRALIA', 'NEW ZEALAND')
                )
            GROUP BY DATASET,
                --CUSTOMERID,
                --SALESPERSONID,
                KPI,
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM'),
                LATESTDATE,
                FISC_YR,
                FISC_PER,
                --MERCHANDISER_NAME,
                --CUSTOMERNAME,
                COUNTRY,
                --STATE,
                PARENT_CUSTOMER,
                RETAIL_ENVIRONMENT,
                CHANNEL,
                --RETAILER,
                --BUSINESS_UNIT,
                prod_hier_l4,
                PRIORITY_STORE_FLAG
        ) OSA
),
promo as
(   
    SELECT 
        DATASET,
        --CUSTOMERID,
        --SALESPERSONID,
        KPI,
        TO_DATE(YM || '15', 'YYYYMMDD') AS SCHEDULEDDATE,
        LATESTDATE,
        FISC_YR,
        FISC_PER,
        --MERCHANDISER_NAME,
        --CUSTOMERNAME,
        COUNTRY,
        --STATE,
        PARENT_CUSTOMER,
        RETAIL_ENVIRONMENT,
        CHANNEL,
        --RETAILER,
        --BUSINESS_UNIT,
        prod_hier_l4,
        PRIORITY_STORE_FLAG,
        KPI_CHNL_WT,
        CHANNEL_WEIGHTAGE,
        mkt_share,
        SALIENCE_VAL,
        round(ACTUAL_VALUE,4) as ACTUAL_VALUE,
        round(REF_VALUE,4) as REF_VALUE,
        round(KPI_ACTUAL_WT_VAL,4) as KPI_ACTUAL_WT_VAL,
        round(KPI_REF_VAL,4) as KPI_REF_VAL,
        round(KPI_REF_WT_VAL,4) as KPI_REF_WT_VAL,
        --PHOTO_URL,
        --gcph_category,
        --gcph_subcategory,
        compliance,
        gap_to_target
    FROM 
        (
            SELECT DATASET,
                --CUSTOMERID,
                --SALESPERSONID,
                KPI,
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM') AS YM,
                LATESTDATE,
                FISC_YR,
                FISC_PER,
                --MERCHANDISER_NAME,
                --CUSTOMERNAME,
                COUNTRY,
                --STATE,
                PARENT_CUSTOMER,
                RETAIL_ENVIRONMENT,
                CHANNEL,
                --RETAILER,
                --BUSINESS_UNIT,
                prod_hier_l4,
                PRIORITY_STORE_FLAG,
                MAX(KPI_CHNL_WT) AS KPI_CHNL_WT,
                MAX(CHANNEL_WEIGHTAGE) AS CHANNEL_WEIGHTAGE,
                max(mkt_share) as mkt_share,
                MAX(SALIENCE_VAL) AS SALIENCE_VAL,
                SUM(ACTUAL_VALUE) AS ACTUAL_VALUE,
                SUM(REF_VALUE) AS REF_VALUE,
                SUM(KPI_ACTUAL_WT_VAL) AS KPI_ACTUAL_WT_VAL,
                SUM(KPI_REF_VAL) AS KPI_REF_VAL,
                SUM(KPI_REF_WT_VAL) AS KPI_REF_WT_VAL,
                --PHOTO_URL,
                --gcph_category,
                --gcph_subcategory,
                ((sum(ACTUAL_VALUE)) /(sum(REF_VALUE))) as compliance,
                case
                    when (
                        round((max(mkt_share)) -trunc((sum(ACTUAL_VALUE) / sum(REF_VALUE)),4),3)
                    ) <= '0' THEN '0'
                    ELSE (
                        round((max(mkt_share)) -trunc((sum(ACTUAL_VALUE) / sum(REF_VALUE)),4),3)
                    )
                END as gap_to_target
            FROM (
                    SELECT 'PROMO COMPLIANCE' AS DATASET,
                        --CUSTOMERID,
                        --SALESPERSONID,
                        'SIZE OF THE PRIZE' AS KPI,
                        SCHEDULEDDATE,
                        LATESTDATE,
                        FISC_YR,
                        FISC_PER,
                        --MERCHANDISER_NAME,
                        --CUSTOMERNAME,
                        COUNTRY,
                        --STATE,
                        PARENT_CUSTOMER,
                        RETAIL_ENVIRONMENT,
                        CHANNEL,
                        --RETAILER,
                        --BUSINESS_UNIT,
                        prod_hier_l4,
                        PRIORITY_STORE_FLAG,
                        KPI_CHNL_WT,
                        mkt_share,
                        CHANNEL_WEIGHTAGE,
                        SALIENCE_VAL,
                        ACTUAL_VALUE,
                        REF_VALUE,
                        ROUND(ACTUAL_VALUE * WEIGHT_PROMO, 4) AS KPI_ACTUAL_WT_VAL,
                        REF_VALUE AS KPI_REF_VAL,
                        ROUND(REF_VALUE * WEIGHT_PROMO, 4) AS KPI_REF_WT_VAL --PHOTO_URL,
                    FROM EDW_PERFECT_STORE_KPI_DATA
                    WHERE UPPER(KPI) = 'PROMO COMPLIANCE'
                        AND upper(PRIORITY_STORE_FLAG) = 'Y'
                        AND UPPER(COUNTRY) in ('AUSTRALIA', 'NEW ZEALAND')
                )
            GROUP BY DATASET,
                --CUSTOMERID,
                --SALESPERSONID,
                KPI,
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM'),
                LATESTDATE,
                FISC_YR,
                FISC_PER,
                --MERCHANDISER_NAME,
                --CUSTOMERNAME,
                COUNTRY,
                --STATE,
                PARENT_CUSTOMER,
                RETAIL_ENVIRONMENT,
                CHANNEL,
                --RETAILER,
                --BUSINESS_UNIT,
                prod_hier_l4,
                PRIORITY_STORE_FLAG
        ) PROMO
),
pog_anz as
(   
    SELECT 
        DATASET,
        --CUSTOMERID,
        --SALESPERSONID,
        KPI,
        TO_DATE(YM || '15', 'YYYYMMDD') AS SCHEDULEDDATE,
        LATESTDATE,
        FISC_YR,
        FISC_PER,
        --MERCHANDISER_NAME,
        --CUSTOMERNAME,
        COUNTRY,
        --STATE,
        PARENT_CUSTOMER,
        RETAIL_ENVIRONMENT,
        CHANNEL,
        --RETAILER,
        --BUSINESS_UNIT,
        prod_hier_l4,
        PRIORITY_STORE_FLAG,
        KPI_CHNL_WT,
        CHANNEL_WEIGHTAGE,
        mkt_share,
        SALIENCE_VAL,
        round(ACTUAL_VALUE,4) as ACTUAL_VALUE,
        round(REF_VALUE,4) as REF_VALUE,
        round(KPI_ACTUAL_WT_VAL,4) as KPI_ACTUAL_WT_VAL,
        round(KPI_REF_VAL,4) as KPI_REF_VAL,
        round(KPI_REF_WT_VAL,4) as KPI_REF_WT_VAL,
        --PHOTO_URL,
        --gcph_category,
        --gcph_subcategory,
        compliance,
        gap_to_target
    FROM 
        (
            SELECT DATASET,
                --CUSTOMERID,
                --SALESPERSONID,
                KPI,
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM') AS YM,
                LATESTDATE,
                FISC_YR,
                FISC_PER,
                --MERCHANDISER_NAME,
                --CUSTOMERNAME,
                COUNTRY,
                --STATE,
                PARENT_CUSTOMER,
                RETAIL_ENVIRONMENT,
                CHANNEL,
                --RETAILER,
                --BUSINESS_UNIT,
                prod_hier_l4,
                PRIORITY_STORE_FLAG,
                MAX(KPI_CHNL_WT) AS KPI_CHNL_WT,
                MAX(CHANNEL_WEIGHTAGE) AS CHANNEL_WEIGHTAGE,
                max(mkt_share) as mkt_share,
                MAX(SALIENCE_VAL) AS SALIENCE_VAL,
                SUM(ACTUAL_VALUE) AS ACTUAL_VALUE,
                SUM(REF_VALUE) AS REF_VALUE,
                SUM(KPI_ACTUAL_WT_VAL * WEIGHT_PLANOGRAM) AS KPI_ACTUAL_WT_VAL,
                SUM(KPI_REF_VAL) AS KPI_REF_VAL,
                SUM(KPI_REF_WT_VAL * WEIGHT_PLANOGRAM) AS KPI_REF_WT_VAL,
                --PHOTO_URL,
                --gcph_category,
                --gcph_subcategory,
                case
                    when ((sum(ACTUAL_VALUE)) /(sum(nullif(REF_VALUE, 0)))) > 1 THEN '1'
                    ELSE ((sum(ACTUAL_VALUE)) /(sum(nullif(REF_VALUE, 0))))
                END as compliance,
                case
                    when (
                        (max(mkt_share)) -(
                            case
                                when ((sum(ACTUAL_VALUE)) /(sum(nullif(REF_VALUE, 0)))) > 1 THEN '1'
                                ELSE ((sum(ACTUAL_VALUE)) /(sum(nullif(REF_VALUE, 0))))
                            END
                        )
                    ) <= '0' THEN '0'
                    ELSE (
                        (max(mkt_share)) -(
                            case
                                when ((sum(ACTUAL_VALUE)) /(sum(nullif(REF_VALUE, 0)))) > 1 THEN '1'
                                ELSE ((sum(ACTUAL_VALUE)) /(sum(nullif(REF_VALUE, 0))))
                            END
                        )
                    )
                END as gap_to_target
            FROM (
                    SELECT 'PLANOGRAM COMPLIANCE' AS DATASET,
                        --CUSTOMERID,
                        --SALESPERSONID,
                        'SIZE OF THE PRIZE' AS KPI,
                        SCHEDULEDDATE,
                        LATESTDATE,
                        FISC_YR,
                        FISC_PER,
                        --MERCHANDISER_NAME,
                        --CUSTOMERNAME,
                        COUNTRY,
                        --STATE,
                        PARENT_CUSTOMER,
                        RETAIL_ENVIRONMENT,
                        CHANNEL,
                        --RETAILER,
                        --BUSINESS_UNIT,
                        prod_hier_l4,
                        PRIORITY_STORE_FLAG,
                        KPI_CHNL_WT,
                        mkt_share,
                        CHANNEL_WEIGHTAGE,
                        ROUND(WEIGHT_PLANOGRAM, 4) AS WEIGHT_PLANOGRAM,
                        SALIENCE_VAL,
                        CASE
                            WHEN MKT_SHARE IS NOT NULL THEN ACTUAL_VALUE
                            ELSE NULL
                        END AS ACTUAL_VALUE,
                        ROUND(REF_VALUE * MKT_SHARE, 4) AS REF_VALUE,
                        CASE
                            WHEN MKT_SHARE IS NOT NULL THEN ACTUAL_VALUE
                            ELSE NULL
                        END AS KPI_ACTUAL_WT_VAL,
                        ROUND(REF_VALUE * MKT_SHARE, 4) AS KPI_REF_VAL,
                        ROUND(REF_VALUE * MKT_SHARE, 4) AS KPI_REF_WT_VAL --PHOTO_URL,
                    FROM EDW_PERFECT_STORE_KPI_DATA
                    WHERE UPPER(KPI) = 'PLANOGRAM COMPLIANCE'
                        AND upper(PRIORITY_STORE_FLAG) = 'Y'
                        AND UPPER(COUNTRY) IN ('AUSTRALIA', 'NEW ZEALAND')
                )
            GROUP BY DATASET,
                --CUSTOMERID,
                --SALESPERSONID,
                KPI,
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM'),
                LATESTDATE,
                FISC_YR,
                FISC_PER,
                --MERCHANDISER_NAME,
                --CUSTOMERNAME,
                COUNTRY,
                --STATE,
                PARENT_CUSTOMER,
                RETAIL_ENVIRONMENT,
                CHANNEL,
                --RETAILER,
                --BUSINESS_UNIT,
                prod_hier_l4,
                PRIORITY_STORE_FLAG
        ) POG_ANZ
),
sos as 
(   
    SELECT 
        DATASET,
        --CUSTOMERID,
        --SALESPERSONID,
        KPI,
        TO_DATE(YM || '15', 'YYYYMMDD') AS SCHEDULEDDATE,
        LATESTDATE,
        FISC_YR,
        FISC_PER,
        --MERCHANDISER_NAME,
        --CUSTOMERNAME,
        COUNTRY,
        --STATE,
        PARENT_CUSTOMER,
        RETAIL_ENVIRONMENT,
        CHANNEL,
        --RETAILER,
        --BUSINESS_UNIT,
        prod_hier_l4,
        PRIORITY_STORE_FLAG,
        KPI_CHNL_WT,
        CHANNEL_WEIGHTAGE,
        mkt_share,
        SALIENCE_VAL,
        round(ACTUAL_VALUE,4) as ACTUAL_VALUE,
        round(REF_VALUE,4) as REF_VALUE,
        round(KPI_ACTUAL_WT_VAL,4) as KPI_ACTUAL_WT_VAL,
        round(KPI_REF_VAL,4) as KPI_REF_VAL,
        round(KPI_REF_WT_VAL,4) as KPI_REF_WT_VAL,
        --PHOTO_URL,
        --gcph_category,
        --gcph_subcategory,
        compliance,
        gap_to_target
    FROM 
        (
            SELECT DATASET,
                --CUSTOMERID,
                --SALESPERSONID,
                KPI,
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM') AS YM,
                LATESTDATE,
                FISC_YR,
                FISC_PER,
                --MERCHANDISER_NAME,
                --CUSTOMERNAME,
                COUNTRY,
                --STATE,
                PARENT_CUSTOMER,
                RETAIL_ENVIRONMENT,
                CHANNEL,
                --RETAILER,
                --BUSINESS_UNIT,
                prod_hier_l4,
                PRIORITY_STORE_FLAG,
                MAX(KPI_CHNL_WT) AS KPI_CHNL_WT,
                MAX(CHANNEL_WEIGHTAGE) AS CHANNEL_WEIGHTAGE,
                max(mkt_share) as mkt_share,
                MAX(SALIENCE_VAL) AS SALIENCE_VAL,
                SUM(NUM_VALUE) AS ACTUAL_VALUE,
                SUM(DEN_VALUE) AS REF_VALUE,
                SUM(NUM_VALUE * WEIGHT_SOS) AS KPI_ACTUAL_WT_VAL,
                SUM(DEN_VALUE) AS KPI_REF_VAL,
                SUM(DEN_VALUE * WEIGHT_SOS) AS KPI_REF_WT_VAL,
                --PHOTO_URL,
                --gcph_category,
                --gcph_subcategory,
                case
                    when ((sum(NUM_VALUE)) /(sum(nullif(DEN_VALUE, 0)))) > 1 THEN '1'
                    ELSE ((sum(NUM_VALUE)) /(sum(nullif(DEN_VALUE, 0))))
                END as compliance,
                case
                    when (
                        (max(mkt_share)) -(
                            case
                                when ((sum(NUM_VALUE)) /(sum(nullif(DEN_VALUE, 0)))) > 1 THEN '1'
                                ELSE ((sum(NUM_VALUE)) /(sum(nullif(DEN_VALUE, 0))))
                            END
                        )
                    ) <= '0' THEN '0'
                    ELSE (
                        (max(mkt_share)) -(
                            case
                                when ((sum(NUM_VALUE)) /(sum(nullif(DEN_VALUE, 0)))) > 1 THEN '1'
                                ELSE ((sum(NUM_VALUE)) /(sum(nullif(DEN_VALUE, 0))))
                            END
                        )
                    )
                END as gap_to_target
            FROM 
                (
                    SELECT DATASET,
                        --CUSTOMERID,
                        --SALESPERSONID,
                        KPI,
                        TRANS.SCHEDULEDDATE,
                        LATESTDATE,
                        FISC_YR,
                        FISC_PER,
                        --MERCHANDISER_NAME,
                        --TRANS.CUSTOMERNAME,
                        TRANS.COUNTRY,
                        --STATE,
                        PARENT_CUSTOMER,
                        RETAIL_ENVIRONMENT,
                        CHANNEL,
                        --RETAILER,
                        --BUSINESS_UNIT,
                        PRIORITY_STORE_FLAG,
                        TRANS.PROD_HIER_L4,
                        TRANS.PROD_HIER_L5,
                        MAX(KPI_CHNL_WT) AS KPI_CHNL_WT,
                        max(mkt_share) as mkt_share,
                        MAX(CHANNEL_WEIGHTAGE) AS CHANNEL_WEIGHTAGE,
                        MAX(WEIGHT_SOS) AS WEIGHT_SOS,
                        MAX(SALIENCE_VAL) AS SALIENCE_VAL,
                        round(SUM(NUM_VALUE),4) AS NUM_VALUE,
                        round(SUM(DEN_VALUE),4) AS DEN_VALUE
                    FROM 
                        (
                            SELECT 'SOS COMPLIANCE' AS DATASET,
                                --CUSTOMERID,
                                --SALESPERSONID,
                                'SIZE OF THE PRIZE' AS KPI,
                                SCHEDULEDDATE,
                                LATESTDATE,
                                FISC_YR,
                                FISC_PER,
                                --MERCHANDISER_NAME,
                                CUSTOMERNAME,
                                COUNTRY,
                                --STATE,
                                PARENT_CUSTOMER,
                                RETAIL_ENVIRONMENT,
                                CHANNEL,
                                --RETAILER,
                                --BUSINESS_UNIT,
                                PRIORITY_STORE_FLAG,
                                KPI_CHNL_WT,
                                mkt_share,
                                CHANNEL_WEIGHTAGE,
                                ROUND(WEIGHT_SOS, 4) AS WEIGHT_SOS,
                                SALIENCE_VAL,
                                PROD_HIER_L4,
                                PROD_HIER_L5,
                                QUES_TYPE,
                                CASE
                                    WHEN UPPER(QUES_TYPE) = 'NUMERATOR'
                                    AND MKT_SHARE IS NOT NULL THEN ACTUAL_VALUE
                                    ELSE NULL
                                END AS NUM_VALUE,
                                CASE
                                    WHEN UPPER(QUES_TYPE) = 'DENOMINATOR' THEN ROUND(ACTUAL_VALUE * MKT_SHARE, 4)
                                    ELSE NULL
                                END AS DEN_VALUE --PHOTO_URL,
                            FROM EDW_PERFECT_STORE_KPI_DATA
                            WHERE UPPER(KPI) = 'SOS COMPLIANCE'
                                AND UPPER(COUNTRY) in ('AUSTRALIA', 'NEW ZEALAND')
                                AND (
                                    REGEXP_LIKE(ACTUAL_VALUE,'^[0-9]+\\.[0-9]+$')
                                    OR REGEXP_LIKE(ACTUAL_VALUE, '^[0-9]+$')
                                )
                        ) TRANS
                        INNER JOIN (
                            SELECT DISTINCT COUNTRY,
                                CUSTOMERNAME,
                                SCHEDULEDDATE,
                                PROD_HIER_L4,
                                PROD_HIER_L5
                            FROM EDW_PERFECT_STORE_KPI_DATA
                            WHERE UPPER(KPI) = 'SOS COMPLIANCE'
                                AND UPPER(COUNTRY) in ('AUSTRALIA', 'NEW ZEALAND')
                                AND (
                                    REGEXP_LIKE(ACTUAL_VALUE,'^[0-9]+\\.[0-9]+$')
                                    OR REGEXP_LIKE(ACTUAL_VALUE, '^[0-9]+$')
                                )
                            GROUP BY COUNTRY,
                                CUSTOMERNAME,
                                SCHEDULEDDATE,
                                PROD_HIER_L4,
                                PROD_HIER_L5
                            HAVING COUNT(DISTINCT QUES_TYPE) = 2
                        ) NUM_DEN ON NVL (NUM_DEN.COUNTRY, 'X') = NVL (TRANS.COUNTRY, 'X')
                        AND NVL (NUM_DEN.CUSTOMERNAME, 'X') = NVL (TRANS.CUSTOMERNAME, 'X')
                        AND NUM_DEN.SCHEDULEDDATE = TRANS.SCHEDULEDDATE
                        AND NVL (NUM_DEN.PROD_HIER_L4, 'X') = NVL (TRANS.PROD_HIER_L4, 'X')
                        AND NVL (NUM_DEN.PROD_HIER_L5, 'X') = NVL (TRANS.PROD_HIER_L5, 'X')
                    GROUP BY DATASET,
                        --CUSTOMERID,
                        --SALESPERSONID,
                        KPI,
                        TRANS.SCHEDULEDDATE,
                        LATESTDATE,
                        FISC_YR,
                        FISC_PER,
                        --MERCHANDISER_NAME,
                        TRANS.CUSTOMERNAME,
                        TRANS.COUNTRY,
                        --STATE,
                        PARENT_CUSTOMER,
                        RETAIL_ENVIRONMENT,
                        CHANNEL,
                        --RETAILER,
                        --BUSINESS_UNIT,
                        PRIORITY_STORE_FLAG,
                        TRANS.PROD_HIER_L4,
                        TRANS.PROD_HIER_L5
                )
            GROUP BY DATASET,
                --CUSTOMERID,
                --SALESPERSONID,
                KPI,
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM'),
                LATESTDATE,
                FISC_YR,
                FISC_PER,
                --MERCHANDISER_NAME,
                --CUSTOMERNAME,
                COUNTRY,
                --STATE,
                PARENT_CUSTOMER,
                RETAIL_ENVIRONMENT,
                CHANNEL,
                --RETAILER,
                --BUSINESS_UNIT,
                prod_hier_l4,
                PRIORITY_STORE_FLAG -- take only 'Y'
        ) SOS
    where SOS.PRIORITY_STORE_FLAG = 'Y'
),
size_of_price as 
(
    select
        null as hashkey,
        null as hash_row,
        dataset,
        null as customerid,
        null as salespersonid,
        null as visitid,
        null as questiontext,
        null as productid,
        kpi,
        scheduleddate,
        latestdate,
        fisc_yr,
        fisc_per,
        null as merchandiser_name,
        null as customername,
        country,
        null as state,
        parent_customer,
        retail_environment,
        channel,
        null as retailer,
        null as business_unit,
        null as eannumber,
        null as matl_num,
        null as prod_hier_l1,
        null as prod_hier_l2,
        null as prod_hier_l3,
        prod_hier_l4,
        null as prod_hier_l5,
        null as prod_hier_l6,
        null as prod_hier_l7,
        null as prod_hier_l8,
        null as prod_hier_l9,
        null as ques_type,
        null as "y/n_flag",
        priority_store_flag,
        null as add_info,
        null as response,
        null as response_score,
        kpi_chnl_wt,
        channel_weightage,
        null as weight_msl,
        null as weight_oos,
        null as weight_soa,
        null as weight_sos,
        null as weight_promo,
        null as weight_planogram,
        null as weight_display,
        mkt_share,
        salience_val,
        actual_value,
        ref_value,
        kpi_actual_wt_val,
        kpi_ref_val,
        kpi_ref_wt_val,
        null as photo_url,
        compliance,
        gap_to_target,
        null as compliance_propogated,
        null as gap_propagated
    from 
    (
        select * from msl
        union all
        select * from osa
        union all
        select * from promo
        union all
        select * from pog_anz
        union all
        select * from sos        
    )
),
fisc_per_table as 
(
    select country,
        dataset,
        substring(a.fisc_per, 1, 4) as fisc_yr,
        parent_customer,
        retail_environment,
        channel,
        prod_hier_l4,
        priority_store_flag,
        min(fisc_per) as min_fisc_per,
        max(b.max_mnth_id) as max_mnth_id
    from (
            select *
            from size_of_price
            WHERE kpi = 'SIZE OF THE PRIZE'
        ) a
        join (
            select substring(a.fisc_per, 1, 4) as fisc_yr,
                max(b.mnth_id) as max_mnth_id
            from (
                    select *
                    from size_of_price
                    WHERE kpi = 'SIZE OF THE PRIZE'
                ) a
                join edw_vw_os_time_dim b on substring(a.fisc_per, 1, 4) = b."year"
            group by substring(a.fisc_per, 1, 4)
        ) b on substring(a.fisc_per, 1, 4) = b.fisc_yr
    group by country,
        dataset,
        substring(a.fisc_per, 1, 4),
        parent_customer,
        retail_environment,
        channel,
        prod_hier_l4,
        priority_store_flag
    order by country,
        dataset,
        substring(a.fisc_per, 1, 4),
        parent_customer,
        retail_environment,
        channel,
        prod_hier_l4,
        priority_store_flag
),
prod_hier_data_new as 
(
    select 
        country,
        dataset,
        cast(a.mnth_id as integer) as fisc_per,
        parent_customer,
        retail_environment,
        channel,
        prod_hier_l4,
        priority_store_flag
    from 
        (
            select distinct mnth_id from edw_vw_os_time_dim
            where "year" in 
                (
                    select distinct substring(fisc_per, 1, 4) from size_of_price
                    WHERE kpi = 'SIZE OF THE PRIZE'
                )
        ) a
        left join fisc_per_table b on a.mnth_id >= b.min_fisc_per
        and a.mnth_id <= b.max_mnth_id
    order by 
        country,
        dataset,
        a.mnth_id,
        parent_customer,
        retail_environment,
        channel,
        prod_hier_l4,
        priority_store_flag
),
size_of_price_temp as 
(
    SELECT 
        null as hashkey,
        null as hash_row,
        a.DATASET,
        null as customerid,
        null as salespersonid,
        null as visitid,
        null as questiontext,
        null as productid,
        'SIZE OF THE PRIZE TEMP' as KPI,
        TO_DATE(a.fisc_per || '15', 'YYYYMMDD') as SCHEDULEDDATE,
        LATESTDATE,
        cast(substring(a.fisc_per, 1, 4) as integer) as FISC_YR,
        a.FISC_PER as FISC_PER,
        null as merchandiser_name,
        null as customername,
        a.COUNTRY,
        null as state,
        a.PARENT_CUSTOMER as PARENT_CUSTOMER,
        a.RETAIL_ENVIRONMENT,
        a.CHANNEL,
        null as retailer,
        null as business_unit,
        null as eannumber,
        null as matl_num,
        null as prod_hier_l1,
        null as prod_hier_l2,
        null as prod_hier_l3,    
        a.prod_hier_l4 as prod_hier_l4,
        null as prod_hier_l5,
        null as prod_hier_l6,
        null as prod_hier_l7,
        null as prod_hier_l8,
        null as prod_hier_l9,
        null as ques_type,
        null as "y/n_flag",
        a.PRIORITY_STORE_FLAG,
        null as add_info,
        null as response,
        null as response_score,
        KPI_CHNL_WT,
        CHANNEL_WEIGHTAGE,
        null as weight_msl,
        null as weight_oos,
        null as weight_soa,
        null as weight_sos,
        null as weight_promo,
        null as weight_planogram,
        null as weight_display,
        mkt_share,
        SALIENCE_VAL,
        ACTUAL_VALUE,
        REF_VALUE,
        KPI_ACTUAL_WT_VAL,
        KPI_REF_VAL,
        KPI_REF_WT_VAL,
        null as photo_url,
        compliance,
        gap_to_target,
        null as compliance_propogated,
        null as gap_propagated
    FROM prod_hier_data_new a
        left join (
            select *
            from size_of_price
            WHERE kpi = 'SIZE OF THE PRIZE'
        ) b on a.fisc_per = b.fisc_per
        and a.country = b.country
        and a.dataset = b.dataset
        and a.parent_customer = b.parent_customer
        and a.prod_hier_l4 = b.prod_hier_l4
),
wks_sotp_propagation_hash as 
(
    SELECT *,
        --nvl(lead(compliance,1) over (partition by hashkey order by fisc_per),compliance),
        --lead(hashkey,1) over (partition by hashkey order by fisc_per),
        max(compliance) over (
            partition by hashkey
            order by fisc_per rows between unbounded preceding and current row
        ) as complaince_max
    FROM 
        (
            SELECT dataset,
                country,
                parent_customer,
                retail_environment,
                channel,
                prod_hier_l4,
                fisc_yr,
                fisc_per,
                compliance,
                MD5(
                    nvl(dataset, 'a') || nvl(country, 'b') || nvl(parent_customer, 'c') || nvl(prod_hier_l4, 'd') || nvl(fisc_yr, 2099)
                ) as hashkey
            FROM size_of_price_temp
            WHERE kpi = 'SIZE OF THE PRIZE TEMP' --AND fisc_yr = 2021 and fisc_per < 202104
                --AND retail_environment = 'AU INDY PHARMACY'
            ORDER BY dataset,
                country,
                parent_customer,
                retail_environment,
                channel,
                prod_hier_l4,
                fisc_yr,
                fisc_per
        )
    ORDER BY dataset,
        country,
        parent_customer,
        retail_environment,
        channel,
        prod_hier_l4,
        fisc_yr,
        fisc_per  
),
size_of_price_prop as
(   
    select 
        null as hashkey,
        null as hash_row,
        temp.DATASET as DATASET,
        null as customerid,
        null as salespersonid,
        null as visitid,
        null as questiontext,
        null as productid,
        'SIZE OF THE PRIZE PROP' AS KPI,
        temp.SCHEDULEDDATE as SCHEDULEDDATE,
        LATESTDATE,
        temp.FISC_YR as FISC_YR,
        temp.FISC_PER as FISC_PER,
        null as merchandiser_name,
        null as customername,
        temp.COUNTRY as country,
        null as state,
        temp.PARENT_CUSTOMER as PARENT_CUSTOMER,
        temp.RETAIL_ENVIRONMENT as RETAIL_ENVIRONMENT,
        temp.CHANNEL as CHANNEL,
        null as retailer,
        null as business_unit,
        null as eannumber,
        null as matl_num,
        null as prod_hier_l1,
        null as prod_hier_l2,
        null as prod_hier_l3,
        temp.prod_hier_l4 as prod_hier_l4,
        null as prod_hier_l5,
        null as prod_hier_l6,
        null as prod_hier_l7,
        null as prod_hier_l8,
        null as prod_hier_l9,
        null as ques_type,
        null as "y/n_flag",
        PRIORITY_STORE_FLAG,
        null as add_info,
        null as response,
        null as response_score,
        KPI_CHNL_WT,
        CHANNEL_WEIGHTAGE,
        null as weight_msl,
        null as weight_oos,
        null as weight_soa,
        null as weight_sos,
        null as weight_promo,
        null as weight_planogram,
        null as weight_display,
        mkt_share,
        SALIENCE_VAL,
        ACTUAL_VALUE,
        REF_VALUE,
        KPI_ACTUAL_WT_VAL,
        KPI_REF_VAL,
        KPI_REF_WT_VAL,
        null as photo_url,
        temp.compliance,
        gap_to_target,
        PROP.complaince_max as compliance_propogated,
        case
            when (
                coalesce(mkt_share, 0) - coalesce(PROP.complaince_max, 0)
            ) < '0' then '0'
            else (
                coalesce(mkt_share, 0) - coalesce(PROP.complaince_max, 0)
            )
        END as gap_propagated
    from (
            select *
            FROM size_of_price_temp
            WHERE kpi = 'SIZE OF THE PRIZE TEMP'
        ) temp
        left join wks_sotp_propagation_hash PROP on temp.dataset = PROP.dataset
        and temp.country = PROP.country
        and temp.parent_customer = PROP.parent_customer
        and temp.prod_hier_l4 = PROP.prod_hier_l4
        and temp.fisc_per = PROP.fisc_per
),
final as 
(
    select 
        hashkey::varchar(32) as hashkey,
        hash_row::number(38,0) as hash_row,
        dataset::varchar(49) as dataset,
        customerid::varchar(255) as customerid,
        salespersonid::varchar(255) as salespersonid,
        visitid::varchar(255) as visitid,
        questiontext::varchar(512) as questiontext,
        productid::varchar(255) as productid,
        kpi::varchar(67) as kpi,
        scheduleddate::date as scheduleddate,
        latestdate::date as latestdate,
        fisc_yr::number(18,0) as fisc_yr,
        fisc_per::number(18,0) as fisc_per,
        merchandiser_name::varchar(512) as merchandiser_name,
        customername::varchar(500) as customername,
        country::varchar(200) as country,
        state::varchar(256) as state,
        parent_customer::varchar(255) as parent_customer,
        retail_environment::varchar(256) as retail_environment,
        channel::varchar(255) as channel,
        retailer::varchar(331) as retailer,
        business_unit::varchar(200) as business_unit,
        eannumber::varchar(150) as eannumber,
        matl_num::varchar(100) as matl_num,
        prod_hier_l1::varchar(500) as prod_hier_l1,
        prod_hier_l2::varchar(500) as prod_hier_l2,
        prod_hier_l3::varchar(510) as prod_hier_l3,
        prod_hier_l4::varchar(510) as prod_hier_l4,
        prod_hier_l5::varchar(2000) as prod_hier_l5,
        prod_hier_l6::varchar(500) as prod_hier_l6,
        prod_hier_l7::varchar(500) as prod_hier_l7,
        prod_hier_l8::varchar(500) as prod_hier_l8,
        prod_hier_l9::varchar(1000) as prod_hier_l9,
        ques_type::varchar(112) as ques_type,
        "y/n_flag":: varchar(150) as "y/n_flag",
        priority_store_flag::varchar(10) as priority_store_flag,
        add_info::varchar(65535) as add_info,
        response::varchar(65535) as response,
        response_score::varchar(65535) as response_score,
        kpi_chnl_wt::float as kpi_chnl_wt,
        channel_weightage::float as channel_weightage,
        weight_msl::number(38,37) as weight_msl,
        weight_oos::number(38,37) as weight_oos,
        weight_soa::number(38,37) as weight_soa,
        weight_sos::number(38,37) as weight_sos,
        weight_promo::number(38,37) as weight_promo,
        weight_planogram::number(38,37) as weight_planogram,
        weight_display::number(38,37) as weight_display,
        mkt_share::float as mkt_share,
        salience_val::number(20,4) as salience_val,
        actual_value::number(14,4) as actual_value,
        ref_value::number(14,4) as ref_value,
        kpi_actual_wt_val::number(14,4) as kpi_actual_wt_val,
        kpi_ref_val::number(14,4) as kpi_ref_val,
        kpi_ref_wt_val::number(14,4) as kpi_ref_wt_val,
        photo_url::varchar(500) as photo_url,
        compliance::number(14,3) as compliance,
        gap_to_target::number(14,3) as gap_to_target,
        compliance_propogated::number(14,3) as compliance_propogated,
        gap_propagated::number(14,3) as gap_propagated
    from 
    (
        select * from size_of_price
        union all
        select * from size_of_price_temp
        union all
        select * from size_of_price_prop
    )
)
select * from final