{{
    config(
        post_hook="update {{this}} set actual_value = '1560.0000' where kpi = 'SOS COMPLIANCE' and ques_type = 'DENOMINATOR' and visitid = '9ADBD255F522C00194BF212618BB845E' and country = 'Taiwan' and scheduleddate = '2023-09-13'"
    )
}}
with edw_perfect_store_kpi_data as 
(
    select * from aspedw_integration.edw_perfect_store_kpi_data
),
edw_perfect_store_rebase_wt_temp as 
(
    select * from aspedw_integration.edw_perfect_store_rebase_wt_temp
),
edw_vw_perfect_store_trax_products as 
(
    select * from pcfedw_integration.edw_vw_perfect_store_trax_products  
),
edw_sales_reporting as 
(
    select * from pcfedw_integration.edw_sales_reporting
),
sdl_mds_pacific_ps_benchmarks as 
(
    select * from dev_dna_load.aspsdl_raw.sdl_mds_pacific_ps_benchmarks
),
edw_pacific_perenso_ims_analysis as
(
    select * from pcfedw_integration.edw_pacific_perenso_ims_analysis    
),
edw_product_key_attributes as 
(
    select * from aspedw_integration.edw_product_key_attributes
),
union_1 as 
(   
    SELECT 
        hashkey,
        hash_row,
        dataset,
        customerid,
        salespersonid,
        visitid,
        questiontext,
        productid,
        kpi,
        scheduleddate,
        latestdate,
        fisc_yr,
        fisc_per,
        merchandiser_name,
        customername,
        country,
        state,
        parent_customer,
        retail_environment,
        channel,
        retailer,
        business_unit,
        eannumber,
        matl_num,
        prod_hier_l1,
        prod_hier_l2,
        prod_hier_l3,
        prod_hier_l4,
        prod_hier_l5,
        prod_hier_l6,
        prod_hier_l7,
        prod_hier_l8,
        prod_hier_l9,
        ques_type,
        y_n_flag as "y/n_flag",
        priority_store_flag,
        add_info,
        response,
        response_score,
        kpi_chnl_wt,
        channel_weightage,
        weight_msl,
        weight_oos,
        weight_soa,
        weight_sos,
        weight_promo,
        weight_planogram,
        weight_display,
        mkt_share,
        salience_val,
        actual_value,
        ref_value,
        kpi_actual_wt_val,
        kpi_ref_val,
        kpi_ref_wt_val,
        photo_url,
        null as compliance,
        null as gap_to_target,
        null as compliance_propogated,
        null as gap_propagated,
        null as full_opportunity_lcy,
        null as weighted_opportunity_lcy,
        null as full_opportunity_usd,
        null as weighted_opportunity_usd,
        null as sotp_lcy,
        null as sotp_usd,
        store_grade
    FROM EDW_PERFECT_STORE_KPI_DATA
)
,
msl as
(   
    SELECT 
        DATASET,
        CUSTOMERID,
        SALESPERSONID,
        KPI,
        TO_DATE(YM || '15', 'YYYYMMDD') AS SCHEDULEDDATE,
        LATESTDATE,
        FISC_YR,
        FISC_PER,
        MERCHANDISER_NAME,
        CUSTOMERNAME,
        COUNTRY,
        STATE,
        PARENT_CUSTOMER,
        RETAIL_ENVIRONMENT,
        CHANNEL,
        RETAILER,
        BUSINESS_UNIT,
        PRIORITY_STORE_FLAG,
        KPI_CHNL_WT,
        CHANNEL_WEIGHTAGE,
        SALIENCE_VAL,
        ACTUAL_VALUE,
        REF_VALUE,
        KPI_ACTUAL_WT_VAL,
        KPI_REF_VAL,
        KPI_REF_WT_VAL,
        PHOTO_URL,
        STORE_GRADE
    FROM 
        (
            SELECT 
                rtrim(DATASET) as DATASET,
                rtrim(CUSTOMERID) as CUSTOMERID,
                rtrim(SALESPERSONID) as SALESPERSONID,
                rtrim(KPI) as KPI,
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM') AS YM,
                rtrim(LATESTDATE) as LATESTDATE,
                rtrim(FISC_YR) as FISC_YR,
                rtrim(FISC_PER) as FISC_PER,
                rtrim(MERCHANDISER_NAME) as MERCHANDISER_NAME,
                rtrim(CUSTOMERNAME) as CUSTOMERNAME,
                rtrim(COUNTRY) as COUNTRY,
                rtrim(STATE) as STATE,
                rtrim(PARENT_CUSTOMER) as PARENT_CUSTOMER,
                rtrim(RETAIL_ENVIRONMENT) as RETAIL_ENVIRONMENT,
                rtrim(CHANNEL) as CHANNEL,
                rtrim(RETAILER) as RETAILER,
                rtrim(BUSINESS_UNIT) as BUSINESS_UNIT,
                rtrim(PRIORITY_STORE_FLAG) as PRIORITY_STORE_FLAG,
                MAX(KPI_CHNL_WT) AS KPI_CHNL_WT,
                MAX(CHANNEL_WEIGHTAGE) AS CHANNEL_WEIGHTAGE,
                MAX(SALIENCE_VAL) AS SALIENCE_VAL,
                SUM(ACTUAL_VALUE) AS ACTUAL_VALUE,
                SUM(REF_VALUE) AS REF_VALUE,
                SUM(KPI_ACTUAL_WT_VAL) AS KPI_ACTUAL_WT_VAL,
                SUM(KPI_REF_VAL) AS KPI_REF_VAL,
                SUM(KPI_REF_WT_VAL) AS KPI_REF_WT_VAL,
                rtrim(PHOTO_URL) as PHOTO_URL,
                rtrim(STORE_GRADE) as STORE_GRADE
            FROM 
                (
                    SELECT 'MSL COMPLIANCE' AS DATASET,
                        CUSTOMERID,
                        SALESPERSONID,
                        'PERFECT STORE SCORE' AS KPI,
                        SCHEDULEDDATE,
                        LATESTDATE,
                        FISC_YR,
                        FISC_PER,
                        MERCHANDISER_NAME,
                        CUSTOMERNAME,
                        COUNTRY,
                        STATE,
                        PARENT_CUSTOMER,
                        RETAIL_ENVIRONMENT,
                        CHANNEL,
                        RETAILER,
                        BUSINESS_UNIT,
                        PRIORITY_STORE_FLAG,
                        KPI_CHNL_WT,
                        CHANNEL_WEIGHTAGE,
                        SALIENCE_VAL,
                        ACTUAL_VALUE,
                        REF_VALUE,
                        ROUND(ACTUAL_VALUE * WEIGHT_MSL, 4) AS KPI_ACTUAL_WT_VAL,
                        REF_VALUE AS KPI_REF_VAL,
                        ROUND(REF_VALUE * WEIGHT_MSL, 4) AS KPI_REF_WT_VAL,
                        PHOTO_URL,
                        STORE_GRADE
                    FROM EDW_PERFECT_STORE_KPI_DATA
                    WHERE UPPER(KPI) = 'MSL COMPLIANCE'
                        AND UPPER(COUNTRY) <> 'INDIA'
                )
            GROUP BY 
                rtrim(DATASET),
                rtrim(CUSTOMERID),
                rtrim(SALESPERSONID),
                rtrim(KPI),
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM'),
                rtrim(LATESTDATE),
                rtrim(FISC_YR),
                rtrim(FISC_PER),
                rtrim(MERCHANDISER_NAME),
                rtrim(CUSTOMERNAME),
                rtrim(COUNTRY),
                rtrim(STATE),
                rtrim(PARENT_CUSTOMER),
                rtrim(RETAIL_ENVIRONMENT),
                rtrim(CHANNEL),
                rtrim(RETAILER),
                rtrim(BUSINESS_UNIT),
                rtrim(PRIORITY_STORE_FLAG),
                rtrim(PHOTO_URL),
                rtrim(STORE_GRADE) ----- India GT -----------------------
                --- this union added as part of India GT should not be included if online is going live first
            UNION ALL
            SELECT 
                rtrim(DATASET) as DATASET,
                rtrim(CUSTOMERID) as CUSTOMERID,
                rtrim(SALESPERSONID) as SALESPERSONID,
                rtrim(KPI) as KPI,
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM') AS YM,
                rtrim(LATESTDATE) as LATESTDATE,
                rtrim(FISC_YR) as FISC_YR,
                rtrim(FISC_PER) as FISC_PER,
                rtrim(MERCHANDISER_NAME) as MERCHANDISER_NAME,
                rtrim(CUSTOMERNAME) as CUSTOMERNAME,
                rtrim(COUNTRY) as COUNTRY,
                rtrim(STATE) as STATE,
                rtrim(PARENT_CUSTOMER) as PARENT_CUSTOMER,
                rtrim(RETAIL_ENVIRONMENT) as RETAIL_ENVIRONMENT,
                rtrim(CHANNEL) as CHANNEL,
                rtrim(RETAILER) as RETAILER,
                rtrim(BUSINESS_UNIT) as BUSINESS_UNIT,
                rtrim(PRIORITY_STORE_FLAG) as PRIORITY_STORE_FLAG,
                MAX(KPI_CHNL_WT) AS KPI_CHNL_WT,
                MAX(CHANNEL_WEIGHTAGE) AS CHANNEL_WEIGHTAGE,
                MAX(SALIENCE_VAL) AS SALIENCE_VAL,
                SUM(ACTUAL_VALUE) AS ACTUAL_VALUE,
                --KPI_ACTUAL_WT_VAL
                SUM(KPI_REF_VAL) AS REF_VALUE,
                --KPI_REF_VAL
                SUM(KPI_ACTUAL_WT_VAL) AS KPI_ACTUAL_WT_VAL,
                -- KPI_ACTUAL_WT_VAL*weight_msl
                SUM(KPI_REF_VAL) AS KPI_REF_VAL,
                --KPI_REF_VAL
                SUM(KPI_REF_WT_VAL) AS KPI_REF_WT_VAL,
                -- KPI_REF_VAL*weight_msl
                rtrim(PHOTO_URL) as PHOTO_URL,
                rtrim(STORE_GRADE) as STORE_GRADE
            FROM 
                (
                    SELECT 'MSL COMPLIANCE' AS DATASET,
                        CUSTOMERID,
                        SALESPERSONID,
                        'PERFECT STORE SCORE' AS KPI,
                        SCHEDULEDDATE,
                        LATESTDATE,
                        FISC_YR,
                        FISC_PER,
                        MERCHANDISER_NAME,
                        CUSTOMERNAME,
                        COUNTRY,
                        STATE,
                        PARENT_CUSTOMER,
                        RETAIL_ENVIRONMENT,
                        CHANNEL,
                        RETAILER,
                        BUSINESS_UNIT,
                        PRIORITY_STORE_FLAG,
                        KPI_CHNL_WT,
                        CHANNEL_WEIGHTAGE,
                        SALIENCE_VAL,
                        KPI_ACTUAL_WT_VAL AS ACTUAL_VALUE,
                        REF_VALUE,
                        ROUND(KPI_ACTUAL_WT_VAL * WEIGHT_MSL, 4) AS KPI_ACTUAL_WT_VAL,
                        KPI_REF_VAL AS KPI_REF_VAL,
                        ROUND(KPI_REF_VAL * WEIGHT_MSL, 4) AS KPI_REF_WT_VAL,
                        PHOTO_URL,
                        STORE_GRADE
                    FROM EDW_PERFECT_STORE_KPI_DATA
                    WHERE UPPER(KPI) = 'MSL COMPLIANCE'
                        AND UPPER(COUNTRY || CHANNEL) IN('INDIAGT', 'INDIASELF SERVICE STORE')
                        AND UPPER(COUNTRY) = 'INDIA'
                        AND KPI_REF_WT_VAL = '1'
                ) PS
            GROUP BY 
                rtrim(DATASET),
                rtrim(CUSTOMERID),
                rtrim(SALESPERSONID),
                rtrim(KPI),
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM'),
                rtrim(LATESTDATE),
                rtrim(FISC_YR),
                rtrim(FISC_PER),
                rtrim(MERCHANDISER_NAME),
                rtrim(CUSTOMERNAME),
                rtrim(COUNTRY),
                rtrim(STATE),
                rtrim(PARENT_CUSTOMER),
                rtrim(RETAIL_ENVIRONMENT),
                rtrim(CHANNEL),
                rtrim(RETAILER),
                rtrim(BUSINESS_UNIT),
                rtrim(PRIORITY_STORE_FLAG),
                rtrim(PHOTO_URL),
                rtrim(STORE_GRADE)
                ------------------- India MT -----------------------------
            UNION ALL
            SELECT 
                rtrim(DATASET) as DATASET,
                rtrim(CUSTOMERID) as CUSTOMERID,
                rtrim(SALESPERSONID) as SALESPERSONID,
                rtrim(KPI) as KPI,
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM') AS YM,
                rtrim(LATESTDATE) as LATESTDATE,
                rtrim(FISC_YR) as FISC_YR,
                rtrim(FISC_PER) as FISC_PER,
                rtrim(MERCHANDISER_NAME) as MERCHANDISER_NAME,
                rtrim(CUSTOMERNAME) as CUSTOMERNAME,
                rtrim(COUNTRY) as COUNTRY,
                rtrim(STATE) as STATE,
                rtrim(PARENT_CUSTOMER) as PARENT_CUSTOMER,
                rtrim(RETAIL_ENVIRONMENT) as RETAIL_ENVIRONMENT,
                rtrim(CHANNEL) as CHANNEL,
                rtrim(RETAILER) as RETAILER,
                rtrim(BUSINESS_UNIT) as BUSINESS_UNIT,
                rtrim(PRIORITY_STORE_FLAG) as PRIORITY_STORE_FLAG,
                MAX(KPI_CHNL_WT) AS KPI_CHNL_WT,
                MAX(CHANNEL_WEIGHTAGE) AS CHANNEL_WEIGHTAGE,
                MAX(SALIENCE_VAL) AS SALIENCE_VAL,
                SUM(ACTUAL_VALUE) AS ACTUAL_VALUE,
                SUM(REF_VALUE) AS REF_VALUE,
                SUM(KPI_ACTUAL_WT_VAL) AS KPI_ACTUAL_WT_VAL,
                SUM(KPI_REF_VAL) AS KPI_REF_VAL,
                SUM(KPI_REF_WT_VAL) AS KPI_REF_WT_VAL,
                rtrim(PHOTO_URL) as PHOTO_URL,
                rtrim(STORE_GRADE) as STORE_GRADE
            FROM 
                (
                    SELECT 'MSL COMPLIANCE' AS DATASET,
                        CUSTOMERID,
                        SALESPERSONID,
                        'PERFECT STORE SCORE' AS KPI,
                        SCHEDULEDDATE,
                        LATESTDATE,
                        FISC_YR,
                        FISC_PER,
                        MERCHANDISER_NAME,
                        CUSTOMERNAME,
                        COUNTRY,
                        STATE,
                        PARENT_CUSTOMER,
                        RETAIL_ENVIRONMENT,
                        CHANNEL,
                        RETAILER,
                        BUSINESS_UNIT,
                        PRIORITY_STORE_FLAG,
                        KPI_CHNL_WT,
                        CHANNEL_WEIGHTAGE,
                        SALIENCE_VAL,
                        ACTUAL_VALUE,
                        REF_VALUE,
                        ROUND(ACTUAL_VALUE * WEIGHT_MSL, 4) AS KPI_ACTUAL_WT_VAL,
                        REF_VALUE AS KPI_REF_VAL,
                        ROUND(REF_VALUE * WEIGHT_MSL, 4) AS KPI_REF_WT_VAL,
                        PHOTO_URL,
                        STORE_GRADE
                    FROM EDW_PERFECT_STORE_KPI_DATA
                    WHERE UPPER(KPI) = 'MSL COMPLIANCE'
                        AND UPPER(COUNTRY || CHANNEL) NOT IN('INDIAGT', 'INDIASELF SERVICE STORE')
                ) PS
            GROUP BY 
                rtrim(DATASET),
                rtrim(CUSTOMERID),
                rtrim(SALESPERSONID),
                rtrim(KPI),
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM'),
                rtrim(LATESTDATE),
                rtrim(FISC_YR),
                rtrim(FISC_PER),
                rtrim(MERCHANDISER_NAME),
                rtrim(CUSTOMERNAME),
                rtrim(COUNTRY),
                rtrim(STATE),
                rtrim(PARENT_CUSTOMER),
                rtrim(RETAIL_ENVIRONMENT),
                rtrim(CHANNEL),
                rtrim(RETAILER),
                rtrim(BUSINESS_UNIT),
                rtrim(PRIORITY_STORE_FLAG),
                rtrim(PHOTO_URL),
                rtrim(STORE_GRADE)
        ) MSL
),
osa as
(   
    SELECT 
        DATASET,
        CUSTOMERID,
        SALESPERSONID,
        KPI,
        TO_DATE(YM || '15', 'YYYYMMDD') AS SCHEDULEDDATE,
        LATESTDATE,
        FISC_YR,
        FISC_PER,
        MERCHANDISER_NAME,
        CUSTOMERNAME,
        COUNTRY,
        STATE,
        PARENT_CUSTOMER,
        RETAIL_ENVIRONMENT,
        CHANNEL,
        RETAILER,
        BUSINESS_UNIT,
        PRIORITY_STORE_FLAG,
        KPI_CHNL_WT,
        CHANNEL_WEIGHTAGE,
        SALIENCE_VAL,
        ACTUAL_VALUE,
        REF_VALUE,
        KPI_ACTUAL_WT_VAL,
        KPI_REF_VAL,
        KPI_REF_WT_VAL,
        PHOTO_URL,
        STORE_GRADE
    FROM 
        (
            SELECT 
                rtrim(DATASET) as DATASET,
                rtrim(CUSTOMERID) as CUSTOMERID,
                rtrim(SALESPERSONID) as SALESPERSONID,
                rtrim(KPI) as KPI,
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM') AS YM,
                rtrim(LATESTDATE) as LATESTDATE,
                rtrim(FISC_YR) as FISC_YR,
                rtrim(FISC_PER) as FISC_PER,
                rtrim(MERCHANDISER_NAME) as MERCHANDISER_NAME,
                rtrim(CUSTOMERNAME) as CUSTOMERNAME,
                rtrim(COUNTRY) as COUNTRY,
                rtrim(STATE) as STATE,
                rtrim(PARENT_CUSTOMER) as PARENT_CUSTOMER,
                rtrim(RETAIL_ENVIRONMENT) as RETAIL_ENVIRONMENT,
                rtrim(CHANNEL) as CHANNEL,
                rtrim(RETAILER) as RETAILER,
                rtrim(BUSINESS_UNIT) as BUSINESS_UNIT,
                rtrim(PRIORITY_STORE_FLAG) as PRIORITY_STORE_FLAG,
                MAX(KPI_CHNL_WT) AS KPI_CHNL_WT,
                MAX(CHANNEL_WEIGHTAGE) AS CHANNEL_WEIGHTAGE,
                MAX(SALIENCE_VAL) AS SALIENCE_VAL,
                SUM(ACTUAL_VALUE) AS ACTUAL_VALUE,
                SUM(REF_VALUE) AS REF_VALUE,
                SUM(KPI_ACTUAL_WT_VAL) AS KPI_ACTUAL_WT_VAL,
                SUM(KPI_REF_VAL) AS KPI_REF_VAL,
                SUM(KPI_REF_WT_VAL) AS KPI_REF_WT_VAL,
                rtrim(PHOTO_URL) as PHOTO_URL,
                rtrim(STORE_GRADE) as STORE_GRADE
            FROM 
                (
                    SELECT 'OOS COMPLIANCE' AS DATASET,
                        CUSTOMERID,
                        SALESPERSONID,
                        'PERFECT STORE SCORE' AS KPI,
                        SCHEDULEDDATE,
                        LATESTDATE,
                        FISC_YR,
                        FISC_PER,
                        MERCHANDISER_NAME,
                        CUSTOMERNAME,
                        COUNTRY,
                        STATE,
                        PARENT_CUSTOMER,
                        RETAIL_ENVIRONMENT,
                        CHANNEL,
                        RETAILER,
                        BUSINESS_UNIT,
                        PRIORITY_STORE_FLAG,
                        KPI_CHNL_WT,
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
                        ROUND(REF_VALUE * WEIGHT_OOS, 4) AS KPI_REF_WT_VAL,
                        PHOTO_URL,
                        STORE_GRADE
                    FROM EDW_PERFECT_STORE_KPI_DATA
                    WHERE UPPER(KPI) = 'OOS COMPLIANCE'
                )
            GROUP BY 
                rtrim(DATASET),
                rtrim(CUSTOMERID),
                rtrim(SALESPERSONID),
                rtrim(KPI),
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM'),
                rtrim(LATESTDATE),
                rtrim(FISC_YR),
                rtrim(FISC_PER),
                rtrim(MERCHANDISER_NAME),
                rtrim(CUSTOMERNAME),
                rtrim(COUNTRY),
                rtrim(STATE),
                rtrim(PARENT_CUSTOMER),
                rtrim(RETAIL_ENVIRONMENT),
                rtrim(CHANNEL),
                rtrim(RETAILER),
                rtrim(BUSINESS_UNIT),
                rtrim(PRIORITY_STORE_FLAG),
                rtrim(PHOTO_URL),
                rtrim(STORE_GRADE)
        ) OSA
),
promo as
(   
    SELECT 
        DATASET,
        CUSTOMERID,
        SALESPERSONID,
        KPI,
        TO_DATE(YM || '15', 'YYYYMMDD') AS SCHEDULEDDATE,
        LATESTDATE,
        FISC_YR,
        FISC_PER,
        MERCHANDISER_NAME,
        CUSTOMERNAME,
        COUNTRY,
        STATE,
        PARENT_CUSTOMER,
        RETAIL_ENVIRONMENT,
        CHANNEL,
        RETAILER,
        BUSINESS_UNIT,
        PRIORITY_STORE_FLAG,
        KPI_CHNL_WT,
        CHANNEL_WEIGHTAGE,
        SALIENCE_VAL,
        ACTUAL_VALUE,
        REF_VALUE,
        KPI_ACTUAL_WT_VAL,
        KPI_REF_VAL,
        KPI_REF_WT_VAL,
        PHOTO_URL,
        STORE_GRADE
    FROM 
        (
            SELECT 
                rtrim(DATASET) as DATASET,
                rtrim(CUSTOMERID) as CUSTOMERID,
                rtrim(SALESPERSONID) as SALESPERSONID,
                rtrim(KPI) as KPI,
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM') AS YM,
                rtrim(LATESTDATE) as LATESTDATE,
                rtrim(FISC_YR) as FISC_YR,
                rtrim(FISC_PER) as FISC_PER,
                rtrim(MERCHANDISER_NAME) as MERCHANDISER_NAME,
                rtrim(CUSTOMERNAME) as CUSTOMERNAME,
                rtrim(COUNTRY) as COUNTRY,
                rtrim(STATE) as STATE,
                rtrim(PARENT_CUSTOMER) as PARENT_CUSTOMER,
                rtrim(RETAIL_ENVIRONMENT) as RETAIL_ENVIRONMENT,
                rtrim(CHANNEL) as CHANNEL,
                rtrim(RETAILER) as RETAILER,
                rtrim(BUSINESS_UNIT) as BUSINESS_UNIT,
                rtrim(PRIORITY_STORE_FLAG) as PRIORITY_STORE_FLAG,
                MAX(KPI_CHNL_WT) AS KPI_CHNL_WT,
                MAX(CHANNEL_WEIGHTAGE) AS CHANNEL_WEIGHTAGE,
                MAX(SALIENCE_VAL) AS SALIENCE_VAL,
                SUM(ACTUAL_VALUE) AS ACTUAL_VALUE,
                SUM(REF_VALUE) AS REF_VALUE,
                SUM(KPI_ACTUAL_WT_VAL) AS KPI_ACTUAL_WT_VAL,
                SUM(KPI_REF_VAL) AS KPI_REF_VAL,
                SUM(KPI_REF_WT_VAL) AS KPI_REF_WT_VAL,
                rtrim(PHOTO_URL) as PHOTO_URL,
                rtrim(STORE_GRADE) as STORE_GRADE
            FROM 
                (
                    SELECT 'PROMO COMPLIANCE' AS DATASET,
                        CUSTOMERID,
                        SALESPERSONID,
                        'PERFECT STORE SCORE' AS KPI,
                        SCHEDULEDDATE,
                        LATESTDATE,
                        FISC_YR,
                        FISC_PER,
                        MERCHANDISER_NAME,
                        CUSTOMERNAME,
                        COUNTRY,
                        STATE,
                        PARENT_CUSTOMER,
                        RETAIL_ENVIRONMENT,
                        CHANNEL,
                        RETAILER,
                        BUSINESS_UNIT,
                        PRIORITY_STORE_FLAG,
                        KPI_CHNL_WT,
                        CHANNEL_WEIGHTAGE,
                        SALIENCE_VAL,
                        ACTUAL_VALUE,
                        REF_VALUE,
                        ROUND(ACTUAL_VALUE * WEIGHT_PROMO, 4) AS KPI_ACTUAL_WT_VAL,
                        REF_VALUE AS KPI_REF_VAL,
                        ROUND(REF_VALUE * WEIGHT_PROMO, 4) AS KPI_REF_WT_VAL,
                        PHOTO_URL,
                        STORE_GRADE
                    FROM EDW_PERFECT_STORE_KPI_DATA
                    WHERE UPPER(KPI) = 'PROMO COMPLIANCE'
                )
            GROUP BY 
                rtrim(DATASET),
                rtrim(CUSTOMERID),
                rtrim(SALESPERSONID),
                rtrim(KPI),
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM'),
                rtrim(LATESTDATE),
                rtrim(FISC_YR),
                rtrim(FISC_PER),
                rtrim(MERCHANDISER_NAME),
                rtrim(CUSTOMERNAME),
                rtrim(COUNTRY),
                rtrim(STATE),
                rtrim(PARENT_CUSTOMER),
                rtrim(RETAIL_ENVIRONMENT),
                rtrim(CHANNEL),
                rtrim(RETAILER),
                rtrim(BUSINESS_UNIT),
                rtrim(PRIORITY_STORE_FLAG),
                rtrim(PHOTO_URL),
                rtrim(STORE_GRADE)
        ) PROMO
),
display as
(   
    SELECT 
        DATASET,
        CUSTOMERID,
        SALESPERSONID,
        KPI,
        TO_DATE(YM || '15', 'YYYYMMDD') AS SCHEDULEDDATE,
        LATESTDATE,
        FISC_YR,
        FISC_PER,
        MERCHANDISER_NAME,
        CUSTOMERNAME,
        COUNTRY,
        STATE,
        PARENT_CUSTOMER,
        RETAIL_ENVIRONMENT,
        CHANNEL,
        RETAILER,
        BUSINESS_UNIT,
        PRIORITY_STORE_FLAG,
        KPI_CHNL_WT,
        CHANNEL_WEIGHTAGE,
        SALIENCE_VAL,
        ACTUAL_VALUE,
        REF_VALUE,
        KPI_ACTUAL_WT_VAL,
        KPI_REF_VAL,
        KPI_REF_WT_VAL,
        PHOTO_URL,
        STORE_GRADE
    FROM 
        (
            SELECT 
                rtrim(DATASET) as DATASET,
                rtrim(CUSTOMERID) as CUSTOMERID,
                rtrim(SALESPERSONID) as SALESPERSONID,
                rtrim(KPI) as KPI,
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM') AS YM,
                rtrim(LATESTDATE) as LATESTDATE,
                rtrim(FISC_YR) as FISC_YR,
                rtrim(FISC_PER) as FISC_PER,
                rtrim(MERCHANDISER_NAME) as MERCHANDISER_NAME,
                rtrim(CUSTOMERNAME) as CUSTOMERNAME,
                rtrim(COUNTRY) as COUNTRY,
                rtrim(STATE) as STATE,
                rtrim(PARENT_CUSTOMER) as PARENT_CUSTOMER,
                rtrim(RETAIL_ENVIRONMENT) as RETAIL_ENVIRONMENT,
                rtrim(CHANNEL) as CHANNEL,
                rtrim(RETAILER) as RETAILER,
                rtrim(BUSINESS_UNIT) as BUSINESS_UNIT,
                rtrim(PRIORITY_STORE_FLAG) as PRIORITY_STORE_FLAG,
                MAX(KPI_CHNL_WT) AS KPI_CHNL_WT,
                MAX(CHANNEL_WEIGHTAGE) AS CHANNEL_WEIGHTAGE,
                MAX(SALIENCE_VAL) AS SALIENCE_VAL,
                SUM(ACTUAL_VALUE) AS ACTUAL_VALUE,
                SUM(REF_VALUE) AS REF_VALUE,
                SUM(KPI_ACTUAL_WT_VAL) AS KPI_ACTUAL_WT_VAL,
                SUM(KPI_REF_VAL) AS KPI_REF_VAL,
                SUM(KPI_REF_WT_VAL) AS KPI_REF_WT_VAL,
                rtrim(PHOTO_URL) as PHOTO_URL,
                rtrim(STORE_GRADE) as STORE_GRADE
            FROM (
                    SELECT 'DISPLAY COMPLIANCE' AS DATASET,
                        CUSTOMERID,
                        SALESPERSONID,
                        'PERFECT STORE SCORE' AS KPI,
                        SCHEDULEDDATE,
                        LATESTDATE,
                        FISC_YR,
                        FISC_PER,
                        MERCHANDISER_NAME,
                        CUSTOMERNAME,
                        COUNTRY,
                        STATE,
                        PARENT_CUSTOMER,
                        RETAIL_ENVIRONMENT,
                        CHANNEL,
                        RETAILER,
                        BUSINESS_UNIT,
                        PRIORITY_STORE_FLAG,
                        KPI_CHNL_WT,
                        CHANNEL_WEIGHTAGE,
                        SALIENCE_VAL,
                        ACTUAL_VALUE,
                        REF_VALUE,
                        ROUND(ACTUAL_VALUE * WEIGHT_DISPLAY, 4) AS KPI_ACTUAL_WT_VAL,
                        REF_VALUE AS KPI_REF_VAL,
                        ROUND(REF_VALUE * WEIGHT_DISPLAY, 4) AS KPI_REF_WT_VAL,
                        PHOTO_URL,
                        STORE_GRADE
                    FROM EDW_PERFECT_STORE_KPI_DATA
                    WHERE UPPER(KPI) = 'DISPLAY COMPLIANCE'
                )
            GROUP BY 
                rtrim(DATASET),
                rtrim(CUSTOMERID),
                rtrim(SALESPERSONID),
                rtrim(KPI),
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM'),
                rtrim(LATESTDATE),
                rtrim(FISC_YR),
                rtrim(FISC_PER),
                rtrim(MERCHANDISER_NAME),
                rtrim(CUSTOMERNAME),
                rtrim(COUNTRY),
                rtrim(STATE),
                rtrim(PARENT_CUSTOMER),
                rtrim(RETAIL_ENVIRONMENT),
                rtrim(CHANNEL),
                rtrim(RETAILER),
                rtrim(BUSINESS_UNIT),
                rtrim(PRIORITY_STORE_FLAG),
                rtrim(PHOTO_URL),
                rtrim(STORE_GRADE)
        ) DISPLAY
),
pog_others as
(   
    SELECT 
        DATASET,
        CUSTOMERID,
        SALESPERSONID,
        KPI,
        TO_DATE(YM || '15', 'YYYYMMDD') AS SCHEDULEDDATE,
        LATESTDATE,
        FISC_YR,
        FISC_PER,
        MERCHANDISER_NAME,
        CUSTOMERNAME,
        COUNTRY,
        STATE,
        PARENT_CUSTOMER,
        RETAIL_ENVIRONMENT,
        CHANNEL,
        RETAILER,
        BUSINESS_UNIT,
        PRIORITY_STORE_FLAG,
        KPI_CHNL_WT,
        CHANNEL_WEIGHTAGE,
        SALIENCE_VAL,
        ACTUAL_VALUE,
        REF_VALUE,
        KPI_ACTUAL_WT_VAL,
        KPI_REF_VAL,
        KPI_REF_WT_VAL,
        PHOTO_URL,
        null as STORE_GRADE
    FROM 
        (
            SELECT 
                rtrim(DATASET) as DATASET,
                rtrim(CUSTOMERID) as CUSTOMERID,
                rtrim(SALESPERSONID) as SALESPERSONID,
                rtrim(KPI) as KPI,
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM') AS YM,
                rtrim(LATESTDATE) as LATESTDATE,
                rtrim(FISC_YR) as FISC_YR,
                rtrim(FISC_PER) as FISC_PER,
                rtrim(MERCHANDISER_NAME) as MERCHANDISER_NAME,
                rtrim(CUSTOMERNAME) as CUSTOMERNAME,
                rtrim(COUNTRY) as COUNTRY,
                rtrim(STATE) as STATE,
                rtrim(PARENT_CUSTOMER) as PARENT_CUSTOMER,
                rtrim(RETAIL_ENVIRONMENT) as RETAIL_ENVIRONMENT,
                rtrim(CHANNEL) as CHANNEL,
                rtrim(RETAILER) as RETAILER,
                rtrim(BUSINESS_UNIT) as BUSINESS_UNIT,
                rtrim(PRIORITY_STORE_FLAG) as PRIORITY_STORE_FLAG,
                MAX(KPI_CHNL_WT) AS KPI_CHNL_WT,
                MAX(CHANNEL_WEIGHTAGE) AS CHANNEL_WEIGHTAGE,
                MAX(SALIENCE_VAL) AS SALIENCE_VAL,
                SUM(ACTUAL_VALUE) AS ACTUAL_VALUE,
                SUM(REF_VALUE) AS REF_VALUE,
                SUM(KPI_ACTUAL_WT_VAL) AS KPI_ACTUAL_WT_VAL,
                SUM(KPI_REF_VAL) AS KPI_REF_VAL,
                SUM(KPI_REF_WT_VAL) AS KPI_REF_WT_VAL,
                rtrim(PHOTO_URL) as PHOTO_URL
            FROM 
                (
                    SELECT 'PLANOGRAM COMPLIANCE' AS DATASET,
                        CUSTOMERID,
                        SALESPERSONID,
                        'PERFECT STORE SCORE' AS KPI,
                        SCHEDULEDDATE,
                        LATESTDATE,
                        FISC_YR,
                        FISC_PER,
                        MERCHANDISER_NAME,
                        CUSTOMERNAME,
                        COUNTRY,
                        STATE,
                        PARENT_CUSTOMER,
                        RETAIL_ENVIRONMENT,
                        CHANNEL,
                        RETAILER,
                        BUSINESS_UNIT,
                        PRIORITY_STORE_FLAG,
                        KPI_CHNL_WT,
                        CHANNEL_WEIGHTAGE,
                        SALIENCE_VAL,
                        ACTUAL_VALUE,
                        REF_VALUE,
                        ROUND(ACTUAL_VALUE * WEIGHT_PLANOGRAM, 4) AS KPI_ACTUAL_WT_VAL,
                        REF_VALUE AS KPI_REF_VAL,
                        ROUND(REF_VALUE * WEIGHT_PLANOGRAM, 4) AS KPI_REF_WT_VAL,
                        PHOTO_URL
                    FROM EDW_PERFECT_STORE_KPI_DATA
                    WHERE UPPER(KPI) = 'PLANOGRAM COMPLIANCE'
                        AND UPPER(COUNTRY) NOT IN ('AUSTRALIA', 'NEW ZEALAND')
                )
            GROUP BY 
                rtrim(DATASET),
                rtrim(CUSTOMERID),
                rtrim(SALESPERSONID),
                rtrim(KPI),
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM'),
                rtrim(LATESTDATE),
                rtrim(FISC_YR),
                rtrim(FISC_PER),
                rtrim(MERCHANDISER_NAME),
                rtrim(CUSTOMERNAME),
                rtrim(COUNTRY),
                rtrim(STATE),
                rtrim(PARENT_CUSTOMER),
                rtrim(RETAIL_ENVIRONMENT),
                rtrim(CHANNEL),
                rtrim(RETAILER),
                rtrim(BUSINESS_UNIT),
                rtrim(PRIORITY_STORE_FLAG),
                rtrim(PHOTO_URL)
        ) POG_Others
),
pog_anz as
(   
    SELECT 
        DATASET,
        CUSTOMERID,
        SALESPERSONID,
        KPI,
        TO_DATE(YM || '15', 'YYYYMMDD') AS SCHEDULEDDATE,
        LATESTDATE,
        FISC_YR,
        FISC_PER,
        MERCHANDISER_NAME,
        CUSTOMERNAME,
        COUNTRY,
        STATE,
        PARENT_CUSTOMER,
        RETAIL_ENVIRONMENT,
        CHANNEL,
        RETAILER,
        BUSINESS_UNIT,
        PRIORITY_STORE_FLAG,
        KPI_CHNL_WT,
        CHANNEL_WEIGHTAGE,
        SALIENCE_VAL,
        ACTUAL_VALUE,
        REF_VALUE,
        KPI_ACTUAL_WT_VAL,
        KPI_REF_VAL,
        KPI_REF_WT_VAL,
        PHOTO_URL,
        STORE_GRADE
    FROM 
        (
            SELECT 
                rtrim(DATASET) as DATASET,
                rtrim(CUSTOMERID) as CUSTOMERID,
                rtrim(SALESPERSONID) as SALESPERSONID,
                rtrim(KPI) as KPI,
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM') AS YM,
                rtrim(LATESTDATE) as LATESTDATE,
                rtrim(FISC_YR) as FISC_YR,
                rtrim(FISC_PER) as FISC_PER,
                rtrim(MERCHANDISER_NAME) as MERCHANDISER_NAME,
                rtrim(CUSTOMERNAME) as CUSTOMERNAME,
                rtrim(COUNTRY) as COUNTRY,
                rtrim(STATE) as STATE,
                rtrim(PARENT_CUSTOMER) as PARENT_CUSTOMER,
                rtrim(RETAIL_ENVIRONMENT) as RETAIL_ENVIRONMENT,
                rtrim(CHANNEL) as CHANNEL,
                rtrim(RETAILER) as RETAILER,
                rtrim(BUSINESS_UNIT) as BUSINESS_UNIT,
                rtrim(PRIORITY_STORE_FLAG) as PRIORITY_STORE_FLAG,
                MAX(KPI_CHNL_WT) AS KPI_CHNL_WT,
                MAX(CHANNEL_WEIGHTAGE) AS CHANNEL_WEIGHTAGE,
                MAX(SALIENCE_VAL) AS SALIENCE_VAL,
                SUM(ACTUAL_VALUE) AS ACTUAL_VALUE,
                SUM(REF_VALUE) AS REF_VALUE,
                SUM(KPI_ACTUAL_WT_VAL * WEIGHT_PLANOGRAM) AS KPI_ACTUAL_WT_VAL,
                SUM(KPI_REF_VAL) AS KPI_REF_VAL,
                SUM(KPI_REF_WT_VAL * WEIGHT_PLANOGRAM) AS KPI_REF_WT_VAL,
                rtrim(PHOTO_URL) as PHOTO_URL,
                rtrim(STORE_GRADE) as STORE_GRADE
            FROM 
                (
                    SELECT 'PLANOGRAM COMPLIANCE' AS DATASET,
                        CUSTOMERID,
                        SALESPERSONID,
                        'PERFECT STORE SCORE' AS KPI,
                        SCHEDULEDDATE,
                        LATESTDATE,
                        FISC_YR,
                        FISC_PER,
                        MERCHANDISER_NAME,
                        CUSTOMERNAME,
                        COUNTRY,
                        STATE,
                        PARENT_CUSTOMER,
                        RETAIL_ENVIRONMENT,
                        CHANNEL,
                        RETAILER,
                        BUSINESS_UNIT,
                        PRIORITY_STORE_FLAG,
                        KPI_CHNL_WT,
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
                        ROUND(REF_VALUE * MKT_SHARE, 4) AS KPI_REF_WT_VAL,
                        PHOTO_URL,
                        STORE_GRADE
                    FROM EDW_PERFECT_STORE_KPI_DATA
                    WHERE UPPER(KPI) = 'PLANOGRAM COMPLIANCE'
                        AND UPPER(COUNTRY) IN ('AUSTRALIA', 'NEW ZEALAND')
                )
            GROUP BY 
                rtrim(DATASET),
                rtrim(CUSTOMERID),
                rtrim(SALESPERSONID),
                rtrim(KPI),
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM'),
                rtrim(LATESTDATE),
                rtrim(FISC_YR),
                rtrim(FISC_PER),
                rtrim(MERCHANDISER_NAME),
                rtrim(CUSTOMERNAME),
                rtrim(COUNTRY),
                rtrim(STATE),
                rtrim(PARENT_CUSTOMER),
                rtrim(RETAIL_ENVIRONMENT),
                rtrim(CHANNEL),
                rtrim(RETAILER),
                rtrim(BUSINESS_UNIT),
                rtrim(PRIORITY_STORE_FLAG),
                rtrim(PHOTO_URL),
                rtrim(STORE_GRADE)
        ) POG_ANZ
),
sos as
(   
    SELECT 
        DATASET,
        CUSTOMERID,
        SALESPERSONID,
        KPI,
        TO_DATE(YM || '15', 'YYYYMMDD') AS SCHEDULEDDATE,
        LATESTDATE,
        FISC_YR,
        FISC_PER,
        MERCHANDISER_NAME,
        CUSTOMERNAME,
        COUNTRY,
        STATE,
        PARENT_CUSTOMER,
        RETAIL_ENVIRONMENT,
        CHANNEL,
        RETAILER,
        BUSINESS_UNIT,
        PRIORITY_STORE_FLAG,
        KPI_CHNL_WT,
        CHANNEL_WEIGHTAGE,
        SALIENCE_VAL,
        ACTUAL_VALUE,
        REF_VALUE,
        KPI_ACTUAL_WT_VAL,
        KPI_REF_VAL,
        KPI_REF_WT_VAL,
        PHOTO_URL,
        STORE_GRADE
    FROM 
        (
            SELECT 
                rtrim(DATASET) as DATASET,
                rtrim(CUSTOMERID) as CUSTOMERID,
                rtrim(SALESPERSONID) as SALESPERSONID,
                rtrim(KPI) as KPI,
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM') AS YM,
                rtrim(LATESTDATE) as LATESTDATE,
                rtrim(FISC_YR) as FISC_YR,
                rtrim(FISC_PER) as FISC_PER,
                rtrim(MERCHANDISER_NAME) as MERCHANDISER_NAME,
                rtrim(CUSTOMERNAME) as CUSTOMERNAME,
                rtrim(COUNTRY) as COUNTRY,
                rtrim(STATE) as STATE,
                rtrim(PARENT_CUSTOMER) as PARENT_CUSTOMER,
                rtrim(RETAIL_ENVIRONMENT) as RETAIL_ENVIRONMENT,
                rtrim(CHANNEL) as CHANNEL,
                rtrim(RETAILER) as RETAILER,
                rtrim(BUSINESS_UNIT) as BUSINESS_UNIT,
                rtrim(PRIORITY_STORE_FLAG) as PRIORITY_STORE_FLAG,
                MAX(KPI_CHNL_WT) AS KPI_CHNL_WT,
                MAX(CHANNEL_WEIGHTAGE) AS CHANNEL_WEIGHTAGE,
                MAX(SALIENCE_VAL) AS SALIENCE_VAL,
                SUM(NUM_VALUE) AS ACTUAL_VALUE,
                SUM(DEN_VALUE) AS REF_VALUE,
                SUM(NUM_VALUE * WEIGHT_SOS) AS KPI_ACTUAL_WT_VAL,
                SUM(DEN_VALUE) AS KPI_REF_VAL,
                SUM(DEN_VALUE * WEIGHT_SOS) AS KPI_REF_WT_VAL,
                rtrim(PHOTO_URL) as PHOTO_URL,
                rtrim(STORE_GRADE) as STORE_GRADE
            FROM 
                (
                    SELECT DATASET,
                        CUSTOMERID,
                        SALESPERSONID,
                        KPI,
                        TRANS.SCHEDULEDDATE,
                        LATESTDATE,
                        FISC_YR,
                        FISC_PER,
                        MERCHANDISER_NAME,
                        TRANS.CUSTOMERNAME,
                        TRANS.COUNTRY,
                        STATE,
                        PARENT_CUSTOMER,
                        RETAIL_ENVIRONMENT,
                        CHANNEL,
                        RETAILER,
                        BUSINESS_UNIT,
                        PRIORITY_STORE_FLAG,
                        TRANS.PROD_HIER_L4,
                        TRANS.PROD_HIER_L5,
                        MAX(KPI_CHNL_WT) AS KPI_CHNL_WT,
                        MAX(CHANNEL_WEIGHTAGE) AS CHANNEL_WEIGHTAGE,
                        MAX(WEIGHT_SOS) AS WEIGHT_SOS,
                        MAX(SALIENCE_VAL) AS SALIENCE_VAL,
                        SUM(NUM_VALUE) AS NUM_VALUE,
                        SUM(DEN_VALUE) AS DEN_VALUE,
                        PHOTO_URL,
                        STORE_GRADE
                    FROM (
                            SELECT 'SOS COMPLIANCE' AS DATASET,
                                CUSTOMERID,
                                SALESPERSONID,
                                'PERFECT STORE SCORE' AS KPI,
                                SCHEDULEDDATE,
                                LATESTDATE,
                                FISC_YR,
                                FISC_PER,
                                MERCHANDISER_NAME,
                                CUSTOMERNAME,
                                COUNTRY,
                                STATE,
                                PARENT_CUSTOMER,
                                RETAIL_ENVIRONMENT,
                                CHANNEL,
                                RETAILER,
                                BUSINESS_UNIT,
                                PRIORITY_STORE_FLAG,
                                KPI_CHNL_WT,
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
                                END AS DEN_VALUE,
                                PHOTO_URL,
                                STORE_GRADE
                            FROM EDW_PERFECT_STORE_KPI_DATA
                            WHERE UPPER(KPI) = 'SOS COMPLIANCE'
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
                        CUSTOMERID,
                        SALESPERSONID,
                        KPI,
                        TRANS.SCHEDULEDDATE,
                        LATESTDATE,
                        FISC_YR,
                        FISC_PER,
                        MERCHANDISER_NAME,
                        TRANS.CUSTOMERNAME,
                        TRANS.COUNTRY,
                        STATE,
                        PARENT_CUSTOMER,
                        RETAIL_ENVIRONMENT,
                        CHANNEL,
                        RETAILER,
                        BUSINESS_UNIT,
                        PRIORITY_STORE_FLAG,
                        TRANS.PROD_HIER_L4,
                        TRANS.PROD_HIER_L5,
                        PHOTO_URL,
                        STORE_GRADE
                )
            GROUP BY 
                rtrim(DATASET),
                rtrim(CUSTOMERID),
                rtrim(SALESPERSONID),
                rtrim(KPI),
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM'),
                rtrim(LATESTDATE),
                rtrim(FISC_YR),
                rtrim(FISC_PER),
                rtrim(MERCHANDISER_NAME),
                rtrim(CUSTOMERNAME),
                rtrim(COUNTRY),
                rtrim(STATE),
                rtrim(PARENT_CUSTOMER),
                rtrim(RETAIL_ENVIRONMENT),
                rtrim(CHANNEL),
                rtrim(RETAILER),
                rtrim(BUSINESS_UNIT),
                rtrim(PRIORITY_STORE_FLAG),
                rtrim(PHOTO_URL),
                rtrim(STORE_GRADE)
        ) SOS
),
soa as
(   
    SELECT 
        DATASET,
        CUSTOMERID,
        SALESPERSONID,
        KPI,
        TO_DATE(YM || '15', 'YYYYMMDD') AS SCHEDULEDDATE,
        LATESTDATE,
        FISC_YR,
        FISC_PER,
        MERCHANDISER_NAME,
        CUSTOMERNAME,
        COUNTRY,
        STATE,
        PARENT_CUSTOMER,
        RETAIL_ENVIRONMENT,
        CHANNEL,
        RETAILER,
        BUSINESS_UNIT,
        PRIORITY_STORE_FLAG,
        KPI_CHNL_WT,
        CHANNEL_WEIGHTAGE,
        SALIENCE_VAL,
        ACTUAL_VALUE,
        REF_VALUE,
        KPI_ACTUAL_WT_VAL,
        KPI_REF_VAL,
        KPI_REF_WT_VAL,
        PHOTO_URL,
        STORE_GRADE
    FROM 
        (
            SELECT 
                rtrim(DATASET) as DATASET,
                rtrim(CUSTOMERID) as CUSTOMERID,
                rtrim(SALESPERSONID) as SALESPERSONID,
                rtrim(KPI) as KPI,
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM') AS YM,
                rtrim(LATESTDATE) as LATESTDATE,
                rtrim(FISC_YR) as FISC_YR,
                rtrim(FISC_PER) as FISC_PER,
                rtrim(MERCHANDISER_NAME) as MERCHANDISER_NAME,
                rtrim(CUSTOMERNAME) as CUSTOMERNAME,
                rtrim(COUNTRY) as COUNTRY,
                rtrim(STATE) as STATE,
                rtrim(PARENT_CUSTOMER) as PARENT_CUSTOMER,
                rtrim(RETAIL_ENVIRONMENT) as RETAIL_ENVIRONMENT,
                rtrim(CHANNEL) as CHANNEL,
                rtrim(RETAILER) as RETAILER,
                rtrim(BUSINESS_UNIT) as BUSINESS_UNIT,
                rtrim(PRIORITY_STORE_FLAG) as PRIORITY_STORE_FLAG,
                MAX(KPI_CHNL_WT) AS KPI_CHNL_WT,
                MAX(CHANNEL_WEIGHTAGE) AS CHANNEL_WEIGHTAGE,
                MAX(SALIENCE_VAL) AS SALIENCE_VAL,
                SUM(NUM_VALUE) AS ACTUAL_VALUE,
                SUM(DEN_VALUE) AS REF_VALUE,
                SUM(NUM_VALUE * WEIGHT_SOA) AS KPI_ACTUAL_WT_VAL,
                SUM(DEN_VALUE) AS KPI_REF_VAL,
                SUM(DEN_VALUE * WEIGHT_SOA) AS KPI_REF_WT_VAL,
                rtrim(PHOTO_URL) as PHOTO_URL,
                rtrim(STORE_GRADE) as STORE_GRADE
            FROM 
                (
                    SELECT DATASET,
                        CUSTOMERID,
                        SALESPERSONID,
                        KPI,
                        TRANS.SCHEDULEDDATE,
                        LATESTDATE,
                        FISC_YR,
                        FISC_PER,
                        MERCHANDISER_NAME,
                        TRANS.CUSTOMERNAME,
                        TRANS.COUNTRY,
                        STATE,
                        PARENT_CUSTOMER,
                        RETAIL_ENVIRONMENT,
                        CHANNEL,
                        RETAILER,
                        BUSINESS_UNIT,
                        PRIORITY_STORE_FLAG,
                        TRANS.PROD_HIER_L4,
                        TRANS.PROD_HIER_L5,
                        MAX(KPI_CHNL_WT) AS KPI_CHNL_WT,
                        MAX(CHANNEL_WEIGHTAGE) AS CHANNEL_WEIGHTAGE,
                        MAX(WEIGHT_SOA) AS WEIGHT_SOA,
                        MAX(SALIENCE_VAL) AS SALIENCE_VAL,
                        SUM(NUM_VALUE) AS NUM_VALUE,
                        SUM(DEN_VALUE) AS DEN_VALUE,
                        PHOTO_URL,
                        STORE_GRADE
                    FROM (
                            SELECT 'SOA COMPLIANCE' AS DATASET,
                                CUSTOMERID,
                                SALESPERSONID,
                                'PERFECT STORE SCORE' AS KPI,
                                SCHEDULEDDATE,
                                LATESTDATE,
                                FISC_YR,
                                FISC_PER,
                                MERCHANDISER_NAME,
                                CUSTOMERNAME,
                                COUNTRY,
                                STATE,
                                PARENT_CUSTOMER,
                                RETAIL_ENVIRONMENT,
                                CHANNEL,
                                RETAILER,
                                BUSINESS_UNIT,
                                PRIORITY_STORE_FLAG,
                                KPI_CHNL_WT,
                                CHANNEL_WEIGHTAGE,
                                ROUND(WEIGHT_SOA, 4) AS WEIGHT_SOA,
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
                                END AS DEN_VALUE,
                                PHOTO_URL,
                                STORE_GRADE
                            FROM EDW_PERFECT_STORE_KPI_DATA
                            WHERE UPPER(KPI) = 'SOA COMPLIANCE'
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
                            WHERE UPPER(KPI) = 'SOA COMPLIANCE'
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
                        CUSTOMERID,
                        SALESPERSONID,
                        KPI,
                        TRANS.SCHEDULEDDATE,
                        LATESTDATE,
                        FISC_YR,
                        FISC_PER,
                        MERCHANDISER_NAME,
                        TRANS.CUSTOMERNAME,
                        TRANS.COUNTRY,
                        STATE,
                        PARENT_CUSTOMER,
                        RETAIL_ENVIRONMENT,
                        CHANNEL,
                        RETAILER,
                        BUSINESS_UNIT,
                        PRIORITY_STORE_FLAG,
                        TRANS.PROD_HIER_L4,
                        TRANS.PROD_HIER_L5,
                        PHOTO_URL,
                        STORE_GRADE
                )
            GROUP BY 
                rtrim(DATASET),
                rtrim(CUSTOMERID),
                rtrim(SALESPERSONID),
                rtrim(KPI),
                TO_CHAR(SCHEDULEDDATE, 'YYYYMM'),
                rtrim(LATESTDATE),
                rtrim(FISC_YR),
                rtrim(FISC_PER),
                rtrim(MERCHANDISER_NAME),
                rtrim(CUSTOMERNAME),
                rtrim(COUNTRY),
                rtrim(STATE),
                rtrim(PARENT_CUSTOMER),
                rtrim(RETAIL_ENVIRONMENT),
                rtrim(CHANNEL),
                rtrim(RETAILER),
                rtrim(BUSINESS_UNIT),
                rtrim(PRIORITY_STORE_FLAG),
                rtrim(PHOTO_URL),
                rtrim(STORE_GRADE)
        ) soa
),
agg_perfect_store_kpi_data as 
(   
    select
        null as hashkey,
        null as hash_row,
        dataset,
        customerid,
        salespersonid,
        null as visitid,
        null as questiontext,
        null as productid,
        kpi,
        scheduleddate,
        latestdate,
        fisc_yr,
        fisc_per,
        merchandiser_name,
        customername,
        country,
        state,
        parent_customer,
        retail_environment,
        channel,
        retailer,
        business_unit,
        null as eannumber,
        null as matl_num,
        null as prod_hier_l1,
        null as prod_hier_l2,
        null as prod_hier_l3,
        null as prod_hier_l4,
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
        null as mkt_share,
        salience_val,
        actual_value,
        ref_value,
        kpi_actual_wt_val,
        kpi_ref_val,
        kpi_ref_wt_val,
        photo_url,
        null as compliance,
        null as gap_to_target,
        null as compliance_propogated,
        null as gap_propagated,
        null as full_opportunity_lcy,
        null as weighted_opportunity_lcy,
        null as full_opportunity_usd,
        null as weighted_opportunity_usd,
        null as sotp_lcy,
        null as sotp_usd,
        store_grade
    from
    (   
        select * from msl 
        union all
        select * from osa 
        union all
        select * from promo 
        union all
        select * from display 
        union all
        select * from pog_others 
        union all
        select * from pog_anz 
        union all
        select * from sos 
        union all
        select * from soa
    )
),
sotp as
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
        compliance_propogated,
        gap_propagated,
        full_opportunity_lcy,
        weighted_opportunity_lcy,
        full_opportunity_usd,
        weighted_opportunity_usd,
        (
            nullif(weighted_opportunity_lcy, 0) / nullif(compliance_propogated, 0)
        ) * (
            case
                when (
                    coalesce(mkt_share, 0) - coalesce(compliance_propogated, 0)
                ) < '0' then '0'
                else (
                    coalesce(mkt_share, 0) - coalesce(compliance_propogated, 0)
                )
            END
        ) as sotp_lcy,
        (
            nullif(weighted_opportunity_usd, 0) / nullif(compliance_propogated, 0)
        ) * (
            case
                when (
                    coalesce(mkt_share, 0) - coalesce(compliance_propogated, 0)
                ) < '0' then '0'
                else (
                    coalesce(mkt_share, 0) - coalesce(compliance_propogated, 0)
                )
            END
        ) as sotp_usd,
        null as store_grade
    from 
        (
            select 
                DATASET,
                'SIZE OF THE PRIZE' AS KPI,
                SCHEDULEDDATE,
                LATESTDATE,
                FISC_YR,
                PROP.FISC_PER,
                PROP.COUNTRY,
                PROP.PARENT_CUSTOMER,
                PROP.RETAIL_ENVIRONMENT,
                CHANNEL,
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
                --PROP.gcph_category,
                --PROP.gcph_subcategory,
                compliance,
                gap_to_target,
                compliance_propogated,
                gap_propagated,
                "nts_lcy" AS full_opportunity_lcy,
                case
                    when ("nts_lcy" * kpi_chnl_wt) is null then (
                        max("nts_lcy" * kpi_chnl_wt) over (
                            partition by dataset,
                            prop.country,
                            prop.parent_customer,
                            prod_hier_l4,
                            prop.fisc_yr
                            order by dataset,
                                prop.country,
                                prop.parent_customer,
                                prod_hier_l4,
                                prop.fisc_yr,
                                prop.fisc_per rows between unbounded preceding and current row
                        )
                    )
                    else ("nts_lcy" * kpi_chnl_wt)
                end as weighted_opportunity_lcy,
                "nts_usd" AS full_opportunity_usd,
                case
                    when ("nts_usd" * kpi_chnl_wt) is null then (
                        max("nts_usd" * kpi_chnl_wt) over (
                            partition by dataset,
                            prop.country,
                            prop.parent_customer,
                            prod_hier_l4,
                            prop.fisc_yr
                            order by dataset,
                                prop.country,
                                prop.parent_customer,
                                prod_hier_l4,
                                prop.fisc_yr,
                                prop.fisc_per rows between unbounded preceding and current row
                        )
                    )
                    else ("nts_usd" * kpi_chnl_wt)
                end AS weighted_opportunity_usd
            from 
                (
                    select * FROM EDW_PERFECT_STORE_REBASE_WT_temp 
                    WHERE kpi = 'SIZE OF THE PRIZE PROP'
                ) PROP
                left join 
                (
                    -- Non Wholesaler NTS Data by Account Banner
                    SELECT "parent_customer" as parent_customer,
                        "retail_environment" as retail_environment,
                        "trax_category" as trax_category,
                        "trax_sub_category" as trax_sub_category,
                        target_start_date,
                        target_end_date,
                        SUM("nts_lcy") AS "nts_lcy",
                        SUM("nts_usd") AS "nts_usd"
                    FROM
                        (
                            (
                                SELECT LTRIM(ean_number, '0') AS "ean",
                                    category_local_name AS "trax_category",
                                    subcategory_local_name AS "trax_sub_category"
                                FROM edw_vw_perfect_store_trax_products
                                GROUP BY 1,
                                    2,
                                    3
                            ) trax_master
                            JOIN 
                            (
                                SELECT 'Australia' AS "country",
                                    'SAP' AS "datasource",
                                    CASE
                                        WHEN UPPER(sales_office_desc) IN ('COLES TEAM', 'WOOLWORTHS TEAM') THEN 'AU MAJOR CHAIN SUPER'
                                        WHEN UPPER(sales_grp_desc) = 'MCG - MY CHEMIST' THEN 'BIG BOX'
                                        ELSE UPPER(sales_office_desc)
                                    END AS "retail_environment",
                                    CASE
                                        WHEN UPPER(sales_grp_desc) = 'MCG - MY CHEMIST' THEN 'CHEMIST WAREHOUSE'
                                        ELSE UPPER(sales_grp_desc)
                                    END AS "parent_customer",
                                    LTRIM(edw_sales_reporting.bar_cd, '0') AS "ean",
                                    edw_sales_reporting.jj_mnth AS "month",
                                    edw_sales_reporting.jj_year AS "year",
                                    --edw_sales_reporting.jj_period AS fisc_per,
                                    c.target_start_date as target_start_date,
                                    c.target_end_date as target_end_date,
                                    SUM(
                                        CASE
                                            WHEN to_ccy = 'AUD' THEN nts_val
                                            ELSE 0
                                        END
                                    ) AS "nts_lcy",
                                    SUM(
                                        CASE
                                            WHEN to_ccy = 'USD' THEN nts_val
                                            ELSE 0
                                        END
                                    ) AS "nts_usd"
                                FROM edw_sales_reporting
                                    LEFT JOIN 
                                    (
                                        SELECT market,
                                            re,
                                            attribute_2,
                                            target_start_date,
                                            target_end_date,
                                            ref_start_date,
                                            ref_end_date,
                                            multiplier
                                        FROM sdl_mds_pacific_ps_benchmarks
                                        WHERE UPPER(attribute_1) = 'RE'
                                    ) c ON CASE
                                        WHEN upper(edw_sales_reporting.sales_office_desc::text) = 'COLES TEAM'::text
                                        OR upper(edw_sales_reporting.sales_office_desc::text) = 'WOOLWORTHS TEAM'::text THEN 'AU MAJOR CHAIN SUPER'::text
                                        WHEN upper(edw_sales_reporting.sales_grp_desc::text) = 'MCG - MY CHEMIST'::text THEN 'BIG BOX'::text
                                        ELSE upper(edw_sales_reporting.sales_office_desc::text)
                                    END::character varying = UPPER(c.re::character varying)
                                    AND c.market = 'Australia'
                                    AND TO_DATE(
                                        '01 ' || RIGHT(edw_sales_reporting.jj_period::text, 2) || ' ' || edw_sales_reporting.jj_year::text,
                                        'DD MM YYYY'
                                    ) >= ref_start_date
                                    AND TO_DATE(
                                        '01 ' || RIGHT(edw_sales_reporting.jj_period::text, 2) || ' ' || edw_sales_reporting.jj_year::text,
                                        'DD MM YYYY'
                                    ) <= ref_end_date
                                WHERE --jj_year > EXTRACT(YEAR FROM SYSDATE) - 2 AND
                                    UPPER(sales_grp_desc) IN (
                                        'WOOLWORTHS',
                                        'COLES',
                                        'MCG - MY CHEMIST',
                                        'METCASH'
                                    )
                                GROUP BY 1,
                                    2,
                                    3,
                                    4,
                                    5,
                                    6,
                                    7,
                                    8,
                                    9
                            ) sap ON sap."ean" = trax_master."ean"
                        )
                    group by 1,
                        2,
                        3,
                        4,
                        5,
                        6
                    UNION ALL
                    -- Wholesaler NTS Data by Account Banner
                    SELECT 
                        "parent_customer" as parent_customer,
                        "retail_environment" as retail_environment,
                        "trax_category" as trax_category,
                        "trax_sub_category" as trax_sub_category,
                        target_start_date,
                        target_end_date,
                        SUM("nts_lcy") AS "nts_lcy",
                        SUM("nts_usd") AS "nts_usd"
                    FROM
                        (
                            (
                                SELECT LTRIM(ean_number, '0') AS "ean",
                                    category_local_name AS "trax_category",
                                    subcategory_local_name AS "trax_sub_category"
                                FROM edw_vw_perfect_store_trax_products
                                GROUP BY 1,
                                    2,
                                    3
                            ) trax_master
                            JOIN 
                            (
                                SELECT b.ctry_nm AS "country",
                                    'Perenso' AS "datasource",
                                    CASE
                                        WHEN UPPER(acct_banner) = 'PRICELINE CORPORATE' THEN 'BEAUTY'
                                        ELSE 'AU INDY PHARMACY'
                                    END AS "retail_environment",
                                    UPPER(acct_banner) AS "parent_customer",
                                    LTRIM(prod_ean, '0') AS "ean",
                                    jj_mnth AS "month",
                                    jj_year AS "year",
                                    --jj_mnth_id AS fisc_per,
                                    c.target_start_date,
                                    c.target_end_date,
                                    SUM(aud_nis) AS "nts_lcy",
                                    SUM(usd_nis) AS "nts_usd" -- This appears to be 0, need to join to exchange rate table to get USD value
                                FROM edw_pacific_perenso_ims_analysis a
                                    LEFT JOIN edw_product_key_attributes b ON a.prod_ean = b.ean_upc
                                    AND b.ctry_nm = 'Australia'
                                    LEFT JOIN (
                                        SELECT market,
                                            re,
                                            attribute_2,
                                            target_start_date,
                                            target_end_date,
                                            ref_start_date,
                                            ref_end_date,
                                            multiplier
                                        FROM sdl_mds_pacific_ps_benchmarks
                                        WHERE UPPER(attribute_1) = 'RE'
                                    ) c ON CASE
                                        WHEN upper(a.acct_banner::text) = 'PRICELINE CORPORATE'::text THEN 'BEAUTY'::text
                                        ELSE 'AU INDY PHARMACY'::text
                                    END::character varying = UPPER(c.re)
                                    AND b.ctry_nm = c.market
                                    AND a.delvry_dt >= ref_start_date
                                    AND a.delvry_dt <= ref_end_date
                                WHERE order_type = 'Shipped Weekly' --AND jj_year > EXTRACT(YEAR FROM SYSDATE) - 2
                                    AND UPPER(acct_banner) NOT IN (
                                        'WOOLWORTHS',
                                        'COLES',
                                        'MCG - MY CHEMIST',
                                        'METCASH'
                                    )
                                GROUP BY 1,
                                    2,
                                    3,
                                    4,
                                    5,
                                    6,
                                    7,
                                    8,
                                    9
                            ) perenso ON perenso."ean" = trax_master."ean"
                        )
                    group by 1,
                        2,
                        3,
                        4,
                        5,
                        6
                ) nts ON upper(PROP.parent_customer::text) = upper(nts.parent_customer::text)
                AND PROP.scheduleddate >= nts.target_start_date
                AND PROP.scheduleddate <= nts.target_end_date
                AND upper(PROP.retail_environment::text) = upper(nts.retail_environment::text)
                AND upper(PROP.prod_hier_l4) = upper(nts.trax_sub_category)
        )
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
        "y/n_flag"::varchar(150) as "y/n_flag",
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
        gap_propagated::number(14,3) as gap_propagated,
        full_opportunity_lcy::number(14,4) as full_opportunity_lcy,
        weighted_opportunity_lcy::number(14,4) as weighted_opportunity_lcy,
        full_opportunity_usd::number(14,4) as full_opportunity_usd,
        weighted_opportunity_usd::number(14,4) as weighted_opportunity_usd,
        sotp_lcy::number(14,3) as sotp_lcy,
        sotp_usd::number(14,3) as sotp_usd,
        store_grade::varchar(50) as store_grade
    from 
    (
        select * from union_1 
        union all
        select * from agg_perfect_store_kpi_data
        union all 
        select * from sotp       
    )
)
select * from final