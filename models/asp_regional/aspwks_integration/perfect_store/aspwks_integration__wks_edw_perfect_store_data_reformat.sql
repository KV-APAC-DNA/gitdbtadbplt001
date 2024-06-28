with edw_perfect_store as 
(
    select * from snapaspedw_integration.edw_perfect_store
),
itg_mds_rg_ps_channel_weights as 
(
    select * from snapaspitg_integration.itg_mds_rg_ps_channel_weights
),
itg_mds_rg_ps_market_coverage as 
(
    select * from snapaspitg_integration.itg_mds_rg_ps_market_coverage
),
msl as 
(
    select 
        dataset as dataset,
        customerid as customerid,
        salespersonid as salespersonid,
        visitid as visitid,
        questiontext as questiontext,
        productid as productid,
        kpi as kpi,
        scheduleddate as scheduleddate,
        fisc_yr as fisc_yr,
        fisc_per as fisc_per,
        case
            when nullif(trim(lastname), '') is not null then firstname || ',' || lastname
            else firstname
        end as merchandiser_name,
        customername as customername,
        country as country,
        state as state,
        storereference as parent_customer,
        storetype as retail_environment,
        channel as channel,
        salesgroup as retailer,
        bu as business_unit,
        eannumber as eannumber,
        matl_num as matl_num,
        prod_hier_l1 as prod_hier_l1,
        prod_hier_l2 as prod_hier_l2,
        prod_hier_l3 as prod_hier_l3,
        prod_hier_l4 as prod_hier_l4,
        prod_hier_l5 as prod_hier_l5,
        prod_hier_l6 as prod_hier_l6,
        prod_hier_l7 as prod_hier_l7,
        prod_hier_l8 as prod_hier_l8,
        productname as prod_hier_l9,
        kpi_chnl_wt as kpi_chnl_wt,
        mkt_share as mkt_share,
        ques_desc as ques_type,
        "y/n_flag" as "y/n_flag",
        priority_store_flag as priority_store_flag,
        response,
        response_score,
        null as add_info,
        case
            when upper(presence) = 'TRUE' then 1
            else 0
        end as actual_value,
        1 as ref_value,
        photo_url
    from edw_perfect_store
    where upper(kpi) = 'MSL COMPLIANCE'
        AND UPPER(VST_STATUS) = 'COMPLETED'
        AND UPPER(MUSTCARRYITEM) = 'TRUE'
),
oos as 
(
    select --'ps_data' as identifier,
        dataset as dataset,
        customerid as customerid,
        salespersonid as salespersonid,
        visitid as visitid,
        questiontext as questiontext,
        productid as productid,
        kpi as kpi,
        scheduleddate as scheduleddate,
        fisc_yr as fisc_yr,
        fisc_per as fisc_per,
        case
            when nullif(trim(lastname), '') is not null then firstname || ',' || lastname
            else firstname
        end as merchandiser_name,
        customername as customername,
        country as country,
        state as state,
        storereference as parent_customer,
        storetype as retail_environment,
        channel as channel,
        salesgroup as retailer,
        bu as business_unit,
        eannumber as eannumber,
        matl_num as matl_num,
        prod_hier_l1 as prod_hier_l1,
        prod_hier_l2 as prod_hier_l2,
        prod_hier_l3 as prod_hier_l3,
        prod_hier_l4 as prod_hier_l4,
        prod_hier_l5 as prod_hier_l5,
        prod_hier_l6 as prod_hier_l6,
        prod_hier_l7 as prod_hier_l7,
        prod_hier_l8 as prod_hier_l8,
        productname as prod_hier_l9,
        kpi_chnl_wt as kpi_chnl_wt,
        mkt_share as mkt_share,
        ques_desc as ques_type,
        "y/n_flag" as "y/n_flag",
        priority_store_flag as priority_store_flag,
        response,
        response_score,
        outofstock as add_info,
        case
            when upper(outofstock) <> '' then 1
            else 0
        end as actual_value,
        1 as ref_value,
        photo_url
    from edw_perfect_store
    WHERE UPPER(KPI) = 'OOS COMPLIANCE'
        AND UPPER(VST_STATUS) = 'COMPLETED'
        AND UPPER(MUSTCARRYITEM) = 'TRUE'
        AND UPPER(PRESENCE) = 'TRUE'
),
other as 
(
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
        CATEGORY AS PROD_HIER_L4,
        SEGMENT AS PROD_HIER_L5,
        BRAND AS PROD_HIER_L6,
        PROD_HIER_L7 AS PROD_HIER_L7,
        PROD_HIER_L8 AS PROD_HIER_L8,
        PRODUCTNAME AS PROD_HIER_L9,
        KPI_CHNL_WT AS KPI_CHNL_WT,
        MKT_SHARE AS MKT_SHARE,
        QUES_DESC AS QUES_TYPE,
        "Y/N_FLAG" AS "Y/N_FLAG",
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
                    WHEN LTRIM(VALUE) ~ '^[0-9]+\\.[0-9]+$'
                    OR LTRIM(VALUE) ~ '^[0-9]+$' THEN CAST(VALUE AS NUMERIC(14, 4))
                    ELSE NULL
                END
            )
            WHEN UPPER(KPI) = 'PROMO COMPLIANCE' THEN (
                CASE
                    WHEN UPPER(COUNTRY) = 'INDONESIA'
                    AND UPPER("Y/N_FLAG") = 'YES'
                    AND UPPER(POSM_EXECUTION_FLAG) = 'Y' THEN 1
                    WHEN UPPER(COUNTRY) = 'INDONESIA'
                    AND UPPER("Y/N_FLAG") = 'YES'
                    AND UPPER(POSM_EXECUTION_FLAG) = 'N' THEN 0.5
                    WHEN UPPER(COUNTRY) = 'MALAYSIA'
                    AND UPPER("Y/N_FLAG") = 'YES'
                    AND UPPER(POSM_EXECUTION_FLAG) = 'N' THEN 1
                    WHEN UPPER(COUNTRY) = 'MALAYSIA'
                    AND UPPER("Y/N_FLAG") = 'YES'
                    AND UPPER(POSM_EXECUTION_FLAG) = 'Y' THEN 0.5
                    WHEN UPPER(COUNTRY) NOT IN ('INDONESIA', 'MALAYSIA')
                    AND UPPER("Y/N_FLAG") = 'YES' THEN 1
                    ELSE 0
                END
            )
            WHEN UPPER(KPI) = 'DISPLAY COMPLIANCE' THEN (
                CASE
                    WHEN UPPER("Y/N_FLAG") = 'YES' THEN 1
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
                            WHEN UPPER("Y/N_FLAG") = 'YES' THEN 1
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
                    WHEN LTRIM(VALUE) ~ '^[0-9]+\\.[0-9]+$'
                    OR LTRIM(VALUE) ~ '^[0-9]+$' THEN CAST(VALUE AS NUMERIC(14, 4))
                    ELSE NULL
                END
            )
            WHEN UPPER(KPI) = 'PROMO COMPLIANCE' THEN (
                CASE
                    WHEN UPPER("Y/N_FLAG") = 'NO'
                    OR UPPER("Y/N_FLAG") = 'YES' THEN 1
                    ELSE 0
                END
            )
            WHEN UPPER(KPI) = 'DISPLAY COMPLIANCE' THEN (
                CASE
                    WHEN UPPER("Y/N_FLAG") = 'NO'
                    OR UPPER("Y/N_FLAG") = 'YES' THEN 1
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
                            WHEN UPPER("Y/N_FLAG") = 'YES'
                            OR UPPER("Y/N_FLAG") = 'NO' THEN 1
                            ELSE 0
                        END
                    )
                END
            )
            ELSE 0
        END AS REF_VALUE,
        PHOTO_URL
    FROM EDW_PERFECT_STORE
    WHERE UPPER(KPI) NOT IN ('MSL COMPLIANCE', 'OOS COMPLIANCE')
),
ps_data as 
(
    select * from msl
    union all
    select * from oos
    union all
    select * from other
),
final as
(   
    SELECT 
        PS_DATA.DATASET,
        PS_DATA.CUSTOMERID,
        PS_DATA.SALESPERSONID,
        PS_DATA.VISITID,
        PS_DATA.QUESTIONTEXT,
        PS_DATA.PRODUCTID,
        PS_DATA.KPI,
        PS_DATA.SCHEDULEDDATE,
        CASE
            WHEN UPPER(PS_DATA.COUNTRY) IN ('AUSTRALIA', 'NEW ZEALAND') THEN to_date (ADD_MONTHS (TO_DATE(SUBSTRING(to_date(convert_timezone('UTC', current_timestamp())), 1, 7) || '-15',    'YYYY-MM-DD'),-2))
            ELSE to_date(ADD_MONTHS (TO_DATE(SUBSTRING(to_date(convert_timezone('UTC', current_timestamp())), 1, 7) || '-15','YYYY-MM-DD'),-1))
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
        PS_DATA."Y/N_FLAG",
        PS_DATA.PRIORITY_STORE_FLAG,
        PS_DATA.ADD_INFO,
        PS_DATA.RESPONSE,
        PS_DATA.RESPONSE_SCORE,
        PS_DATA.KPI_CHNL_WT,
        PS_DATA.MKT_SHARE,
        SALIENCE.COVERAGE AS SALIENCE_VAL,
        CHNL_WEIGHT.WEIGHT AS CHANNEL_WEIGHTAGE,
        PS_DATA.ACTUAL_VALUE,
        PS_DATA.REF_VALUE,
        PS_DATA.PHOTO_URL
    FROM PS_DATA 
        --JOIN (SELECT 'PS_DATA' AS IDENTIFIER,
        --TO_DATE(DATE_PART('YEAR',SYSDATE) ||DATE_PART ('MONTH',SYSDATE) ||15,'YYYYMMDD') AS LATEST_DATE
        --FROM EDW_PERFECT_STORE) MAX_DATE ON MAX_DATE.IDENTIFIER = PS_DATA.IDENTIFIER
        LEFT JOIN itg_mds_rg_ps_channel_weights CHNL_WEIGHT ON UPPER (PS_DATA.COUNTRY) = UPPER (CHNL_WEIGHT.COUNTRY)
        AND 
        (
            CASE
                WHEN UPPER (CHNL_WEIGHT.channel_re) = 'RE' THEN UPPER (PS_DATA.RETAIL_ENVIRONMENT)
                ELSE UPPER (PS_DATA.CHANNEL)
            END
        ) = UPPER (CHNL_WEIGHT.CHANNEL_RE_VALUE)
        LEFT JOIN ITG_MDS_RG_PS_MARKET_COVERAGE SALIENCE ON UPPER (PS_DATA.COUNTRY) = UPPER (SALIENCE.CNTY_NM)
        AND UPPER (PS_DATA.CHANNEL) = UPPER (SALIENCE.CHANNEL)
    WHERE DATE_PART(year, SCHEDULEDDATE) >= (DATE_PART(year, convert_timezone('UTC', current_timestamp())) -1)
)
select * from final