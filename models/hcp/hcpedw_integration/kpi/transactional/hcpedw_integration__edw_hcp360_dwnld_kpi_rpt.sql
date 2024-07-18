with vw_edw_hcp360_hcpmaster_dim_golden_record as(
    select * from {{ ref('hcpedw_integration__vw_edw_hcp360_hcpmaster_dim_golden_record') }}
),
vw_edw_hcp360_hcpmaster_dim as(
    select * from {{ ref('hcpedw_integration__vw_edw_hcp360_hcpmaster_dim') }}
),
vw_edw_hcp360_hcpmaster_dim_nongolden_record as(
    select * from {{ ref('hcpedw_integration__vw_edw_hcp360_hcpmaster_dim_nongolden_record') }}
),
vw_edw_hcp360_hcpmaster_dim_reject_record as(
    select * from {{ ref('hcpedw_integration__vw_edw_hcp360_hcpmaster_dim_reject_record') }}
),
edw_hcp360_in_ventasys_samples_fact as (
    select * from {{ ref('hcpedw_integration__edw_hcp360_in_ventasys_samples_fact') }}
),
edw_hcp360_kpi_rpt as(
    select * from {{ ref('hcpedw_integration__edw_hcp360_kpi_rpt') }}
),
union_1 as (
    select BRAND,
        VENTASYS_ID,
        VENTASYS_NAME,
        VENTASYS_MOBILE,
        VENTASYS_EMAIL,
        VEEVA_ID,
        VEEVA_NAME,
        VEEVA_MOBILE,
        VEEVA_EMAIL,
        SFMC_ID,
        SFMC_NAME,
        SFMC_MOBILE,
        SFMC_EMAIL,
        MASTER_HCP_KEY,
        SOURCE_SYSTEM,
        HCP_ID,
        CUSTOMER_NAME,
        CELL_PHONE,
        EMAIL,
        ACCOUNT_SOURCE_ID,
        VENTASYS_TEAM_NAME,
        VENTASYS_CUSTID,
        SUBSCRIBER_KEY,
        DATA_SOURCE,
        COUNTRY_CODE,
        REGION,
        ZONE,
        AREA,
        classification,
        speciality,
        core_noncore,
        null::VARCHAR(12) as YEAR,
        null::VARCHAR(11) as MONTH,
        null::DATE as HCP_CREATED_DATE,
        null::VARCHAR(20) as TERRITORY_ID,
        null::VARCHAR(50) as REGION_HQ,
        null::VARCHAR(1) as IS_ACTIVE,
        null::VARCHAR(1) as IS_ACTIVE_MSR,
        null::DATE as FIRST_PRESCRIPTION_DATE,
        null::VARCHAR(14) as PRESCRIBER_TYPE,
        null::NUMBER(38, 5) as TOTAL_PRESCRIPTIONS,
        null::NUMBER(38, 2) as PRESCRIPTION_UNITS,
        null::NUMBER(18, 0) as UNIQUE_PRODUCT_TYPE,
        null::NUMBER(38, 0) as PLANNED_VISITS,
        null::NUMBER(18, 0) as ACTUAL_VISITS,
        null::NUMBER(18, 0) as PHONE_CONNECTS,
        null::NUMBER(18, 0) as VIDEO_CONNECTS,
        null::NUMBER(18, 0) as F2F_CONNECTS,
        null::NUMBER(38, 4) as AVG_PROD_DETAILED,
        null::NUMBER(18, 0) as LBL_GIVEN,
        null::NUMBER(18, 0) as SAMPLES_GIVEN,
        null::NUMBER(18, 0) as EMAILS_SENT,
        null::NUMBER(18, 0) as EMAILS_DELIVERED,
        null::NUMBER(18, 0) as EMAILS_OPENED,
        null::NUMBER(18, 0) as EMAILS_CLICKED,
        null::NUMBER(18, 0) as EMAILS_UNIQUE_CLICKED,
        null::NUMBER(18, 0) as EMAILS_UNSUSBSCRIBED,
        null::NUMBER(18, 0) as EMAILS_FORWARDED,
        null::NUMBER(38, 4) as AVERAGE_CALL_DURATION,
        null::NUMBER(18, 0) as EVENTS_REGISTERED,
        null::NUMBER(18, 0) as EVENTS_ATTENDED,
        null::NUMBER(18, 0) as EVENTS_AS_SPEAKER,
        null::NUMBER(38, 4) as AVG_KEY_MSGS_DELIVERED,
        null::VARCHAR(32) as MASTER_ID,
        null::DATE as ACTIVITY_DATE,
        null::VARCHAR(50) as PRESCRIPTION_ID
    from (
            SELECT 
                G.brand as BRAND,
                VENTASYS_ID as VENTASYS_ID,
                VENTASYS_NAME as VENTASYS_NAME,
                VENTASYS_MOBILE as VENTASYS_MOBILE,
                VENTASYS_EMAIL as VENTASYS_EMAIL,
                VEEVA_ID as VEEVA_ID,
                VEEVA_NAME as VEEVA_NAME,
                VEEVA_MOBILE as VEEVA_MOBILE,
                VEEVA_EMAIL as VEEVA_EMAIL,
                SFMC_ID as SFMC_ID,
                SFMC_NAME as SFMC_NAME,
                SFMC_MOBILE as SFMC_MOBILE,
                SFMC_EMAIL as SFMC_EMAIL,
                MASTER_HCP_KEY as MASTER_HCP_KEY,
                NULL as SOURCE_SYSTEM,
                NULL as HCP_ID,
                NULL as CUSTOMER_NAME,
                NULL as CELL_PHONE,
                NULL as EMAIL,
                NULL as ACCOUNT_SOURCE_ID,
                NULL as VENTASYS_TEAM_NAME,
                NULL as VENTASYS_CUSTID,
                NULL as SUBSCRIBER_KEY,
                'golden_record' as DATA_SOURCE,
                'IN' as COUNTRY_CODE,
                vhcp_region as REGION,
                vhcp_zone as ZONE,
                vhcp_territory as AREA,
                SPECIALITY as SPECIALITY,
                CLASSIFICATION as CLASSIFICATION,
                CORE_NONCORE as CORE_NONCORE
            FROM VW_EDW_HCP360_HCPMASTER_DIM_GOLDEN_RECORD G
                LEFT JOIN VW_EDW_HCP360_HCPMASTER_DIM M ON G.MASTER_HCP_KEY = M.HCP_MASTER_ID
                AND G.BRAND = M.BRAND
            UNION ALL
            SELECT 
                TEAM_NAME as BRAND,
                NULL as VENTASYS_ID,
                NULL as VENTASYS_NAME,
                NULL as VENTASYS_MOBILE,
                NULL as VENTASYS_EMAIL,
                NULL as VEEVA_ID,
                NULL as VEEVA_NAME,
                NULL as VEEVA_MOBILE,
                NULL as VEEVA_EMAIL,
                NULL as SFMC_ID,
                NULL as SFMC_NAME,
                NULL as SFMC_MOBILE,
                NULL as SFMC_EMAIL,
                NULL as MASTER_HCP_KEY,
                source_system as SOURCE_SYSTEM,
                hcp_id as HCP_ID,
                customer_name as CUSTOMER_NAME,
                cell_phone as CELL_PHONE,
                email as EMAIL,
                NULL as ACCOUNT_SOURCE_ID,
                NULL as VENTASYS_TEAM_NAME,
                NULL as VENTASYS_CUSTID,
                NULL as SUBSCRIBER_KEY,
                'nongolden_record' as DATA_SOURCE,
                'IN' as COUNTRY_CODE,
                vhcp_region as REGION,
                vhcp_zone as ZONE,
                vhcp_territory as AREA,
                speciality as SPECIALITY,
                classification as CLASSIFICATION,
                core_noncore as CORE_NONCORE
            FROM vw_edw_hcp360_hcpmaster_dim_nongolden_record N
                LEFT JOIN VW_EDW_HCP360_HCPMASTER_DIM M ON N.TEAM_NAME = M.BRAND
                AND (
                    N.HCP_ID = M.hcp_id_veeva
                    OR N.HCP_ID = M.hcp_id_ventasys
                )
            UNION ALL
            SELECT 
                R.brand as BRAND,
                NULL as VENTASYS_ID,
                NULL as VENTASYS_NAME,
                NULL as VENTASYS_MOBILE,
                NULL as VENTASYS_EMAIL,
                NULL as VEEVA_ID,
                NULL as VEEVA_NAME,
                NULL as VEEVA_MOBILE,
                NULL as VEEVA_EMAIL,
                NULL as SFMC_ID,
                NULL as SFMC_NAME,
                NULL as SFMC_MOBILE,
                NULL as SFMC_EMAIL,
                master_hcp_key as MASTER_HCP_KEY,
                NULL as SOURCE_SYSTEM,
                NULL as HCP_ID,
                NULL as CUSTOMER_NAME,
                MOBILE_PHONE as CELL_PHONE,
                PERSON_EMAIL as EMAIL,
                account_source_id as ACCOUNT_SOURCE_ID,
                ventasys_team_name as VENTASYS_TEAM_NAME,
                ventasys_custid as VENTASYS_CUSTID,
                subscriber_key as SUBSCRIBER_KEY,
                'reject_record' as DATA_SOURCE,
                'IN' as COUNTRY_CODE,
                vhcp_region as REGION,
                vhcp_zone as ZONE,
                vhcp_territory as AREA,
                speciality as SPECIALITY,
                classification as CLASSIFICATION,
                core_noncore as CORE_NONCORE
            FROM vw_edw_hcp360_hcpmaster_dim_reject_record r
                LEFT JOIN VW_EDW_HCP360_HCPMASTER_DIM M ON R.MASTER_HCP_KEY = M.HCP_MASTER_ID
                AND R.BRAND = M.BRAND
        )
),
BASE_union_2 AS (
    SELECT DISTINCT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        HCP_MASTER_ID,
        HCP_CREATED_DATE,
        TERRITORY_ID,
        REGION,
        ZONE,
        TERRITORY,
        REGION_HQ,
        SPECIALITY,
        CORE_NONCORE,
        CLASSIFICATION,
        FIELD_REP_ACTIVE,
        FIRST_PRESCRIPTION_DATE,
        PLANNED_CALL_CNT AS PLANNED_VISITS
    FROM EDW_HCP360_KPI_RPT
    WHERE SOURCE_SYSTEM = 'VENTASYS'
        AND TRANSACTION_FLAG = 'N'
        AND HCP_ID IS NOT NULL
),
SAMPLE1 AS (
    SELECT TEAM_BRAND_NAME,
        TO_CHAR(SAMPLE_DATE, 'YYYY') AS YEAR,
        TO_CHAR(SAMPLE_DATE, 'MON') AS MONTH,
        HCP_ID,
        COUNT(DISTINCT SAMPLE_PRODUCT) AS SAMPLES_GIVEN
    FROM EDW_HCP360_IN_VENTASYS_SAMPLES_FACT
    WHERE SAMPLE_PRODUCT LIKE '%Sample%'
        OR SAMPLE_PRODUCT LIKE '%(S)%'
    GROUP BY 1,
        2,
        3,
        4
),
LABLE AS (
    SELECT TEAM_BRAND_NAME,
        TO_CHAR(SAMPLE_DATE, 'YYYY') AS YEAR,
        TO_CHAR(SAMPLE_DATE, 'MON') AS MONTH,
        HCP_ID,
        COUNT(DISTINCT SAMPLE_PRODUCT) AS LABLES
    FROM edw_hcp360_in_ventasys_samples_fact
    WHERE SAMPLE_PRODUCT NOT LIKE '%Sample%'
        AND SAMPLE_PRODUCT NOT LIKE '%(S)%'
    GROUP BY 1,
        2,
        3,
        4
),
CALC_1 AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        COUNT(DISTINCT PRESCRIPTION_ID) AS TOTAL_PRESCRIPTIONS,
        SUM(NVL (NO_OF_PRESCRIPTION_UNITS, 0)) AS PRESCRIPTION_UNITS,
        AVG(CALL_DURATION) AS AVERAGE_CALL_DURATION
    FROM EDW_HCP360_KPI_RPT
    WHERE SOURCE_SYSTEM = 'VENTASYS'
        AND TRANSACTION_FLAG = 'Y'
    GROUP BY 1,
        2,
        3,
        4
),
UNIQPRODTYPE AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        COUNT(DISTINCT PRODUCT_INDICATION_NAME) AS UNIQPROD
    FROM EDW_HCP360_KPI_RPT
    WHERE SOURCE_SYSTEM = 'VENTASYS'
        AND PRESCRIPTION_ID IS NOT NULL
    GROUP BY 1,
        2,
        3,
        4
),
ACTUAL_VISITS AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        COUNT(DISTINCT CALL_SOURCE_ID) AS ACTUAL_VISITS
    FROM EDW_HCP360_KPI_RPT
    WHERE SOURCE_SYSTEM = 'VENTASYS'
        AND TRANSACTION_FLAG = 'Y'
        AND CALL_SOURCE_ID IS NOT NULL
    GROUP BY 1,
        2,
        3,
        4
),
PHONE AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        COUNT(DISTINCT CALL_SOURCE_ID) AS PHONES
    FROM EDW_HCP360_KPI_RPT
    WHERE SOURCE_SYSTEM = 'VENTASYS'
        AND TRANSACTION_FLAG = 'Y'
        AND CALL_SOURCE_ID IS NOT NULL
        AND CALL_TYPE = 'Phone'
    GROUP BY 1,
        2,
        3,
        4
),
VIDEO AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        COUNT(DISTINCT CALL_SOURCE_ID) AS NOOFVIDEO
    FROM EDW_HCP360_KPI_RPT
    WHERE SOURCE_SYSTEM = 'VENTASYS'
        AND CALL_TYPE = 'Video'
        AND TRANSACTION_FLAG = 'Y'
        AND CALL_SOURCE_ID IS NOT NULL
    GROUP BY 1,
        2,
        3,
        4
),
FACETOFACE AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        COUNT(DISTINCT CALL_SOURCE_ID) AS NOOFF2F
    FROM EDW_HCP360_KPI_RPT
    WHERE SOURCE_SYSTEM = 'VENTASYS'
        AND CALL_TYPE LIKE 'Face to Face%'
        AND TRANSACTION_FLAG = 'Y'
        AND CALL_SOURCE_ID IS NOT NULL
    GROUP BY 1,
        2,
        3,
        4
),
AVG_PROD_DETAILED AS (
    SELECT BRAND,
        YEAR,
        MONTH,
        HCP_ID,
        AVG(CAST(PIN AS NUMERIC(10, 4))) AS AVGPRODDETAIL
    FROM (
            SELECT BRAND,
                TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
                TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
                HCP_ID,
                CALL_SOURCE_ID,
                COUNT(DISTINCT PRODUCT_INDICATION_NAME) AS PIN
            FROM EDW_HCP360_KPI_RPT
            WHERE SOURCE_SYSTEM = 'VENTASYS'
                AND CALL_SOURCE_ID IS NOT NULL
                AND PRODUCT_INDICATION_NAME IS NOT NULL
            GROUP BY 1,
                2,
                3,
                4,
                5
        )
    GROUP BY 1,
        2,
        3,
        4
),
EVENTREGISTERED AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        HCP_MASTER_ID,
        COUNT(DISTINCT MEDICAL_EVENT_ID) AS EVENT_REG
    FROM EDW_HCP360_KPI_RPT
    WHERE SOURCE_SYSTEM = 'VEEVA'
    GROUP BY 1,
        2,
        3,
        4,
        5
),
EVENTATTENDED AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        HCP_MASTER_ID,
        COUNT(DISTINCT MEDICAL_EVENT_ID) AS EVENT_ATEND
    FROM EDW_HCP360_KPI_RPT
    WHERE SOURCE_SYSTEM = 'VEEVA'
        AND ATTENDEE_STATUS = 'Attended'
    GROUP BY 1,
        2,
        3,
        4,
        5
),
ASSPEAKER AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        HCP_MASTER_ID,
        COUNT(DISTINCT MEDICAL_EVENT_ID) AS SPEAKER
    FROM EDW_HCP360_KPI_RPT
    WHERE SOURCE_SYSTEM = 'VEEVA'
        AND ATTENDEE_STATUS = 'Attended'
        AND EVENT_ROLE = 'Speaker'
    GROUP BY 1,
        2,
        3,
        4,
        5
),
AVG_KEY_MSG AS (
    SELECT BRAND,
        YEAR,
        MONTH,
        HCP_ID,
        HCP_MASTER_ID,
        AVG(CAST(KM AS NUMERIC(10, 4))) AS AVG_KEY_MSG
    FROM (
            SELECT BRAND,
                TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
                TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
                HCP_ID,
                HCP_MASTER_ID,
                CALL_SOURCE_ID,
                COUNT(DISTINCT KEY_MESSAGE) AS KM
            FROM EDW_HCP360_KPI_RPT
            WHERE SOURCE_SYSTEM = 'VEEVA'
                AND CALL_SOURCE_ID IS NOT NULL
            GROUP BY 1,
                2,
                3,
                4,
                5,
                6
        )
    GROUP BY 1,
        2,
        3,
        4,
        5
),
NOOFEMAILSENT AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        HCP_MASTER_ID,
        COUNT(EMAIL_NAME) AS EMAILSENT
    FROM EDW_HCP360_KPI_RPT
    WHERE SOURCE_SYSTEM = 'SFMC'
        AND EMAIL_ACTIVITY_TYPE = 'SENT'
    GROUP BY 1,
        2,
        3,
        4,
        5
),
NOOFEMAILDELIVERED AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        HCP_MASTER_ID,
        COUNT(EMAIL_NAME) AS EMAILSDELIVERED
    FROM EDW_HCP360_KPI_RPT
    WHERE SOURCE_SYSTEM = 'SFMC'
        AND EMAIL_ACTIVITY_TYPE = 'SENT'
        AND EMAIL_DELIVERED_FLAG = 'Y'
    GROUP BY 1,
        2,
        3,
        4,
        5
),
NOOFEMAILOPENED AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        HCP_MASTER_ID,
        COUNT(EMAIL_NAME) AS EMAILSOPENED
    FROM EDW_HCP360_KPI_RPT
    WHERE SOURCE_SYSTEM = 'SFMC'
        AND EMAIL_ACTIVITY_TYPE = 'OPEN'
        AND IS_UNIQUE = 'Y'
    GROUP BY 1,
        2,
        3,
        4,
        5
),
NOOFEMAILCLICKED AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        HCP_MASTER_ID,
        COUNT(EMAIL_NAME) AS EMAILSCLICKED
    FROM EDW_HCP360_KPI_RPT
    WHERE SOURCE_SYSTEM = 'SFMC'
        AND EMAIL_ACTIVITY_TYPE = 'CLICK'
    GROUP BY 1,
        2,
        3,
        4,
        5
),
NOOFEMAILCLICKUNIQ AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        HCP_MASTER_ID,
        COUNT(EMAIL_NAME) AS EMAILSUNIQCLICK
    FROM EDW_HCP360_KPI_RPT
    WHERE SOURCE_SYSTEM = 'SFMC'
        AND EMAIL_ACTIVITY_TYPE = 'CLICK'
        AND IS_UNIQUE = 'Y'
    GROUP BY 1,
        2,
        3,
        4,
        5
),
NOOFEMAILUNSUBS AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        HCP_MASTER_ID,
        COUNT(EMAIL_NAME) AS EMAILSUNSUBSCRIBED
    FROM EDW_HCP360_KPI_RPT
    WHERE SOURCE_SYSTEM = 'SFMC'
        AND EMAIL_ACTIVITY_TYPE = 'UNSUBSCRIBE'
    GROUP BY 1,
        2,
        3,
        4,
        5
),
NOOFEMAILFORWARD AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        HCP_MASTER_ID,
        COUNT(EMAIL_NAME) AS EMAILSFORWARDED
    FROM EDW_HCP360_KPI_RPT
    WHERE SOURCE_SYSTEM = 'SFMC'
        AND EMAIL_ACTIVITY_TYPE = 'FORWARD'
    GROUP BY 1,
        2,
        3,
        4,
        5
),
touchpoint_ventasys as (
    SELECT 
        B.BRAND as BRAND,
        null as VENTASYS_ID,
        null as VENTASYS_NAME,
        null as VENTASYS_MOBILE,
        null as VENTASYS_EMAIL,
        null as VEEVA_ID,
        null as VEEVA_NAME,
        null as VEEVA_MOBILE,
        null as VEEVA_EMAIL,
        null as SFMC_ID,
        null as SFMC_NAME,
        null as SFMC_MOBILE,
        null as SFMC_EMAIL,
        null as MASTER_HCP_KEY,
        'Ventasys' as SOURCE_SYSTEM,
        B.HCP_ID as HCP_ID,
        null as  CUSTOMER_NAME,
        null as  CELL_PHONE,
        null as  EMAIL,
        null as  ACCOUNT_SOURCE_ID,
        null as  VENTASYS_TEAM_NAME,
        null as  VENTASYS_CUSTID,
        null as  SUBSCRIBER_KEY,
        'touchpoint' as DATA_SOURCE,
        'IN' as COUNTRY_CODE,
        B.REGION as REGION,
        B.ZONE as ZONE,
        B.TERRITORY as AREA,
        B.CLASSIFICATION as CLASSIFICATION,
        B.SPECIALITY as SPECIALITY,
        B.CORE_NONCORE as CORE_NONCORE,
        B.YEAR as YEAR,
        B.MONTH as MONTH,
        B.HCP_CREATED_DATE as HCP_CREATED_DATE,
        B.TERRITORY_ID as TERRITORY_ID,
        B.REGION_HQ as REGION_HQ,
        'Y' as IS_ACTIVE,
        B.FIELD_REP_ACTIVE as IS_ACTIVE_MSR,
        B.FIRST_PRESCRIPTION_DATE as FIRST_PRESCRIPTION_DATE,
        CASE
            WHEN C.TOTAL_PRESCRIPTIONS >= 1 THEN 'Prescriber'
            ELSE 'Non-Prescriber'
        END as PRESCRIBER_TYPE,
        C.TOTAL_PRESCRIPTIONS as TOTAL_PRESCRIPTIONS,
        C.PRESCRIPTION_UNITS as PRESCRIPTION_UNITS,
        U.UNIQPROD as UNIQUE_PRODUCT_TYPE,
        B.PLANNED_VISITS as PLANNED_VISITS,
        AV.ACTUAL_VISITS as ACTUAL_VISITS,
        P.PHONES as PHONE_CONNECTS,
        V.NOOFVIDEO as VIDEO_CONNECTS,
        F.NOOFF2F as F2F_CONNECTS,
        A.AVGPRODDETAIL as AVG_PROD_DETAILED,
        L.LABLES as LBL_GIVEN,
        S.SAMPLES_GIVEN as SAMPLES_GIVEN,
        ES.EMAILSENT as EMAILS_SENT,
        D.EMAILSDELIVERED as EMAILS_DELIVERED,
        O.EMAILSOPENED as EMAILS_OPENED,
        EC.EMAILSCLICKED as EMAILS_CLICKED,
        UC.EMAILSUNIQCLICK as EMAILS_UNIQUE_CLICKED,
        EU.EMAILSUNSUBSCRIBED as EMAILS_UNSUSBSCRIBED,
        EF.EMAILSFORWARDED as EMAILS_FORWARDED,
        C.AVERAGE_CALL_DURATION as AVERAGE_CALL_DURATION,
        ER.EVENT_REG as EVENTS_REGISTERED,
        EA.EVENT_ATEND as EVENTS_ATTENDED,
        SP.SPEAKER as EVENTS_AS_SPEAKER,
        KM.AVG_KEY_MSG as AVG_KEY_MSGS_DELIVERED,
        B.HCP_MASTER_ID as MASTER_ID,
        null as ACTIVITY_DATE,
        null as PRESCRIPTION_ID
    FROM BASE_union_2 B
        LEFT JOIN CALC_1 C ON B.BRAND = C.BRAND
        AND B.HCP_ID = C.HCP_ID
        AND B.YEAR = C.YEAR
        AND B.MONTH = C.MONTH
        LEFT JOIN UNIQPRODTYPE U ON B.BRAND = U.BRAND
        AND B.HCP_ID = U.HCP_ID
        AND B.YEAR = U.YEAR
        AND B.MONTH = U.MONTH
        LEFT JOIN ACTUAL_VISITS AV ON B.BRAND = AV.BRAND
        AND B.HCP_ID = AV.HCP_ID
        AND B.YEAR = AV.YEAR
        AND B.MONTH = AV.MONTH
        LEFT JOIN PHONE P ON B.BRAND = P.BRAND
        AND B.HCP_ID = P.HCP_ID
        AND B.YEAR = P.YEAR
        AND B.MONTH = P.MONTH
        LEFT JOIN AVG_PROD_DETAILED A ON B.BRAND = A.BRAND
        AND B.HCP_ID = A.HCP_ID
        AND B.YEAR = A.YEAR
        AND B.MONTH = A.MONTH
        LEFT JOIN VIDEO V ON B.BRAND = V.BRAND
        AND B.HCP_ID = V.HCP_ID
        AND B.YEAR = V.YEAR
        AND B.MONTH = V.MONTH
        LEFT JOIN FACETOFACE F ON B.BRAND = F.BRAND
        AND B.HCP_ID = F.HCP_ID
        AND B.YEAR = F.YEAR
        AND B.MONTH = F.MONTH
        LEFT JOIN SAMPLE1 S ON B.BRAND = CASE
            WHEN S.TEAM_BRAND_NAME = 'JB' THEN 'JBABY'
            WHEN S.TEAM_BRAND_NAME = 'ORSL' THEN 'ORSL'
            WHEN S.TEAM_BRAND_NAME = 'DERMA' THEN 'DERMA'
        END
        AND B.HCP_ID = S.HCP_ID
        AND B.YEAR = S.YEAR
        AND B.MONTH = S.MONTH
        LEFT JOIN LABLE L ON B.BRAND = CASE
            WHEN L.TEAM_BRAND_NAME = 'JB' THEN 'JBABY'
            WHEN L.TEAM_BRAND_NAME = 'ORSL' THEN 'ORSL'
            WHEN L.TEAM_BRAND_NAME = 'DERMA' THEN 'DERMA'
        END
        AND B.HCP_ID = L.HCP_ID
        AND B.YEAR = L.YEAR
        AND B.MONTH = L.MONTH
        LEFT JOIN AVG_KEY_MSG KM ON B.BRAND = KM.BRAND
        AND B.HCP_MASTER_ID = KM.HCP_MASTER_ID
        AND B.YEAR = KM.YEAR
        AND B.MONTH = KM.MONTH
        LEFT JOIN EVENTREGISTERED ER ON B.BRAND = ER.BRAND
        AND B.HCP_MASTER_ID = ER.HCP_MASTER_ID
        AND B.YEAR = ER.YEAR
        AND B.MONTH = ER.MONTH
        LEFT JOIN EVENTATTENDED EA ON B.BRAND = EA.BRAND
        AND B.HCP_MASTER_ID = EA.HCP_MASTER_ID
        AND B.YEAR = EA.YEAR
        AND B.MONTH = EA.MONTH
        LEFT JOIN ASSPEAKER SP ON B.BRAND = SP.BRAND
        AND B.HCP_MASTER_ID = SP.HCP_MASTER_ID
        AND B.YEAR = SP.YEAR
        AND B.MONTH = SP.MONTH
        LEFT JOIN NOOFEMAILSENT ES ON B.BRAND = ES.BRAND
        AND B.HCP_MASTER_ID = ES.HCP_MASTER_ID
        AND B.YEAR = ES.YEAR
        AND B.MONTH = ES.MONTH
        LEFT JOIN NOOFEMAILDELIVERED D ON B.BRAND = D.BRAND
        AND B.HCP_MASTER_ID = D.HCP_MASTER_ID
        AND B.YEAR = D.YEAR
        AND B.MONTH = D.MONTH
        LEFT JOIN NOOFEMAILOPENED O ON B.BRAND = O.BRAND
        AND B.HCP_MASTER_ID = O.HCP_MASTER_ID
        AND B.YEAR = O.YEAR
        AND B.MONTH = O.MONTH
        LEFT JOIN NOOFEMAILCLICKED EC ON B.BRAND = EC.BRAND
        AND B.HCP_MASTER_ID = EC.HCP_MASTER_ID
        AND B.YEAR = EC.YEAR
        AND B.MONTH = EC.MONTH
        LEFT JOIN NOOFEMAILCLICKUNIQ UC ON B.BRAND = UC.BRAND
        AND B.HCP_MASTER_ID = UC.HCP_MASTER_ID
        AND B.YEAR = UC.YEAR
        AND B.MONTH = UC.MONTH
        LEFT JOIN NOOFEMAILUNSUBS EU ON B.BRAND = EU.BRAND
        AND B.HCP_MASTER_ID = EU.HCP_MASTER_ID
        AND B.YEAR = EU.YEAR
        AND B.MONTH = EU.MONTH
        LEFT JOIN NOOFEMAILFORWARD EF ON B.BRAND = EF.BRAND
        AND B.HCP_MASTER_ID = EF.HCP_MASTER_ID
        AND B.YEAR = EF.YEAR
        AND B.MONTH = EF.MONTH
),
combined_union_1_2 as (
    select *
    from union_1
    union all
    select *
    from touchpoint_ventasys
),
BASE2 AS (
    SELECT DISTINCT kpi.BRAND,
        TO_CHAR(kpi.ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(kpi.ACTIVITY_DATE, 'MON') AS MONTH,
        kpi.HCP_ID,
        kpi.REGION,
        kpi.ZONE,
        kpi.TERRITORY,
        kpi.REGION_HQ,
        kpi.SPECIALITY,
        kpi.CORE_NONCORE,
        kpi.CLASSIFICATION,
        kpi.FIELD_REP_ACTIVE,
        kpi.PLANNED_CALL_CNT AS PLANNED_VISITS
    FROM EDW_HCP360_KPI_RPT kpi
        LEFT JOIN combined_union_1_2 download ON kpi.HCP_MASTER_ID = download.MASTER_ID
    WHERE kpi.SOURCE_SYSTEM = 'VEEVA'
        AND kpi.TRANSACTION_FLAG = 'Y'
        AND kpi.HCP_ID IS NOT NULL
        AND (
            download.MASTER_ID IS NULL
            OR kpi.HCP_MASTER_ID IS NULL
        )
),
ACTUALVISIT AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        COUNT(DISTINCT CALL_SOURCE_ID) AS ACTUAL_VISITS
    FROM EDW_HCP360_KPI_RPT
    WHERE SOURCE_SYSTEM = 'VEEVA'
        AND TRANSACTION_FLAG = 'Y'
        AND CALL_SOURCE_ID IS NOT NULL
    GROUP BY 1,
        2,
        3,
        4
),
CALC_2 AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        AVG(CALL_DURATION) AS AVERAGE_CALL_DURATION
    FROM EDW_HCP360_KPI_RPT
    WHERE SOURCE_SYSTEM = 'VEEVA'
        AND TRANSACTION_FLAG = 'Y'
    GROUP BY 1,
        2,
        3,
        4
),
EVENTREGISTERED AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        COUNT(DISTINCT MEDICAL_EVENT_ID) AS EVENT_REG
    FROM EDW_HCP360_KPI_RPT
    WHERE SOURCE_SYSTEM = 'VEEVA'
    GROUP BY 1,
        2,
        3,
        4
),
EVENTATTENDED AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        COUNT(DISTINCT MEDICAL_EVENT_ID) AS EVENT_ATEND
    FROM EDW_HCP360_KPI_RPT
    WHERE SOURCE_SYSTEM = 'VEEVA'
        AND ATTENDEE_STATUS = 'Attended'
    GROUP BY 1,
        2,
        3,
        4
),
ASSPEAKER AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        COUNT(DISTINCT MEDICAL_EVENT_ID) AS SPEAKER
    FROM EDW_HCP360_KPI_RPT
    WHERE SOURCE_SYSTEM = 'VEEVA'
        AND ATTENDEE_STATUS = 'Attended'
        AND EVENT_ROLE = 'Speaker'
    GROUP BY 1,
        2,
        3,
        4
),
AVG_KEY_MSG AS (
    SELECT BRAND,
        YEAR,
        MONTH,
        HCP_ID,
        AVG(CAST(KM AS NUMERIC(10, 4))) AS AVG_KEY_MSG
    FROM (
            SELECT BRAND,
                TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
                TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
                HCP_ID,
                CALL_SOURCE_ID,
                COUNT(DISTINCT KEY_MESSAGE) AS KM
            FROM EDW_HCP360_KPI_RPT
            WHERE SOURCE_SYSTEM = 'VEEVA'
                AND CALL_SOURCE_ID IS NOT NULL
            GROUP BY 1,
                2,
                3,
                4,
                5
        )
    GROUP BY 1,
        2,
        3,
        4
),
touchpoint_veeva as (
    SELECT 
        B.BRAND as BRAND,
        null as VENTASYS_ID,
        null as VENTASYS_NAME,
        null as VENTASYS_MOBILE,
        null as VENTASYS_EMAIL,
        null as VEEVA_ID,
        null as VEEVA_NAME,
        null as VEEVA_MOBILE,
        null as VEEVA_EMAIL,
        null as SFMC_ID,
        null as SFMC_NAME,
        null as SFMC_MOBILE,
        null as SFMC_EMAIL,
        null as MASTER_HCP_KEY,
        'Veeva' as SOURCE_SYSTEM,
        B.HCP_ID as HCP_ID,
        null as  CUSTOMER_NAME,
        null as  CELL_PHONE,
        null as  EMAIL,
        null as  ACCOUNT_SOURCE_ID,
        null as  VENTASYS_TEAM_NAME,
        null as  VENTASYS_CUSTID,
        null as  SUBSCRIBER_KEY,
        'touchpoint' as DATA_SOURCE,
        'IN' as COUNTRY_CODE,
        B.REGION as REGION,
        B.ZONE as ZONE,
        B.TERRITORY as AREA,
        B.CLASSIFICATION as CLASSIFICATION,
        B.SPECIALITY as SPECIALITY,
        B.CORE_NONCORE as CORE_NONCORE,
        B.YEAR as YEAR,
        B.MONTH as MONTH,
        NULL as HCP_CREATED_DATE,
        NULL as TERRITORY_ID,
        B.REGION_HQ as REGION_HQ,
        'Y' as IS_ACTIVE,
        B.FIELD_REP_ACTIVE as IS_ACTIVE_MSR,
        NULL as FIRST_PRESCRIPTION_DATE,
        'Non-Prescriber' AS PRESCRIBER_TYPE,
        NULL as TOTAL_PRESCRIPTIONS,
        NULL as PRESCRIPTION_UNITS,
        NULL as UNIQUE_PRODUCT_TYPE,
        B.PLANNED_VISITS as PLANNED_VISITS,
        AV.ACTUAL_VISITS as ACTUAL_VISITS,
        NULL as PHONE_CONNECTS,
        NULL as VIDEO_CONNECTS,
        NULL as F2F_CONNECTS,
        NULL as AVG_PROD_DETAILED,
        NULL as LBL_GIVEN,
        NULL as SAMPLES_GIVEN,
        NULL as EMAILS_SENT,
        NULL as EMAILS_DELIVERED,
        NULL as EMAILS_OPENED,
        NULL as EMAILS_CLICKED,
        NULL as EMAILS_UNIQUE_CLICKED,
        NULL as EMAILS_UNSUSBSCRIBED,
        NULL as EMAILS_FORWARDED,
        C.AVERAGE_CALL_DURATION as AVERAGE_CALL_DURATION,
        ER.EVENT_REG as EVENTS_REGISTERED,
        EA.EVENT_ATEND as EVENTS_ATTENDED,
        S.SPEAKER as EVENTS_AS_SPEAKER,
        KM.AVG_KEY_MSG as AVG_KEY_MSGS_DELIVERED,
        null as MASTER_ID,
        null as ACTIVITY_DATE,
        null as PRESCRIPTION_ID
    FROM BASE2 B
        LEFT JOIN CALC_2 C ON B.HCP_ID = C.HCP_ID
        AND B.YEAR = C.YEAR
        AND B.MONTH = C.MONTH
        LEFT JOIN ACTUALVISIT AV ON B.HCP_ID = AV.HCP_ID
        AND B.YEAR = AV.YEAR
        AND B.MONTH = AV.MONTH
        LEFT JOIN EVENTREGISTERED ER ON B.HCP_ID = ER.HCP_ID
        AND B.YEAR = ER.YEAR
        AND B.MONTH = ER.MONTH
        LEFT JOIN EVENTATTENDED EA ON B.HCP_ID = EA.HCP_ID
        AND B.YEAR = EA.YEAR
        AND B.MONTH = EA.MONTH
        LEFT JOIN ASSPEAKER S ON B.HCP_ID = S.HCP_ID
        AND B.YEAR = S.YEAR
        AND B.MONTH = S.MONTH
        LEFT JOIN AVG_KEY_MSG KM ON B.HCP_ID = KM.HCP_ID
        AND B.YEAR = KM.YEAR
        AND B.MONTH = KM.MONTH
),
combined_union_1_2_3 as (
    select *
    from combined_union_1_2
    union all
    select *
    from touchpoint_veeva
),
BASE1 AS (
        SELECT
                DISTINCT KPI.BRAND,
                TO_CHAR(KPI.ACTIVITY_DATE, 'YYYY') AS YEAR,
                TO_CHAR(KPI.ACTIVITY_DATE, 'MON') AS MONTH,
                KPI.HCP_ID,
                KPI.HCP_CREATED_DATE,
                KPI.REGION,
                KPI.ZONE,
                KPI.TERRITORY,
                KPI.REGION_HQ,
                KPI.SPECIALITY,
                KPI.CORE_NONCORE,
                KPI.CLASSIFICATION
         FROM EDW_HCP360_KPI_RPT kpi
    LEFT JOIN {{this}} download
         ON kpi.HCP_MASTER_ID = download.MASTER_ID
    WHERE kpi.SOURCE_SYSTEM = 'SFMC'
                AND kpi.HCP_ID IS NOT NULL
                AND (download.MASTER_ID IS NULL OR kpi.HCP_MASTER_ID IS NULL)
),
NOOFEMAILSENT AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        COUNT(EMAIL_NAME) AS EMAILSENT
    FROM EDW_HCP360_KPI_RPT
    WHERE EMAIL_ACTIVITY_TYPE = 'SENT'
    GROUP BY 1,
        2,
        3,
        4
),
NOOFEMAILDELIVERED AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        COUNT(EMAIL_NAME) AS EMAILSDELIVERED
    FROM EDW_HCP360_KPI_RPT
    WHERE EMAIL_ACTIVITY_TYPE = 'SENT'
        AND EMAIL_DELIVERED_FLAG = 'Y'
    GROUP BY 1,
        2,
        3,
        4
),
NOOFEMAILOPENED AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        COUNT(EMAIL_NAME) AS EMAILSOPENED
    FROM EDW_HCP360_KPI_RPT
    WHERE EMAIL_ACTIVITY_TYPE = 'OPEN'
        AND IS_UNIQUE = 'Y'
    GROUP BY 1,
        2,
        3,
        4
),
NOOFEMAILCLICKED AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        COUNT(EMAIL_NAME) AS EMAILSCLICKED
    FROM EDW_HCP360_KPI_RPT
    WHERE EMAIL_ACTIVITY_TYPE = 'CLICK'
    GROUP BY 1,
        2,
        3,
        4
),
NOOFEMAILCLICKUNIQ AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        COUNT(EMAIL_NAME) AS EMAILSUNIQCLICK
    FROM EDW_HCP360_KPI_RPT
    WHERE EMAIL_ACTIVITY_TYPE = 'CLICK'
        AND IS_UNIQUE = 'Y'
    GROUP BY 1,
        2,
        3,
        4
),
NOOFEMAILUNSUBS AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        COUNT(EMAIL_NAME) AS EMAILSUNSUBSCRIBED
    FROM EDW_HCP360_KPI_RPT
    WHERE EMAIL_ACTIVITY_TYPE = 'UNSUBSCRIBE'
    GROUP BY 1,
        2,
        3,
        4
),
NOOFEMAILFORWARD AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        COUNT(EMAIL_NAME) AS EMAILSFORWARDED
    FROM EDW_HCP360_KPI_RPT
    WHERE EMAIL_ACTIVITY_TYPE = 'FORWARD'
    GROUP BY 1,
        2,
        3,
        4
),
touchpoint_sfmc as (
    SELECT distinct
        B.BRAND as BRAND,
        null as VENTASYS_ID,
        null as VENTASYS_NAME,
        null as VENTASYS_MOBILE,
        null as VENTASYS_EMAIL,
        null as VEEVA_ID,
        null as VEEVA_NAME,
        null as VEEVA_MOBILE,
        null as VEEVA_EMAIL,
        null as SFMC_ID,
        null as SFMC_NAME,
        null as SFMC_MOBILE,
        null as SFMC_EMAIL,
        null as MASTER_HCP_KEY,
        'SFMC' as SOURCE_SYSTEM,
        B.HCP_ID as HCP_ID,
        null as  CUSTOMER_NAME,
        null as  CELL_PHONE,
        null as  EMAIL,
        null as  ACCOUNT_SOURCE_ID,
        null as  VENTASYS_TEAM_NAME,
        null as  VENTASYS_CUSTID,
        null as  SUBSCRIBER_KEY,
        'touchpoint' as DATA_SOURCE,
        'IN' as COUNTRY_CODE,
        B.REGION as REGION,
        B.ZONE as ZONE,
        B.TERRITORY as AREA,
        B.CLASSIFICATION as CLASSIFICATION,
        B.SPECIALITY as SPECIALITY,
        B.CORE_NONCORE as CORE_NONCORE,
        B.YEAR as YEAR,
        B.MONTH as MONTH,
        B.HCP_CREATED_DATE as HCP_CREATED_DATE,
        NULL as TERRITORY_ID,
        B.REGION_HQ as REGION_HQ,
        'Y'as IS_ACTIVE,
        NULL as IS_ACTIVE_MSR,
        NULL::DATE as FIRST_PRESCRIPTION_DATE,
        'Non-Prescriber'::VARCHAR(14) as PRESCRIBER_TYPE,
        NULL as TOTAL_PRESCRIPTIONS,
        NULL as PRESCRIPTION_UNITS,
        NULL as UNIQUE_PRODUCT_TYPE,
        NULL as PLANNED_VISITS,
        NULL as ACTUAL_VISITS,
        NULL as PHONE_CONNECTS,
        NULL as VIDEO_CONNECTS,
        NULL as F2F_CONNECTS,
        NULL as AVG_PROD_DETAILED,
        NULL as LBL_GIVEN,
        NULL as SAMPLES_GIVEN,
        S.EMAILSENT as EMAILS_SENT,
        D.EMAILSDELIVERED as EMAILS_DELIVERED,
        O.EMAILSOPENED as EMAILS_OPENED,
        C.EMAILSCLICKED as EMAILS_CLICKED,
        UC.EMAILSUNIQCLICK as EMAILS_UNIQUE_CLICKED,
        U.EMAILSUNSUBSCRIBED as EMAILS_UNSUSBSCRIBED,
        F.EMAILSFORWARDED as EMAILS_FORWARDED,
        NULL as AVERAGE_CALL_DURATION,
        null as EVENTS_REGISTERED,
        null as EVENTS_ATTENDED,
        null as EVENTS_AS_SPEAKER,
        null as AVG_KEY_MSGS_DELIVERED,
        null as MASTER_ID,
        null as ACTIVITY_DATE,
        null as PRESCRIPTION_ID
    FROM BASE1 B
        LEFT JOIN NOOFEMAILSENT S ON B.HCP_ID = S.HCP_ID
        AND B.YEAR = S.YEAR
        AND B.MONTH = S.MONTH
        LEFT JOIN NOOFEMAILDELIVERED D ON B.HCP_ID = D.HCP_ID
        AND B.YEAR = D.YEAR
        AND B.MONTH = D.MONTH
        LEFT JOIN NOOFEMAILOPENED O ON B.HCP_ID = O.HCP_ID
        AND B.YEAR = O.YEAR
        AND B.MONTH = O.MONTH
        LEFT JOIN NOOFEMAILCLICKED C ON B.HCP_ID = C.HCP_ID
        AND B.YEAR = C.YEAR
        AND B.MONTH = C.MONTH
        LEFT JOIN NOOFEMAILCLICKUNIQ UC ON B.HCP_ID = UC.HCP_ID
        AND B.YEAR = UC.YEAR
        AND B.MONTH = UC.MONTH
        LEFT JOIN NOOFEMAILUNSUBS U ON B.HCP_ID = U.HCP_ID
        AND B.YEAR = U.YEAR
        AND B.MONTH = U.MONTH
        LEFT JOIN NOOFEMAILFORWARD F ON B.HCP_ID = F.HCP_ID
        AND B.YEAR = F.YEAR
        AND B.MONTH = F.MONTH
),
combined_union_1_2_3_4 as (
    select *
    from combined_union_1_2_3
    union all
    select *
    from touchpoint_sfmc
),
BASE AS (
    SELECT DISTINCT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        HCP_MASTER_ID,
        HCP_CREATED_DATE,
        TERRITORY_ID,
        REGION,
        ZONE,
        TERRITORY,
        REGION_HQ,
        SPECIALITY,
        CORE_NONCORE,
        CLASSIFICATION,
        FIELD_REP_ACTIVE,
        FIRST_PRESCRIPTION_DATE
    FROM EDW_HCP360_KPI_RPT
    WHERE SOURCE_SYSTEM = 'VENTASYS'
        AND TRANSACTION_FLAG = 'N'
        AND HCP_ID IS NOT NULL
),
CALC_3 AS (
    SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE, 'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE, 'MON') AS MONTH,
        HCP_ID,
        ACTIVITY_DATE,
        PRESCRIPTION_ID,
        NO_OF_PRESCRIPTION_UNITS
    FROM EDW_HCP360_KPI_RPT
    WHERE SOURCE_SYSTEM = 'VENTASYS'
        AND TRANSACTION_FLAG = 'Y'
        AND PRESCRIPTION_ID IS NOT NULL
),
lapsed_hcp as (
    SELECT 
        B.BRAND as BRAND,
        null as VENTASYS_ID,
        null as VENTASYS_NAME,
        null as VENTASYS_MOBILE,
        null as VENTASYS_EMAIL,
        null as VEEVA_ID,
        null as VEEVA_NAME,
        null as VEEVA_MOBILE,
        null as VEEVA_EMAIL,
        null as SFMC_ID,
        null as SFMC_NAME,
        null as SFMC_MOBILE,
        null as SFMC_EMAIL,
        B.HCP_MASTER_ID as MASTER_HCP_KEY,
        'Ventasys' as SOURCE_SYSTEM,
        B.HCP_ID as HCP_ID,
        null CUSTOMER_NAME,
        null CELL_PHONE,
        null EMAIL,
        null ACCOUNT_SOURCE_ID,
        null VENTASYS_TEAM_NAME,
        null VENTASYS_CUSTID,
        null SUBSCRIBER_KEY,
        'Lapsed HCP' as DATA_SOURCE,
        'IN' as COUNTRY_CODE,
        B.REGION as REGION,
        ZONE as ZONE,
        B.TERRITORY as AREA,
        B.CLASSIFICATION as CLASSIFICATION,
        B.SPECIALITY as SPECIALITY,
        B.CORE_NONCORE as CORE_NONCORE,
        C.YEAR as YEAR,
        C.MONTH as MONTH,
        B.HCP_CREATED_DATE as HCP_CREATED_DATE,
        B.TERRITORY_ID as TERRITORY_ID,
        B.REGION_HQ as REGION_HQ,
        'Y' as IS_ACTIVE,
        B.FIELD_REP_ACTIVE as IS_ACTIVE_MSR,
        B.HCP_CREATED_DATE as FIRST_PRESCRIPTION_DATE,
        NULL as PRESCRIBER_TYPE,
        NULL as TOTAL_PRESCRIPTIONS,
        C.NO_OF_PRESCRIPTION_UNITS as PRESCRIPTION_UNITS,
        NULL as UNIQUE_PRODUCT_TYPE,
        NULL as PLANNED_VISITS,
        NULL as ACTUAL_VISITS,
        NULL as PHONE_CONNECTS,
        NULL as VIDEO_CONNECTS,
        NULL as F2F_CONNECTS,
        NULL as AVG_PROD_DETAILED,
        NULL as LBL_GIVEN,
        NULL as SAMPLES_GIVEN,
        null as EMAILS_SENT,
        null as EMAILS_DELIVERED,
        null as EMAILS_OPENED,
        null as EMAILS_CLICKED,
        null as EMAILS_UNIQUE_CLICKED,
        null as EMAILS_UNSUSBSCRIBED,
        null as EMAILS_FORWARDED,
        NULL as AVERAGE_CALL_DURATION,
        null as EVENTS_REGISTERED,
        null as EVENTS_ATTENDED,
        null as EVENTS_AS_SPEAKER,
        null as AVG_KEY_MSGS_DELIVERED,
        null as MASTER_ID,
        C.ACTIVITY_DATE as ACTIVITY_DATE,
        C.PRESCRIPTION_ID as PRESCRIPTION_ID
    FROM CALC_3 C
        LEFT JOIN BASE B ON C.BRAND = B.BRAND
        AND C.HCP_ID = B.HCP_ID
        AND C.YEAR = B.YEAR
        AND C.MONTH = B.MONTH
),
combined_union_1_2_3_4_5 as (
    select *
    from combined_union_1_2_3_4
    union all
    select *
    from lapsed_hcp
),
final as (
    select BRAND::VARCHAR(20) as BRAND,
        VENTASYS_ID::VARCHAR(20) as VENTASYS_ID,
        VENTASYS_NAME::VARCHAR(50) as VENTASYS_NAME,
        VENTASYS_MOBILE::VARCHAR(50) as VENTASYS_MOBILE,
        VENTASYS_EMAIL::VARCHAR(100) as VENTASYS_EMAIL,
        VEEVA_ID::VARCHAR(20) as VEEVA_ID,
        VEEVA_NAME::VARCHAR(255) as VEEVA_NAME,
        VEEVA_MOBILE::VARCHAR(40) as VEEVA_MOBILE,
        VEEVA_EMAIL::VARCHAR(255) as VEEVA_EMAIL,
        SFMC_ID::VARCHAR(50) as SFMC_ID,
        SFMC_NAME::VARCHAR(100) as SFMC_NAME,
        SFMC_MOBILE::VARCHAR(20) as SFMC_MOBILE,
        SFMC_EMAIL::VARCHAR(50) as SFMC_EMAIL,
        MASTER_HCP_KEY::VARCHAR(50) as MASTER_HCP_KEY,
        SOURCE_SYSTEM::VARCHAR(8) as SOURCE_SYSTEM,
        HCP_ID::VARCHAR(100) as HCP_ID,
        CUSTOMER_NAME::VARCHAR(255) as CUSTOMER_NAME,
        CELL_PHONE::VARCHAR(50) as CELL_PHONE,
        EMAIL::VARCHAR(255) as EMAIL,
        ACCOUNT_SOURCE_ID::VARCHAR(255) as ACCOUNT_SOURCE_ID,
        VENTASYS_TEAM_NAME::VARCHAR(20) as VENTASYS_TEAM_NAME,
        VENTASYS_CUSTID::VARCHAR(20) as VENTASYS_CUSTID,
        SUBSCRIBER_KEY::VARCHAR(50) as SUBSCRIBER_KEY,
        DATA_SOURCE::VARCHAR(100) as DATA_SOURCE,
        COUNTRY_CODE::VARCHAR(20) as COUNTRY_CODE,
        REGION::VARCHAR(100) as REGION,
        ZONE::VARCHAR(50) as ZONE,
        AREA::VARCHAR(50) as AREA,
        CLASSIFICATION::VARCHAR(30) as CLASSIFICATION,
        SPECIALITY::VARCHAR(30) as SPECIALITY,
        CORE_NONCORE::VARCHAR(20) as CORE_NONCORE,
        YEAR::VARCHAR(12) as YEAR,
        MONTH::VARCHAR(11) as MONTH,
        HCP_CREATED_DATE::DATE as HCP_CREATED_DATE,
        TERRITORY_ID::VARCHAR(20) as TERRITORY_ID,
        REGION_HQ::VARCHAR(50) as REGION_HQ,
        IS_ACTIVE::VARCHAR(1) as IS_ACTIVE,
        IS_ACTIVE_MSR::VARCHAR(1) as IS_ACTIVE_MSR,
        FIRST_PRESCRIPTION_DATE::DATE as FIRST_PRESCRIPTION_DATE,
        PRESCRIBER_TYPE::VARCHAR(14) as PRESCRIBER_TYPE,
        TOTAL_PRESCRIPTIONS::NUMBER(38, 5) as TOTAL_PRESCRIPTIONS,
        PRESCRIPTION_UNITS::NUMBER(38, 2) as PRESCRIPTION_UNITS,
        UNIQUE_PRODUCT_TYPE::NUMBER(18, 0) as UNIQUE_PRODUCT_TYPE,
        PLANNED_VISITS::NUMBER(38, 0) as PLANNED_VISITS,
        ACTUAL_VISITS::NUMBER(18, 0) as ACTUAL_VISITS,
        PHONE_CONNECTS::NUMBER(18, 0) as PHONE_CONNECTS,
        VIDEO_CONNECTS::NUMBER(18, 0) as VIDEO_CONNECTS,
        F2F_CONNECTS::NUMBER(18, 0) as F2F_CONNECTS,
        AVG_PROD_DETAILED::NUMBER(38, 4) as AVG_PROD_DETAILED,
        LBL_GIVEN::NUMBER(18, 0) as LBL_GIVEN,
        SAMPLES_GIVEN::NUMBER(18, 0) as SAMPLES_GIVEN,
        EMAILS_SENT::NUMBER(18, 0) as EMAILS_SENT,
        EMAILS_DELIVERED::NUMBER(18, 0) as EMAILS_DELIVERED,
        EMAILS_OPENED::NUMBER(18, 0) as EMAILS_OPENED,
        EMAILS_CLICKED::NUMBER(18, 0) as EMAILS_CLICKED,
        EMAILS_UNIQUE_CLICKED::NUMBER(18, 0) as EMAILS_UNIQUE_CLICKED,
        EMAILS_UNSUSBSCRIBED::NUMBER(18, 0) as EMAILS_UNSUSBSCRIBED,
        EMAILS_FORWARDED::NUMBER(18, 0) as EMAILS_FORWARDED,
        AVERAGE_CALL_DURATION::NUMBER(38, 4) as AVERAGE_CALL_DURATION,
        EVENTS_REGISTERED::NUMBER(18, 0) as EVENTS_REGISTERED,
        EVENTS_ATTENDED::NUMBER(18, 0) as EVENTS_ATTENDED,
        EVENTS_AS_SPEAKER::NUMBER(18, 0) as EVENTS_AS_SPEAKER,
        AVG_KEY_MSGS_DELIVERED::NUMBER(38, 4) as AVG_KEY_MSGS_DELIVERED,
        MASTER_ID::VARCHAR(32) as MASTER_ID,
        ACTIVITY_DATE::DATE as ACTIVITY_DATE,
        PRESCRIPTION_ID::VARCHAR(50) as PRESCRIPTION_ID
    from combined_union_1_2_3_4_5
)
select * from final