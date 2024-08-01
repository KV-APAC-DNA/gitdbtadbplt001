with 
wks_tp_salescube_mat2 as 
(
    select * from {{ ref('hcpwks_integration__wks_tp_salescube_mat2') }}
),
wks_tp_salescube_ytd2 as 
(
    select * from {{ ref('hcpwks_integration__wks_tp_salescube_ytd2') }}
),
wks_tp_salescube_base as 
(
    select * from {{ ref('hcpwks_integration__wks_tp_salescube_base') }}
),
edw_hcp360_kpi_rpt as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_kpi_rpt') }}
),
edw_hcp360_in_ventasys_samples_fact as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_in_ventasys_samples_fact') }}
),
edw_hcp360_in_ventasys_hcp_dim_latest as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_in_ventasys_hcp_dim_latest') }}
),
wks_hcp360_projected_hcp_speciality_detail as 
(
    select * from {{ source('hcpwks_integration', 'wks_hcp360_projected_hcp_speciality_detail') }}
),
edw_hcp360_sfmc_hcp_dim as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_sfmc_hcp_dim') }}
),
itg_hcp360_sfmc_hcp_details as 
(
    select * from {{ ref('hcpitg_integration__itg_hcp360_sfmc_hcp_details') }}
), 
temp_ventasys as 
(
WITH BASE
AS
(SELECT DISTINCT BRAND,
       TO_CHAR(ACTIVITY_DATE,'YYYY') AS YEAR,
       TO_CHAR(ACTIVITY_DATE,'MON') AS MONTH,
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
       IS_ACTIVE,
       FIELD_REP_ACTIVE,
       FIRST_PRESCRIPTION_DATE,
       PLANNED_CALL_CNT AS PLANNED_VISITS
FROM EDW_HCP360_KPI_RPT 
WHERE SOURCE_SYSTEM = 'VENTASYS'
AND   TRANSACTION_FLAG = 'N' 
AND HCP_ID IS NOT NULL
),

SAMPLE1 AS
(SELECT TEAM_BRAND_NAME,
        TO_CHAR(SAMPLE_DATE,'YYYY') AS YEAR,
        TO_CHAR(SAMPLE_DATE,'MON') AS MONTH,
        HCP_ID,
        COUNT(DISTINCT SAMPLE_PRODUCT) AS SAMPLES_GIVEN,
        CASE WHEN TEAM_BRAND_NAME = 'JB' THEN 'JBABY' 
                           WHEN TEAM_BRAND_NAME = 'ORSL' THEN 'ORSL'
                           WHEN TEAM_BRAND_NAME = 'DERMA' THEN 'DERMA'
                           END as join_column2
  FROM EDW_HCP360_IN_VENTASYS_SAMPLES_FACT 
  WHERE SAMPLE_PRODUCT  LIKE '%Sample%' OR SAMPLE_PRODUCT  LIKE '%(S)%'
  GROUP BY 1,
           2,
           3,
           4),
           
LABLE AS
(SELECT TEAM_BRAND_NAME,
        TO_CHAR(SAMPLE_DATE,'YYYY') AS YEAR,
        TO_CHAR(SAMPLE_DATE,'MON') AS MONTH,
        HCP_ID,
        COUNT(DISTINCT SAMPLE_PRODUCT) AS LABLES,
        CASE WHEN
        TEAM_BRAND_NAME = 'JB' THEN 'JBABY' 
        WHEN TEAM_BRAND_NAME = 'ORSL' THEN 'ORSL'
         WHEN TEAM_BRAND_NAME = 'DERMA' THEN 'DERMA'
                           END as join_column
  FROM EDW_HCP360_IN_VENTASYS_SAMPLES_FACT 
  WHERE SAMPLE_PRODUCT NOT LIKE '%Sample%' AND SAMPLE_PRODUCT NOT LIKE '%(S)%'
  GROUP BY 1,
           2,
           3,
           4),
    
CALC
AS
(SELECT BRAND,
       TO_CHAR(ACTIVITY_DATE,'YYYY') AS YEAR,
       TO_CHAR(ACTIVITY_DATE,'MON') AS MONTH,
       HCP_ID,
       COUNT(DISTINCT PRESCRIPTION_ID) AS TOTAL_PRESCRIPTIONS,
       SUM(NVL(NO_OF_PRESCRIPTION_UNITS,0)) AS PRESCRIPTION_UNITS,
       AVG(CALL_DURATION) AS AVERAGE_CALL_DURATION
FROM (SELECT BRAND, ACTIVITY_DATE, HCP_ID, PRESCRIPTION_ID,
             (CASE WHEN PRESCRIPTION_ID IS NULL THEN 0 ELSE NO_OF_PRESCRIPTION_UNITS END) AS NO_OF_PRESCRIPTION_UNITS,
             CALL_DURATION
      FROM EDW_HCP360_KPI_RPT
      WHERE SOURCE_SYSTEM = 'VENTASYS' AND TRANSACTION_FLAG = 'Y')
GROUP BY 1,
         2,
         3,
         4),
         
UNIQPRODTYPE
AS
(SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE,'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE,'MON') AS MONTH,
        HCP_ID,
        COUNT(DISTINCT PRODUCT_INDICATION_NAME) AS UNIQPROD
FROM EDW_HCP360_KPI_RPT
WHERE SOURCE_SYSTEM = 'VENTASYS'
AND   PRESCRIPTION_ID IS NOT NULL  
GROUP BY 1,
         2,
         3,
         4),
         
ACTUAL_VISITS
AS
(SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE,'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE,'MON') AS MONTH,
        HCP_ID,
        COUNT(DISTINCT CALL_SOURCE_ID) AS ACTUAL_VISITS
        
FROM EDW_HCP360_KPI_RPT
WHERE SOURCE_SYSTEM = 'VENTASYS'
AND   TRANSACTION_FLAG = 'Y' AND CALL_SOURCE_ID IS NOT NULL
GROUP BY 1,
         2,
         3,
         4),
PHONE
AS
(SELECT BRAND,
       TO_CHAR(ACTIVITY_DATE,'YYYY') AS YEAR,
       TO_CHAR(ACTIVITY_DATE,'MON') AS MONTH,
       HCP_ID,
       COUNT(DISTINCT CALL_SOURCE_ID) AS PHONES
FROM EDW_HCP360_KPI_RPT
WHERE SOURCE_SYSTEM = 'VENTASYS' AND TRANSACTION_FLAG = 'Y' AND CALL_SOURCE_ID IS NOT NULL 
AND CALL_TYPE = 'Phone'
GROUP BY 1,
         2,
         3,
         4
         ),
VIDEO
AS
(SELECT BRAND,
       TO_CHAR(ACTIVITY_DATE,'YYYY') AS YEAR,
       TO_CHAR(ACTIVITY_DATE,'MON') AS MONTH,
       HCP_ID,
       COUNT(DISTINCT CALL_SOURCE_ID) AS NOOFVIDEO
FROM EDW_HCP360_KPI_RPT
WHERE SOURCE_SYSTEM = 'VENTASYS'  AND CALL_TYPE = 'Video' AND TRANSACTION_FLAG = 'Y' AND CALL_SOURCE_ID IS NOT NULL
GROUP BY 1,
         2,
         3,
         4),
FACETOFACE
AS
(SELECT BRAND,
       TO_CHAR(ACTIVITY_DATE,'YYYY') AS YEAR,
       TO_CHAR(ACTIVITY_DATE,'MON') AS MONTH,
       HCP_ID,
       COUNT(DISTINCT CALL_SOURCE_ID) AS NOOFF2F
FROM EDW_HCP360_KPI_RPT
WHERE (SOURCE_SYSTEM = 'VENTASYS'  AND CALL_TYPE LIKE 'Face to Face%' AND TRANSACTION_FLAG = 'Y' AND CALL_SOURCE_ID IS NOT NULL)
   OR (SOURCE_SYSTEM = 'VENTASYS'  AND CALL_TYPE IS NULL AND TRANSACTION_FLAG = 'Y' AND CALL_SOURCE_ID IS NOT NULL 
       AND TO_CHAR(ACTIVITY_DATE,'YYYY')= '2022' AND TO_CHAR(ACTIVITY_DATE,'MON') IN ('APR','MAY'))
GROUP BY 1,
         2,
         3,
         4),
AVG_PROD_DETAILED
AS
(SELECT BRAND,
       YEAR,
       MONTH,
       HCP_ID,
       AVG(CAST(PIN AS NUMERIC(10,4))) AS AVGPRODDETAIL
FROM (SELECT BRAND,
             TO_CHAR(ACTIVITY_DATE,'YYYY') AS YEAR,
             TO_CHAR(ACTIVITY_DATE,'MON') AS MONTH,
             HCP_ID,
             CALL_SOURCE_ID,
             COUNT(DISTINCT PRODUCT_INDICATION_NAME) AS PIN
      FROM EDW_HCP360_KPI_RPT
      WHERE SOURCE_SYSTEM = 'VENTASYS' AND CALL_SOURCE_ID IS NOT NULL 
      AND PRODUCT_INDICATION_NAME IS NOT NULL
      GROUP BY 1,
               2,
               3,
               4,
               5)
               


GROUP BY 1,
         2,
         3,
         4),
         
EVENTREGISTERED
AS
(SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE,'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE,'MON') AS MONTH,
        HCP_ID,
        HCP_MASTER_ID,
        COUNT(DISTINCT MEDICAL_EVENT_ID) AS EVENT_REG
      FROM EDW_HCP360_KPI_RPT
      WHERE SOURCE_SYSTEM = 'VEEVA' 
      GROUP BY 1,
               2,
               3,
               4,5),
EVENTATTENDED
AS
(SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE,'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE,'MON') AS MONTH,
        HCP_ID,
        HCP_MASTER_ID,
             COUNT(DISTINCT MEDICAL_EVENT_ID) AS EVENT_ATEND
      FROM EDW_HCP360_KPI_RPT
      WHERE SOURCE_SYSTEM = 'VEEVA' 
      
      AND ATTENDEE_STATUS = 'Attended'
      GROUP BY 1,
               2,
               3,
               4,5), 
               
ASSPEAKER
AS
(SELECT BRAND,
        TO_CHAR(ACTIVITY_DATE,'YYYY') AS YEAR,
        TO_CHAR(ACTIVITY_DATE,'MON') AS MONTH,
        HCP_ID,
        HCP_MASTER_ID,
        COUNT(DISTINCT MEDICAL_EVENT_ID) AS SPEAKER
      FROM EDW_HCP360_KPI_RPT
      WHERE SOURCE_SYSTEM = 'VEEVA' 
      
      AND ATTENDEE_STATUS = 'Attended' AND EVENT_ROLE = 'Speaker'
      GROUP BY 1,
               2,
               3,
               4,5),
         
AVG_KEY_MSG            
AS
(SELECT BRAND,
       YEAR,
       MONTH,
       HCP_ID,
       HCP_MASTER_ID,
       AVG(CAST(KM AS NUMERIC(10,4))) AS AVG_KEY_MSG
FROM (SELECT BRAND,
             TO_CHAR(ACTIVITY_DATE,'YYYY') AS YEAR,
             TO_CHAR(ACTIVITY_DATE,'MON') AS MONTH,
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
               5,6)
              
GROUP BY 1,
         2,
         3,
         4,5)  ,
         
NOOFEMAILSENT
AS
(SELECT BRAND,
       TO_CHAR(ACTIVITY_DATE,'YYYY') AS YEAR,
       TO_CHAR(ACTIVITY_DATE,'MON') AS MONTH,
       HCP_ID,
       HCP_MASTER_ID,
       COUNT(EMAIL_NAME) AS EMAILSENT
FROM EDW_HCP360_KPI_RPT
WHERE SOURCE_SYSTEM = 'SFMC' AND EMAIL_ACTIVITY_TYPE = 'SENT'
GROUP BY 1,
         2,
         3,
         4,
         5),


NOOFEMAILDELIVERED
AS
(SELECT BRAND,
       TO_CHAR(ACTIVITY_DATE,'YYYY') AS YEAR,
       TO_CHAR(ACTIVITY_DATE,'MON') AS MONTH,
       HCP_ID,
       HCP_MASTER_ID,
       COUNT(EMAIL_NAME) AS EMAILSDELIVERED
FROM EDW_HCP360_KPI_RPT
WHERE SOURCE_SYSTEM = 'SFMC' AND EMAIL_ACTIVITY_TYPE = 'SENT'
AND   EMAIL_DELIVERED_FLAG = 'Y'
GROUP BY 1,
         2,
         3,
         4,
         5),
NOOFEMAILOPENED
AS
(SELECT BRAND,
       TO_CHAR(ACTIVITY_DATE,'YYYY') AS YEAR,
       TO_CHAR(ACTIVITY_DATE,'MON') AS MONTH,
       HCP_ID,
       HCP_MASTER_ID,
       COUNT(EMAIL_NAME) AS EMAILSOPENED
FROM EDW_HCP360_KPI_RPT
WHERE SOURCE_SYSTEM = 'SFMC' AND EMAIL_ACTIVITY_TYPE = 'OPEN'
AND   IS_UNIQUE = 'Y'
GROUP BY 1,
         2,
         3,
         4,
         5),
NOOFEMAILCLICKED
AS
(SELECT BRAND,
       TO_CHAR(ACTIVITY_DATE,'YYYY') AS YEAR,
       TO_CHAR(ACTIVITY_DATE,'MON') AS MONTH,
       HCP_ID,
       HCP_MASTER_ID,
       COUNT(EMAIL_NAME) AS EMAILSCLICKED
FROM EDW_HCP360_KPI_RPT
WHERE SOURCE_SYSTEM = 'SFMC' AND EMAIL_ACTIVITY_TYPE = 'CLICK'
GROUP BY 1,
         2,
         3,
         4,
         5),
NOOFEMAILCLICKUNIQ
AS
(SELECT BRAND,
       TO_CHAR(ACTIVITY_DATE,'YYYY') AS YEAR,
       TO_CHAR(ACTIVITY_DATE,'MON') AS MONTH,
       HCP_ID,
       HCP_MASTER_ID,
       COUNT(EMAIL_NAME) AS EMAILSUNIQCLICK
FROM EDW_HCP360_KPI_RPT
WHERE SOURCE_SYSTEM = 'SFMC' AND EMAIL_ACTIVITY_TYPE = 'CLICK'
AND   IS_UNIQUE = 'Y'
GROUP BY 1,
         2,
         3,
         4,
         5),
NOOFEMAILUNSUBS
AS
(SELECT BRAND,
       TO_CHAR(ACTIVITY_DATE,'YYYY') AS YEAR,
       TO_CHAR(ACTIVITY_DATE,'MON') AS MONTH,
       HCP_ID,
       HCP_MASTER_ID,
       COUNT(EMAIL_NAME) AS EMAILSUNSUBSCRIBED
FROM EDW_HCP360_KPI_RPT
WHERE SOURCE_SYSTEM = 'SFMC' AND EMAIL_ACTIVITY_TYPE = 'UNSUBSCRIBE'
GROUP BY 1,
         2,
         3,
         4,
         5),
NOOFEMAILFORWARD
AS
(SELECT BRAND,
       TO_CHAR(ACTIVITY_DATE,'YYYY') AS YEAR,
       TO_CHAR(ACTIVITY_DATE,'MON') AS MONTH,
       HCP_ID,
       HCP_MASTER_ID,
       COUNT(EMAIL_NAME) AS EMAILSFORWARDED
FROM EDW_HCP360_KPI_RPT
WHERE SOURCE_SYSTEM = 'SFMC' AND EMAIL_ACTIVITY_TYPE = 'FORWARD'
GROUP BY 1,
         2,
         3,
         4,
         5),  

PD_VENT
AS
(SELECT DIM.hcp_id,
        CONSENT_FLAG,
        CONSENT_DATE
 FROM EDW_HCP360_IN_VENTASYS_HCP_DIM_LATEST DIM
 INNER JOIN (SELECT EMAIL,CONSENT_UPDATE_DATETIME as CONSENT_DATE, ROW_NUMBER() OVER(PARTITION BY EMAIL ORDER BY cust_entered_date) AS RNK
             FROM   EDW_HCP360_IN_VENTASYS_HCP_DIM_LATEST) PD
         ON DIM.EMAIL = PD.EMAIL
        AND PD.RNK =1
 WHERE DIM.HCP_ID IS NOT NULL
)


SELECT 'IQVIA_DASHBOARD'  as data_source,
      'Ventasys' AS SOURCE_SYSTEM,
      'IN' as COUNTRY_CODE,
       B.BRAND,
       B.YEAR,
       B.MONTH,
       B.HCP_ID,
       B.HCP_MASTER_ID,
       B.HCP_CREATED_DATE,
       B.TERRITORY_ID,
       B.REGION,
       B.ZONE,
       B.TERRITORY,
       B.REGION_HQ,
       B.SPECIALITY,
       B.CORE_NONCORE,
       B.CLASSIFICATION,
       B.IS_ACTIVE,
       B.FIELD_REP_ACTIVE,
       B.FIRST_PRESCRIPTION_DATE,
       CASE WHEN C.TOTAL_PRESCRIPTIONS >=1 THEN 'Prescriber'
       ELSE 'Non-Prescriber'
       END AS PRESCRIBER_TYPE,
       C.TOTAL_PRESCRIPTIONS,
       C.PRESCRIPTION_UNITS, 
       U.UNIQPROD, 
       B.PLANNED_VISITS,
       AV.ACTUAL_VISITS,
       P.PHONES,
       V.NOOFVIDEO,
       F.NOOFF2F,
       A.AVGPRODDETAIL,
       C.AVERAGE_CALL_DURATION,
       L.LABLES,
       S.SAMPLES_GIVEN,
       ES.EMAILSENT,
       D.EMAILSDELIVERED,
       O.EMAILSOPENED,
       EC.EMAILSCLICKED,
       UC.EMAILSUNIQCLICK,
       EU.EMAILSUNSUBSCRIBED,
       EF.EMAILSFORWARDED,
       ER.EVENT_REG,
       EA.EVENT_ATEND,
       SP.SPEAKER,
       KM.AVG_KEY_MSG,
       PD.CONSENT_FLAG,
       PH.PROJECTED_DOCTORS,
       PD.CONSENT_DATE
FROM BASE B
  LEFT JOIN CALC C
         ON B.BRAND = C.BRAND
        AND B.HCP_ID = C.HCP_ID
        AND B.YEAR = C.YEAR
        AND B.MONTH = C.MONTH   
  LEFT JOIN UNIQPRODTYPE U
         ON B.BRAND = U.BRAND
        AND B.HCP_ID = U.HCP_ID
        AND B.YEAR = U.YEAR
        AND B.MONTH = U.MONTH  
  LEFT JOIN ACTUAL_VISITS AV
         ON B.BRAND = AV.BRAND
        AND B.HCP_ID = AV.HCP_ID
        AND B.YEAR = AV.YEAR
        AND B.MONTH = AV.MONTH
  LEFT JOIN PHONE P
         ON B.BRAND = P.BRAND
        AND B.HCP_ID = P.HCP_ID
        AND B.YEAR = P.YEAR
        AND B.MONTH = P.MONTH
  LEFT JOIN AVG_PROD_DETAILED A
         ON B.BRAND = A.BRAND
        AND B.HCP_ID = A.HCP_ID
        AND B.YEAR = A.YEAR
        AND B.MONTH = A.MONTH
  LEFT JOIN VIDEO V
         ON B.BRAND = V.BRAND
        AND B.HCP_ID = V.HCP_ID
        AND B.YEAR = V.YEAR
        AND B.MONTH = V.MONTH
  LEFT JOIN FACETOFACE F
         ON B.BRAND = F.BRAND
        AND B.HCP_ID = F.HCP_ID
        AND B.YEAR = F.YEAR
        AND B.MONTH = F.MONTH
  LEFT JOIN SAMPLE1 S
         ON b.brand = s.join_column2 
        AND 
        B.HCP_ID = S.HCP_ID
        AND B.YEAR = S.YEAR
        AND B.MONTH = S.MONTH
  LEFT JOIN LABLE L
         ON b.brand = l.join_column
         AND 
        B.HCP_ID = L.HCP_ID
        AND B.YEAR = L.YEAR
        AND B.MONTH = L.MONTH
        
   LEFT JOIN AVG_KEY_MSG KM
         ON B.BRAND = KM.BRAND
        AND B.HCP_MASTER_ID = KM.HCP_MASTER_ID
        AND B.YEAR = KM.YEAR
        AND B.MONTH = KM.MONTH
        
   LEFT JOIN EVENTREGISTERED ER
        ON B.BRAND = ER.BRAND AND 
            B.HCP_MASTER_ID = ER.HCP_MASTER_ID
        AND B.YEAR = ER.YEAR
        AND B.MONTH = ER.MONTH
  LEFT JOIN EVENTATTENDED EA
        ON B.BRAND = EA.BRAND AND
             B.HCP_MASTER_ID = EA.HCP_MASTER_ID
        AND B.YEAR = EA.YEAR
        AND B.MONTH = EA.MONTH
  LEFT JOIN ASSPEAKER SP
         ON B.BRAND = SP.BRAND AND
             B.HCP_MASTER_ID = SP.HCP_MASTER_ID
        AND B.YEAR = SP.YEAR
        AND B.MONTH = SP.MONTH
        
   LEFT JOIN NOOFEMAILSENT ES
         ON B.BRAND = ES.BRAND
        AND B.HCP_MASTER_ID = ES.HCP_MASTER_ID
        AND B.YEAR = ES.YEAR
        AND B.MONTH = ES.MONTH
  LEFT JOIN NOOFEMAILDELIVERED D
         ON B.BRAND = D.BRAND
        AND B.HCP_MASTER_ID = D.HCP_MASTER_ID
        AND B.YEAR = D.YEAR
        AND B.MONTH = D.MONTH
  LEFT JOIN NOOFEMAILOPENED O
         ON B.BRAND = O.BRAND
        AND B.HCP_MASTER_ID = O.HCP_MASTER_ID
        AND B.YEAR = O.YEAR
        AND B.MONTH = O.MONTH
  LEFT JOIN NOOFEMAILCLICKED EC
         ON B.BRAND = EC.BRAND
        AND B.HCP_MASTER_ID = EC.HCP_MASTER_ID
        AND B.YEAR = EC.YEAR
        AND B.MONTH = EC.MONTH
  LEFT JOIN NOOFEMAILCLICKUNIQ UC
         ON B.BRAND = UC.BRAND
        AND B.HCP_MASTER_ID = UC.HCP_MASTER_ID
        AND B.YEAR = UC.YEAR
        AND B.MONTH = UC.MONTH
  LEFT JOIN NOOFEMAILUNSUBS EU
         ON B.BRAND = EU.BRAND
        AND B.HCP_MASTER_ID =EU.HCP_MASTER_ID
        AND B.YEAR = EU.YEAR
        AND B.MONTH = EU.MONTH
  LEFT JOIN NOOFEMAILFORWARD EF
         ON B.BRAND = EF.BRAND
        AND B.HCP_MASTER_ID = EF.HCP_MASTER_ID
        AND B.YEAR = EF.YEAR
        AND B.MONTH = EF.MONTH
  LEFT JOIN PD_VENT PD
         ON B.HCP_ID = PD.HCP_ID
  LEFT JOIN WKS_HCP360_PROJECTED_HCP_SPECIALITY_DETAIL PH
         ON UPPER (B.SPECIALITY) = UPPER (PH.SPECIALTY)
         AND UPPER (B.BRAND) = UPPER (PH.BRAND)
),
temp_sfmc as 
(
WITH BASE1
AS
(SELECT DISTINCT sfmc.BRAND,
       TO_CHAR(ACTIVITY_DATE,'YYYY') AS YEAR,
       TO_CHAR(ACTIVITY_DATE,'MON') AS MONTH,
       sfmc.HCP_ID as HCP_ID,
       sfmc.HCP_CREATED_DATE,
       sfmc.REGION,
       sfmc.ZONE,
       sfmc.TERRITORY,
       sfmc.REGION_HQ,
       sfmc.SPECIALITY,
       sfmc.CORE_NONCORE,
       sfmc.CLASSIFICATION ,
		vent_hcp.hcp_id as ventasys_hcp_id
       FROM EDW_HCP360_KPI_RPT sfmc
left join (select email_name,hcp_id from EDW_HCP360_KPI_RPT where source_system = 'Ventasys' group by 1,2) vent_hcp
on sfmc.email_name = vent_hcp.email_name
LEFT JOIN temp_ventasys AS rpt
ON sfmc.HCP_MASTER_ID = rpt.hcp_MASTER_ID
WHERE rpt.hcp_MASTER_ID IS NULL
OR sfmc.HCP_MASTER_ID IS NULL
and sfmc.SOURCE_SYSTEM = 'SFMC'

),NOOFEMAILSENT
AS
(SELECT BRAND,
       TO_CHAR(ACTIVITY_DATE,'YYYY') AS YEAR,
       TO_CHAR(ACTIVITY_DATE,'MON') AS MONTH,
       HCP_ID,
       COUNT(EMAIL_NAME) AS EMAILSENT
FROM EDW_HCP360_KPI_RPT
WHERE EMAIL_ACTIVITY_TYPE = 'SENT'
GROUP BY 1,
         2,
         3,
         4
         ),


NOOFEMAILDELIVERED
AS
(SELECT BRAND,
       TO_CHAR(ACTIVITY_DATE,'YYYY') AS YEAR,
       TO_CHAR(ACTIVITY_DATE,'MON') AS MONTH,
       HCP_ID,
       COUNT(EMAIL_NAME) AS EMAILSDELIVERED
FROM EDW_HCP360_KPI_RPT
WHERE EMAIL_ACTIVITY_TYPE = 'SENT'
AND   EMAIL_DELIVERED_FLAG = 'Y'
GROUP BY 1,
         2,
         3,
         4),
NOOFEMAILOPENED
AS
(SELECT BRAND,
       TO_CHAR(ACTIVITY_DATE,'YYYY') AS YEAR,
       TO_CHAR(ACTIVITY_DATE,'MON') AS MONTH,
       HCP_ID,
       COUNT(EMAIL_NAME) AS EMAILSOPENED
FROM EDW_HCP360_KPI_RPT
WHERE EMAIL_ACTIVITY_TYPE = 'OPEN'
AND   IS_UNIQUE = 'Y'
GROUP BY 1,
         2,
         3,
         4),
NOOFEMAILCLICKED
AS
(SELECT BRAND,
       TO_CHAR(ACTIVITY_DATE,'YYYY') AS YEAR,
       TO_CHAR(ACTIVITY_DATE,'MON') AS MONTH,
       HCP_ID,
       COUNT(EMAIL_NAME) AS EMAILSCLICKED
FROM EDW_HCP360_KPI_RPT
WHERE EMAIL_ACTIVITY_TYPE = 'CLICK'
GROUP BY 1,
         2,
         3,
         4),
NOOFEMAILCLICKUNIQ
AS
(SELECT BRAND,
       TO_CHAR(ACTIVITY_DATE,'YYYY') AS YEAR,
       TO_CHAR(ACTIVITY_DATE,'MON') AS MONTH,
       HCP_ID,
       COUNT(EMAIL_NAME) AS EMAILSUNIQCLICK
FROM EDW_HCP360_KPI_RPT
WHERE EMAIL_ACTIVITY_TYPE = 'CLICK'
AND   IS_UNIQUE = 'Y'
GROUP BY 1,
         2,
         3,
         4),
NOOFEMAILUNSUBS
AS
(SELECT BRAND,
       TO_CHAR(ACTIVITY_DATE,'YYYY') AS YEAR,
       TO_CHAR(ACTIVITY_DATE,'MON') AS MONTH,
       HCP_ID,
       COUNT(EMAIL_NAME) AS EMAILSUNSUBSCRIBED
FROM EDW_HCP360_KPI_RPT
WHERE EMAIL_ACTIVITY_TYPE = 'UNSUBSCRIBE'
GROUP BY 1,
         2,
         3,
         4),
NOOFEMAILFORWARD
AS
(SELECT BRAND,
       TO_CHAR(ACTIVITY_DATE,'YYYY') AS YEAR,
       TO_CHAR(ACTIVITY_DATE,'MON') AS MONTH,
       HCP_ID,
       COUNT(EMAIL_NAME) AS EMAILSFORWARDED
FROM EDW_HCP360_KPI_RPT
WHERE EMAIL_ACTIVITY_TYPE = 'FORWARD'
GROUP BY 1,
         2,
         3,
         4),

PD_SFMC
AS
(SELECT DIM.HCP_ID,
        (CASE WHEN UPPER(PD.OPT_IN_FOR_COMMUNICATION) = 'TRUE' THEN '1'
        ELSE '0' 
        END) AS CONSENT_FLAG,
        CONSENT_DATE
 FROM EDW_HCP360_SFMC_HCP_DIM DIM
 INNER JOIN (SELECT EMAIL, OPT_IN_FOR_COMMUNICATION, CREATED_DATE AS CONSENT_DATE, ROW_NUMBER() OVER(PARTITION BY EMAIL ORDER BY UPDATED_DATE) AS RNK
             FROM   ITG_HCP360_SFMC_HCP_DETAILS) PD
         ON DIM.EMAIL = PD.EMAIL
        AND PD.RNK =1
 WHERE DIM.HCP_ID IS NOT NULL)

 SELECT 'IQVIA_DASHBOARD'  as data_source,
      'SFMC ' AS SOURCE_SYSTEM,
      'IN' as COUNTRY_CODE,
       B.BRAND,
       B.YEAR,
       B.MONTH,
       B.HCP_ID,
       B.HCP_CREATED_DATE ,
       NULL as territory_id,
       B.REGION ,
       B.ZONE,
       B.TERRITORY,
       B.REGION_HQ ,
       B.SPECIALITY ,
       B.CORE_NONCORE,
       B.CLASSIFICATION,
       'Y' as IS_ACTIVE,
       NULL as IS_ACTIVE_MSR,
       NULL as FIRST_PRESCRIPTION_DATE,
       'Non-Prescriber' AS PRESCRIBER_TYPE,
       NULL as TOTAL_PRESCRIPTIONS,
       NULL as PRESCRIPTION_UNITS,
       NULL as UNIQUE_PRODUCT_TYPE,
       NULL as PLANNED_VISITS,
       NULL as ACTUAL_VISITS,
       NULL as PHONE_CONNECTS,
       NULL as VIDEO_CONNECTS,
       NULL as F2F_CONNECTS,
       NULL as AVG_PROD_DETAILED,
       NULL as AVERAGE_CALL_DURATION,
       NULL as LBL_GIVEN,
       NULL as SAMPLES_GIVEN,
       S.EMAILSENT,
       D.EMAILSDELIVERED,
       O.EMAILSOPENED,
       C.EMAILSCLICKED,
       UC.EMAILSUNIQCLICK,
       U.EMAILSUNSUBSCRIBED,
       F.EMAILSFORWARDED,
       PD.CONSENT_FLAG,
       PD.CONSENT_DATE,
	   B.ventasys_hcp_id
FROM BASE1 B
  LEFT JOIN NOOFEMAILSENT S
         ON 
            B.HCP_ID = S.HCP_ID
        AND B.YEAR = S.YEAR
        AND B.MONTH = S.MONTH
  LEFT JOIN NOOFEMAILDELIVERED D
         ON 
            B.HCP_ID = D.HCP_ID
        AND B.YEAR = D.YEAR
        AND B.MONTH = D.MONTH
  LEFT JOIN NOOFEMAILOPENED O
         ON 
            B.HCP_ID = O.HCP_ID
        AND B.YEAR = O.YEAR
        AND B.MONTH = O.MONTH
  LEFT JOIN NOOFEMAILCLICKED C
         ON 
            B.HCP_ID = C.HCP_ID
        AND B.YEAR = C.YEAR
        AND B.MONTH = C.MONTH
  LEFT JOIN NOOFEMAILCLICKUNIQ UC
         ON  
         B.HCP_ID = UC.HCP_ID
        AND B.YEAR = UC.YEAR
        AND B.MONTH = UC.MONTH
  LEFT JOIN NOOFEMAILUNSUBS U
         ON 
         B.HCP_ID = U.HCP_ID
        AND B.YEAR = U.YEAR
        AND B.MONTH = U.MONTH
  LEFT JOIN NOOFEMAILFORWARD F
         ON 
         B.HCP_ID = F.HCP_ID
        AND B.YEAR = F.YEAR
        AND B.MONTH = F.MONTH
  LEFT JOIN PD_SFMC PD
         ON B.HCP_ID = PD.HCP_ID
),
temp_ga360 as 
(
    SELECT 
       data_source,
       source_system,
       country_code,
       brand,
       YEAR,
       MONTH,
       REGION,
       all_visitor,
       total_session_duration::numeric(18,2) AS total_session_duration,
       sessions::numeric(18,2) AS sessions,
       total_bounces::numeric(18,2) AS total_bounces
    FROM  (SELECT 'IQVIA_DASHBOARD'  as data_source,
              'GA360' AS source_system,
              'IN' AS country_code,
              brand,
              to_char(to_date(year_month,'YYYYMM'),'YYYY') as year, 
              to_char(to_date(year_month,'YYYYMM'),'MON') as month,
              region AS region,
              SUM(all_visitor) AS all_visitor,
              SUM(total_session_duration) AS total_session_duration,
              SUM(sessions) AS sessions,
              SUM(total_bounces) AS total_bounces
        FROM  edw_hcp360_kpi_rpt
        WHERE source_system = 'GA360' 
        AND country = 'IN'
        AND UPPER(visitor_country) = 'INDIA'
        GROUP BY 1,2,3,4,5,6,7)
),
temp_iqvia as 
(
SELECT 'IQVIA_DASHBOARD'  AS data_source,
       'IQVIA' AS source_system,
       'IN' AS country_code,
       brand,
       iqvia_brand,
       brand_category,
       report_brand_reference,
       iqvia_product_description,
       TO_CHAR(activity_date,'YYYY') AS YEAR,
       TO_CHAR(activity_date,'MON') AS MONTH,
       region,     
       SUM(NOOFPRESCRITIONS) AS TOTALPRESCRIPTIONS_IQVIA,
       AVG(TOTALPRESCRITIONS_BY_BRAND) AS TOTALPRESCRIPTIONS_BY_BRAND,
       AVG(TOTALPRESCRITIONS_JNJ_BRAND) AS TOTALPRESCRIPTIONS_JNJ_BRAND      
    FROM  EDW_HCP360_KPI_RPT
    WHERE SOURCE_SYSTEM = 'IQVIA'
    AND SPECIALITY IS NULL
    AND DIAGNOSIS IS NULL
    AND NVL(ACTIVITY_TYPE,'XYZ') != 'IQVIA_SALES'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11
),
temp_final as 
(
SELECT 'IQVIA_DASHBOARD'  as data_source,
       'SALES_CUBE' AS source_system,
       'IN' AS country_code,
        base.brand,
        to_char(to_date(base.year_month,'YYYYMM'),'YYYY') as year, 
        to_char(to_date(base.year_month,'YYYYMM'),'MON') as month,
        base.region AS region ,
        base.zone AS zone,
        base.sales_area AS AREA,
        SUM(base.sales_value) AS SALES_VALUE,
        COUNT(DISTINCT num_buying_retailer) AS NUM_BUYING_RETAILER,
        MAX(ytd.ret_cnt_ytd) AS NUM_BUYING_RETAILER_YTD,
        MAX(mat.ret_cnt_mat) AS NUM_BUYING_RETAILER_MAT     
FROM  wks_tp_salescube_base base
LEFT JOIN  wks_tp_salescube_ytd2 ytd
        ON base.brand = ytd.brand
       AND base.year_month = ytd.year_month
       AND base.region = ytd.region
       AND base.zone = ytd.zone
       AND base.sales_area = ytd.sales_area
LEFT JOIN  wks_tp_salescube_mat2 mat
        ON base.brand = mat.brand
       AND base.year_month = mat.year_month
       AND base.region = mat.region
       AND base.zone = mat.zone
       AND base.sales_area = mat.sales_area
GROUP BY 1,2,3,4,5,6,7,8,9
),
trans_ventasys as 
(
    select 
        brand,
        null as ventasys_id,
        null as ventasys_name,
        null as ventasys_mobile,
        null as ventasys_email,
        null as veeva_id,
        null as veeva_name,
        null as veeva_mobile,
        null as veeva_email,
        null as sfmc_id,
        null as sfmc_name,
        null as sfmc_mobile,
        null as sfmc_email,
        null as master_hcp_key,
        source_system,
        hcp_id,
        null as customer_name,
        null as cell_phone,
        null as email,
        null as account_source_id,
        null as ventasys_team_name,
        null as ventasys_custid,
        null as subscriber_key,
        data_source,
        country_code,
        region,
        zone,
        territory as area,
        classification,
        speciality,
        core_noncore,
        year,
        month,
        hcp_created_date,
        territory_id,
        region_hq,
        is_active,
        field_rep_active as is_active_msr,
        first_prescription_date,
        prescriber_type,
        total_prescriptions,
        prescription_units,
        uniqprod as unique_product_type,
        planned_visits,
        actual_visits,
        phones as phone_connects,
        noofvideo as video_connects,
        nooff2f as f2f_connects,
        avgproddetail as avg_prod_detailed,
        lables as lbl_given,
        samples_given,
        emailsent as EMAILS_SENT,
        emailsdelivered as EMAILS_DELIVERED,
        emailsopened as EMAILS_OPENED,
        emailsclicked as EMAILS_CLICKED,
        emailsuniqclick as EMAILS_UNIQUE_CLICKED,
        emailsunsubscribed as EMAILS_UNSUSBSCRIBED,
        emailsforwarded as EMAILS_FORWARDED,
        average_call_duration,
        event_reg as events_registered,
        event_atend as events_attended,
        speaker as events_as_speaker,
        avg_key_msg as avg_key_msgs_delivered,
        hcp_master_id as master_id,
        null as activity_date,
        null as prescription_id,
        null as all_visitor,
        null as total_session_duration,
        null as sessions,
        null as total_bounces,
        null as sales_value,
        null as iqvia_brand,
        null as brand_category,
        null as report_brand_reference,
        null as iqvia_product_description,
        null as totalprescriptions_iqvia,
        null as totalprescriptions_by_brand,
        null as totalprescriptions_jnj_brand,
        null as num_buying_retailer,
        null as num_buying_retailer_ytd,
        null as num_buying_retailer_mat,
        consent_flag,
        projected_doctors,
        consent_date,
        null as ventasys_hcp_id
    from temp_ventasys
),
trans_sfmc as 
(
    select 
        brand,
        null as ventasys_id,
        null as ventasys_name,
        null as ventasys_mobile,
        null as ventasys_email,
        null as veeva_id,
        null as veeva_name,
        null as veeva_mobile,
        null as veeva_email,
        null as sfmc_id,
        null as sfmc_name,
        null as sfmc_mobile,
        null as sfmc_email,
        null as master_hcp_key,
        source_system,
        hcp_id,
        null as customer_name,
        null as cell_phone,
        null as email,
        null as account_source_id,
        null as ventasys_team_name,
        null as ventasys_custid,
        null as subscriber_key,
        data_source,
        country_code,
        region,
        zone,
        territory as area,
        classification,
        speciality,
        core_noncore,
        year,
        month,
        hcp_created_date,
        territory_id,
        region_hq,
        is_active,
        is_active_msr,
        first_prescription_date,
        prescriber_type,
        total_prescriptions,
        prescription_units,
        unique_product_type,
        planned_visits,
        actual_visits,
        phone_connects,
        video_connects,
        f2f_connects,
        avg_prod_detailed,
        lbl_given,
        samples_given,
        emailsent as emails_sent,
        emailsdelivered as emails_delivered,
        emailsopened as emails_opened,
        emailsclicked as emails_clicked,
        emailsuniqclick as emails_unique_clicked,
        emailsunsubscribed as emails_unsusbscribed,
        emailsforwarded as emails_forwarded,
        average_call_duration,
        null as events_registered,
        null as events_attended,
        null as events_as_speaker,
        null as avg_key_msgs_delivered,
        null as master_id,
        null as activity_date,
        null as prescription_id,
        null as all_visitor,
        null as total_session_duration,
        null as sessions,
        null as total_bounces,
        null as sales_value,
        null as iqvia_brand,
        null as brand_category,
        null as report_brand_reference,
        null as iqvia_product_description,
        null as totalprescriptions_iqvia,
        null as totalprescriptions_by_brand,
        null as totalprescriptions_jnj_brand,
        null as num_buying_retailer,
        null as num_buying_retailer_ytd,
        null as num_buying_retailer_mat,
        consent_flag,
        null as projected_doctors,
        consent_date,
        ventasys_hcp_id
from temp_sfmc
),
trans_ga360 as 
(
    select 
    brand,
    null as ventasys_id,
    null as ventasys_name,
    null as ventasys_mobile,
    null as ventasys_email,
    null as veeva_id,
    null as veeva_name,
    null as veeva_mobile,
    null as veeva_email,
    null as sfmc_id,
    null as sfmc_name,
    null as sfmc_mobile,
    null as sfmc_email,
    null as master_hcp_key,
    source_system,
    null as hcp_id,
    null as customer_name,
    null as cell_phone,
    null as email,
    null as account_source_id,
    null as ventasys_team_name,
    null as ventasys_custid,
    null as subscriber_key,
    data_source,
    country_code,
    region,
    null as zone,
    null as area,
    null as classification,
    null as speciality,
    null as core_noncore,
    year,
    month,
    null as hcp_created_date,
    null as territory_id,
    null as region_hq,
    null as is_active,
    null as is_active_msr,
    null as first_prescription_date,
    null as prescriber_type,
    null as total_prescriptions,
    null as prescription_units,
    null as unique_product_type,
    null as planned_visits,
    null as actual_visits,
    null as phone_connects,
    null as video_connects,
    null as f2f_connects,
    null as avg_prod_detailed,
    null as lbl_given,
    null as samples_given,
    null as emails_sent,
    null as emails_delivered,
    null as emails_opened,
    null as emails_clicked,
    null as emails_unique_clicked,
    null as emails_unsusbscribed,
    null as emails_forwarded,
    null as average_call_duration,
    null as events_registered,
    null as events_attended,
    null as events_as_speaker ,
    null as avg_key_msgs_delivered,
    null as master_id,
    null as activity_date,
    null as prescription_id,
    all_visitor,
    total_session_duration,
    sessions,
    total_bounces,
    null as sales_value,
    null as iqvia_brand,
    null as brand_category,
    null as report_brand_reference,
    null as iqvia_product_description,
    null as totalprescriptions_iqvia,
    null as totalprescriptions_by_brand,
    null as totalprescriptions_jnj_brand,
    null as num_buying_retailer,
    null as num_buying_retailer_ytd,
    null as num_buying_retailer_mat,
    null as consent_flag,
    null as projected_doctors,
    null as consent_date,
    null as ventasys_hcp_id
    from temp_ga360
),
trans_iqvia as (
    select 
        brand,
        null as ventasys_id,
        null as ventasys_name,
        null as ventasys_mobile,
        null as ventasys_email,
        null as veeva_id,
        null as veeva_name,
        null as veeva_mobile,
        null as veeva_email,
        null as sfmc_id,
        null as sfmc_name,
        null as sfmc_mobile,
        null as sfmc_email,
        null as master_hcp_key,
        source_system,
        null as hcp_id,
        null as customer_name,
        null as cell_phone,
        null as email,
        null as account_source_id,
        null as ventasys_team_name,
        null as ventasys_custid,
        null as subscriber_key,
        data_source,
        country_code,
        region,
        null as zone,
        null as area,
        null as classification,
        null as speciality,
        null as core_noncore,
        year,
        month,
        null as hcp_created_date,
        null as territory_id,
        null as region_hq,
        null as is_active,
        null as is_active_msr,
        null as first_prescription_date,
        null as prescriber_type,
        null as total_prescriptions,
        null as prescription_units,
        null as unique_product_type,
        null as planned_visits,
        null as actual_visits,
        null as phone_connects,
        null as video_connects,
        null as f2f_connects,
        null as avg_prod_detailed,
        null as lbl_given,
        null as samples_given,
        null as emails_sent,
        null as emails_delivered,
        null as emails_opened,
        null as emails_clicked,
        null as emails_unique_clicked,
        null as emails_unsusbscribed,
        null as emails_forwarded,
        null as average_call_duration,
        null as events_registered,
        null as events_attended,
        null as events_as_speaker ,
        null as avg_key_msgs_delivered,
        null as master_id,
        null as activity_date,
        null as prescription_id,
        null as all_visitor,
        null as total_session_duration,
        null as sessions,
        null as total_bounces,
        null as sales_value,
        iqvia_brand,
        brand_category,
        report_brand_reference,
        iqvia_product_description,
        totalprescriptions_iqvia,
        totalprescriptions_by_brand,
        totalprescriptions_jnj_brand,
        null as num_buying_retailer,
        null as num_buying_retailer_ytd,
        null as num_buying_retailer_mat,
        null as consent_flag,
        null as projected_doctors,
        null as consent_date,
        null as ventasys_hcp_id
    from temp_iqvia
),
trans_final as 
(
    select 

        brand,
        null as ventasys_id,
        null as ventasys_name,
        null as ventasys_mobile,
        null as ventasys_email,
        null as veeva_id,
        null as veeva_name,
        null as veeva_mobile,
        null as veeva_email,
        null as sfmc_id,
        null as sfmc_name,
        null as sfmc_mobile,
        null as sfmc_email,
        null as master_hcp_key,
        source_system,
        null as hcp_id,
        null as customer_name,
        null as cell_phone,
        null as email,
        null as account_source_id,
        null as ventasys_team_name,
        null as ventasys_custid,
        null as subscriber_key,
        data_source,
        country_code,
        region,
        zone,
        area,
        null as classification,
        null as speciality,
        null as core_noncore,
        year,
        month,
        null as hcp_created_date,
        null as territory_id,
        null as region_hq,
        null as is_active,
        null as is_active_msr,
        null as first_prescription_date,
        null as prescriber_type,
        null as total_prescriptions,
        null as prescription_units,
        null as unique_product_type,
        null as planned_visits,
        null as actual_visits,
        null as phone_connects,
        null as video_connects,
        null as f2f_connects,
        null as avg_prod_detailed,
        null as lbl_given,
        null as samples_given,
        null as emails_sent,
        null as emails_delivered,
        null as emails_opened,
        null as emails_clicked,
        null as emails_unique_clicked,
        null as emails_unsusbscribed,
        null as emails_forwarded,
        null as average_call_duration,
        null as events_registered,
        null as events_attended,
        null as events_as_speaker ,
        null as avg_key_msgs_delivered,
        null as master_id,
        null as activity_date,
        null as prescription_id,
        null as all_visitor,
        null as total_session_duration,
        null as sessions,
        null as total_bounces,
        sales_value,
        null as iqvia_brand,
        null as brand_category,
        null as report_brand_reference,
        null as iqvia_product_description,
        null as totalprescriptions_iqvia,
        null as totalprescriptions_by_brand,
        null as totalprescriptions_jnj_brand,
        num_buying_retailer,
        num_buying_retailer_ytd,
        num_buying_retailer_mat,
        null as consent_flag,
        null as projected_doctors,
        null as consent_date,
        null as ventasys_hcp_id
    from temp_final
),
trans as 
(
    select * from trans_ventasys
    union all
    select * from trans_sfmc
    union all
    select * from trans_ga360
    union all
    select * from trans_iqvia
    union all
    select * from trans_final
    
),
final as 
(
    select 
    brand::varchar(20) as brand,
	ventasys_id::varchar(20) as ventasys_id,
	ventasys_name::varchar(50) as ventasys_name,
	ventasys_mobile::varchar(50) as ventasys_mobile,
	ventasys_email::varchar(100) as ventasys_email,
	veeva_id::varchar(20) as veeva_id,
	veeva_name::varchar(255) as veeva_name,
	veeva_mobile::varchar(40) as veeva_mobile,
	veeva_email::varchar(255) as veeva_email,
	sfmc_id::varchar(50) as sfmc_id,
	sfmc_name::varchar(100) as sfmc_name,
	sfmc_mobile::varchar(20) as sfmc_mobile,
	sfmc_email::varchar(50) as sfmc_email,
	master_hcp_key::varchar(50) as master_hcp_key,
	source_system::varchar(10) as source_system,
	hcp_id::varchar(100) as hcp_id,
	customer_name::varchar(255) as customer_name,
	cell_phone::varchar(50) as cell_phone,
	email::varchar(255) as email,
	account_source_id::varchar(255) as account_source_id,
	ventasys_team_name::varchar(20) as ventasys_team_name,
	ventasys_custid::varchar(20) as ventasys_custid,
	subscriber_key::varchar(50) as subscriber_key,
	data_source::varchar(100) as data_source,
	country_code::varchar(20) as country_code,
	region::varchar(200) as region,
	zone::varchar(50) as zone,
	area::varchar(50) as area,
	classification::varchar(30) as classification,
	speciality::varchar(30) as speciality,
	core_noncore::varchar(20) as core_noncore,
	year::varchar(12) as year,
	month::varchar(11) as month,
	hcp_created_date::date as hcp_created_date,
	territory_id::varchar(20) as territory_id,
	region_hq::varchar(50) as region_hq,
	is_active::varchar(1) as is_active,
	is_active_msr::varchar(1) as is_active_msr,
	first_prescription_date::date as first_prescription_date,
	prescriber_type::varchar(14) as prescriber_type,
	total_prescriptions::number(38,5) as total_prescriptions,
	prescription_units::number(38,2) as prescription_units,
	unique_product_type::number(18,0) as unique_product_type,
	planned_visits::number(38,0) as planned_visits,
	actual_visits::number(18,0) as actual_visits,
	phone_connects::number(18,0) as phone_connects,
	video_connects::number(18,0) as video_connects,
	f2f_connects::number(18,0) as f2f_connects,
	avg_prod_detailed::number(38,4) as avg_prod_detailed,
	lbl_given::number(18,0) as lbl_given,
	samples_given::number(18,0) as samples_given,
	emails_sent::number(18,0) as emails_sent,
	emails_delivered::number(18,0) as emails_delivered,
	emails_opened::number(18,0) as emails_opened,
	emails_clicked::number(18,0) as emails_clicked,
	emails_unique_clicked::number(18,0) as emails_unique_clicked,
	emails_unsusbscribed::number(18,0) as emails_unsusbscribed,
	emails_forwarded::number(18,0) as emails_forwarded,
	average_call_duration::number(38,4) as average_call_duration,
	events_registered::number(18,0) as events_registered,
	events_attended::number(18,0) as events_attended,
	events_as_speaker::number(18,0) as events_as_speaker,
	avg_key_msgs_delivered::number(38,4) as avg_key_msgs_delivered,
	master_id::varchar(32) as master_id,
	activity_date::date as activity_date,
	prescription_id::varchar(50) as prescription_id,
	all_visitor::number(18,2) as all_visitor,
	total_session_duration::number(18,2) as total_session_duration,
	sessions::number(18,2) as sessions,
	total_bounces::number(18,2) as total_bounces,
	sales_value::number(18,2) as sales_value,
	iqvia_brand::varchar(20) as iqvia_brand,
	brand_category::varchar(20) as brand_category,
	report_brand_reference::varchar(50) as report_brand_reference,
	iqvia_product_description::varchar(50) as iqvia_product_description,
	totalprescriptions_iqvia::number(18,2) as totalprescriptions_iqvia,
	totalprescriptions_by_brand::number(18,2) as totalprescriptions_by_brand,
	totalprescriptions_jnj_brand::number(18,2) as totalprescriptions_jnj_brand,
	num_buying_retailer::number(18,2) as num_buying_retailer,
	num_buying_retailer_ytd::number(18,2) as num_buying_retailer_ytd,
	num_buying_retailer_mat::number(18,2) as num_buying_retailer_mat,
	consent_flag::varchar(10) as consent_flag,
	projected_doctors::number(18,0) as projected_doctors,
	consent_date::date as consent_date,
	ventasys_hcp_id::varchar(100) as ventasys_hcp_id
    from trans
)
select * from final
