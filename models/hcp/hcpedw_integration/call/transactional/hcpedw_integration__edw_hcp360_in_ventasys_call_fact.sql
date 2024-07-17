with itg_hcp360_in_ventasys_dcrdata as
(
    select * from {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_dcrdata') }}
),
itg_hcp360_in_ventasys_detailingdata as
(
    select * from {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_detailingdata') }}
),
final as
(
    SELECT
    TEAM_BRAND_NAME
    ,CALL_ID
    ,EMPLOYEE_ID 
    ,HCP_ID 
    ,CALL_DATE
    ,CALL_TYPE
    ,CONTACT_TYPE
    ,PRODUCT
    ,CRT_DTTM
    ,UPDT_DTTM
    FROM 
    (
    SELECT
    case when DCR.TEAM_NAME ='JB' THEN 'JBABY' ELSE DCR.TEAM_NAME END AS TEAM_BRAND_NAME
    ,DCR.V_DCRID AS CALL_ID
    ,DCR.V_EMPID AS EMPLOYEE_ID
    ,DCR.V_CUSTID AS HCP_ID
    ,DCR.DCR_DT AS CALL_DATE
    ,DCR.DCR_TYPE AS CALL_TYPE
    ,DCR.CONTACT_TYPE AS CONTACT_TYPE
    ,NULL AS PRODUCT
    ,convert_timezone('UTC',current_timestamp())::timestamp_ntz AS CRT_DTTM
    ,convert_timezone('UTC',current_timestamp())::timestamp_ntz AS UPDT_DTTM
    FROM ITG_HCP360_IN_VENTASYS_DCRDATA DCR
    WHERE NOT EXISTS ( SELECT 1
    FROM ITG_HCP360_IN_VENTASYS_DETAILINGDATA DTL
    WHERE DTL.V_EMPID = DCR.V_EMPID
    AND DTL.V_CUSTID = DCR.V_CUSTID
    AND DTL.DCR_DT = DCR.DCR_DT
    AND DTL.TEAM_NAME = DCR.TEAM_NAME
    AND SUBSTRING(DTL.V_DTLID,3,LENGTH(DTL.V_DTLID)-2) = SUBSTRING(DCR.V_DCRID,3,LENGTH(DCR.V_DCRID)-2) )

    UNION ALL

    SELECT
        case when DCR.TEAM_NAME ='JB' THEN 'JBABY' ELSE DCR.TEAM_NAME END AS TEAM_BRAND_NAME
    ,DCR.V_DCRID AS CALL_ID
    ,DCR.V_EMPID AS EMPLOYEE_ID
    ,DCR.V_CUSTID AS HCP_ID
    ,DCR.DCR_DT AS CALL_DATE
    ,DCR.DCR_TYPE AS CALL_TYPE
    ,DCR.CONTACT_TYPE AS CONTACT_TYPE
    ,DTL.P1_DTL AS PRODUCT
    ,convert_timezone('UTC',current_timestamp())::timestamp_ntz AS CRT_DTTM
    ,convert_timezone('UTC',current_timestamp())::timestamp_ntz AS UPDT_DTTM
    FROM ITG_HCP360_IN_VENTASYS_DCRDATA DCR
    JOIN ITG_HCP360_IN_VENTASYS_DETAILINGDATA DTL    
    ON DTL.V_EMPID = DCR.V_EMPID
    AND DTL.V_CUSTID = DCR.V_CUSTID
    AND DTL.DCR_DT = DCR.DCR_DT
    AND DTL.TEAM_NAME = DCR.TEAM_NAME
    AND SUBSTRING(DTL.V_DTLID,3,LENGTH(DTL.V_DTLID)-2) = SUBSTRING(DCR.V_DCRID,3,LENGTH(DCR.V_DCRID)-2) 
    AND DTL.P1_DTL IS NOT NULL

    UNION ALL

    SELECT
    case when DCR.TEAM_NAME ='JB' THEN 'JBABY' ELSE DCR.TEAM_NAME END AS TEAM_BRAND_NAME
    ,DCR.V_DCRID AS CALL_ID
    ,DCR.V_EMPID AS EMPLOYEE_ID
    ,DCR.V_CUSTID AS HCP_ID
    ,DCR.DCR_DT AS CALL_DATE
    ,DCR.DCR_TYPE AS CALL_TYPE
    ,DCR.CONTACT_TYPE AS CONTACT_TYPE
    ,DTL.P2_DTL AS PRODUCT
    ,convert_timezone('UTC',current_timestamp())::timestamp_ntz AS CRT_DTTM
    ,convert_timezone('UTC',current_timestamp())::timestamp_ntz AS UPDT_DTTM
    FROM ITG_HCP360_IN_VENTASYS_DCRDATA DCR
    JOIN ITG_HCP360_IN_VENTASYS_DETAILINGDATA DTL    
    ON DTL.V_EMPID = DCR.V_EMPID
    AND DTL.V_CUSTID = DCR.V_CUSTID
    AND DTL.DCR_DT = DCR.DCR_DT
    AND DTL.TEAM_NAME = DCR.TEAM_NAME
    AND SUBSTRING(DTL.V_DTLID,3,LENGTH(DTL.V_DTLID)-2) = SUBSTRING(DCR.V_DCRID,3,LENGTH(DCR.V_DCRID)-2) 
    AND DTL.P2_DTL IS NOT NULL

    UNION ALL

    SELECT
    case when DTL.TEAM_NAME ='JB' THEN 'JBABY' ELSE DTL.TEAM_NAME END AS TEAM_BRAND_NAME
    ,DCR.V_DCRID AS CALL_ID
    ,DTL.V_EMPID AS EMPLOYEE_ID
    ,DTL.V_CUSTID AS HCP_ID
    ,DTL.DCR_DT AS CALL_DATE
    ,DCR.DCR_TYPE AS CALL_TYPE
    ,DCR.CONTACT_TYPE AS CONTACT_TYPE
    ,DTL.P3_DTL AS PRODUCT
    ,convert_timezone('UTC',current_timestamp())::timestamp_ntz AS CRT_DTTM
    ,convert_timezone('UTC',current_timestamp())::timestamp_ntz AS UPDT_DTTM
    FROM ITG_HCP360_IN_VENTASYS_DCRDATA DCR
    JOIN ITG_HCP360_IN_VENTASYS_DETAILINGDATA DTL    
    ON DTL.V_EMPID = DCR.V_EMPID
    AND DTL.V_CUSTID = DCR.V_CUSTID
    AND DTL.DCR_DT = DCR.DCR_DT
    AND DTL.TEAM_NAME = DCR.TEAM_NAME
    AND SUBSTRING(DTL.V_DTLID,3,LENGTH(DTL.V_DTLID)-2) = SUBSTRING(DCR.V_DCRID,3,LENGTH(DCR.V_DCRID)-2) 
    AND DTL.P3_DTL IS NOT NULL

    UNION ALL

    SELECT
    case when DTL.TEAM_NAME ='JB' THEN 'JBABY' ELSE DTL.TEAM_NAME END AS TEAM_BRAND_NAME
    ,DCR.V_DCRID AS CALL_ID
    ,DTL.V_EMPID AS EMPLOYEE_ID
    ,DTL.V_CUSTID AS HCP_ID
    ,DTL.DCR_DT AS CALL_DATE
    ,DCR.DCR_TYPE AS CALL_TYPE
    ,DCR.CONTACT_TYPE AS CONTACT_TYPE
    ,DTL.P4_DTL AS PRODUCT
    ,convert_timezone('UTC',current_timestamp())::timestamp_ntz AS CRT_DTTM
    ,convert_timezone('UTC',current_timestamp())::timestamp_ntz AS UPDT_DTTM
    FROM ITG_HCP360_IN_VENTASYS_DCRDATA DCR
    JOIN ITG_HCP360_IN_VENTASYS_DETAILINGDATA DTL    
    ON DTL.V_EMPID = DCR.V_EMPID
    AND DTL.V_CUSTID = DCR.V_CUSTID
    AND DTL.DCR_DT = DCR.DCR_DT
    AND DTL.TEAM_NAME = DCR.TEAM_NAME
    AND SUBSTRING(DTL.V_DTLID,3,LENGTH(DTL.V_DTLID)-2) = SUBSTRING(DCR.V_DCRID,3,LENGTH(DCR.V_DCRID)-2) 
    AND DTL.P4_DTL IS NOT NULL
    )
)
select team_brand_name::varchar(20) as team_brand_name,
    call_id::varchar(20) as call_id,
    employee_id::varchar(20) as employee_id,
    hcp_id::varchar(20) as hcp_id,
    call_date::date as call_date,
    call_type::varchar(20) as call_type,
    contact_type::varchar(50) as contact_type,
    product::varchar(50) as product,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
 from final