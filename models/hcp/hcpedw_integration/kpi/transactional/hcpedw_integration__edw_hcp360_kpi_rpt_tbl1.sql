{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = " {% if is_incremental() %}
        delete from {{this}} where rtrim(upper(source_system)) = 'VENTASYS';
        {% endif %}"
    )
}}
with
vw_edw_hcp360_ventasys_hcp_dim as 
(
    select * from {{ ref('hcpedw_integration__vw_edw_hcp360_ventasys_hcp_dim') }}
),
edw_hcp360_in_ventasys_brand_map as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_in_ventasys_brand_map') }}
),
edw_hcp360_in_ventasys_territory_dim as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_in_ventasys_territory_dim') }}
),
vw_edw_hcp360_hcpmaster_dim as 
(
    select * from {{ ref('hcpedw_integration__vw_edw_hcp360_hcpmaster_dim') }}
),
edw_hcp360_in_ventasys_employee_dim as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_in_ventasys_employee_dim') }}
),
wks_edw_hcp360_ventasys_call_rx as 
(
    select * from {{ ref('hcpwks_integration__wks_edw_hcp360_ventasys_call_rx') }}
),
edw_hcp360_call_fact as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_call_fact') }}
),
edw_hcp360_ventasys_hcp_dim_snapshot as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_ventasys_hcp_dim_snapshot') }}
),
wks_edw_hcp360_ventasys_hcp_status as 
(
    select * from {{ ref('hcpwks_integration__wks_edw_hcp360_ventasys_hcp_status') }}
),
tempa as 
(
    SELECT 
         'IN' AS COUNTRY												
        ,'VENTASYS' AS SOURCE_SYSTEM					
        ,'NON_DIGITAL' AS CHANNEL				
        ,'F2F' AS ACTIVITY_TYPE					
        ,HCP.HCP_ID											
        ,HCP.HCP_MASTER_ID									
        ,CALL.EMPLOYEE_ID									
        ,CASE WHEN HCP.BRAND_NAME ='JB' THEN 'JBABY' ELSE HCP.BRAND_NAME END AS BRAND											
        ,NULL AS BRAND_CATEGORY			
      ,CASE WHEN  HCP.BRAND_NAME ='ORSL' THEN 
  				case   when HCP.speciality in ( 'Physician','Phy','GP - MBBS')   then 'GP - MBBS'
  					   when HCP.speciality in ('GP - Non-MBBS') then  'GP - Non-MBBS'
  					   when HCP.speciality in ('pedia','Pedia','Pediatrician') then 'Pediatrics'
  					   when HCP.speciality in ( 'Gynecologist','Gyne','Gynac') then 'Gynecology'
  					   when HCP.speciality not in ('Physician','Phy','GP - MBBS','GP - Non-MBBS','pedia','Pedia','Pediatrician','Gynecologist','Gyne','Gynac') then 'Others' 
  				end  
  		WHEN  HCP.BRAND_NAME IN ('JB','JBABY') THEN 
  				case  when  HCP.speciality in ( 'Gyne') then 'Gynecology'  
  					  when  HCP.speciality in ('pedia','Pedia') then 'Pediatrics'
  					  when  HCP.speciality not in ('Gyne','pedia','Pedia') then 'Others'
  				end
  		WHEN  HCP.BRAND_NAME ='DERMA' THEN   
  				case when  HCP.speciality in ( 'Derma') then 'Dermatologist'  
  					 when  HCP.speciality in ('pedia','Pedia') then 'Pediatrics'
  					 when  HCP.speciality in ('Cosmo') then 'Cosmo'
  					 when  HCP.speciality not in ('Derma','pedia','Pedia','Cosmo') then 'Others'
                  end
          END AS Speciality							
      ,HCP.CORE_NONCORE									
      ,HCP.CLASSIFICATION 								
      ,SUBSTRING(HCP.TERRITORY,6,LENGTH(HCP.TERRITORY)) AS TERRITORY										
      ,SUBSTRING(HCP.REGION_HQ,6,LENGTH(HCP.REGION_HQ)) AS REGION		
      ,VW_MASTER.ORGANIZATION_L3_NAME  AS REGION_HQ 									
      ,SUBSTRING(HCP.ZONE,6,LENGTH(HCP.ZONE)) AS  ZONE		
      ,NULL AS HCP_CREATED_DATE								
      ,CALL.ACTIVITY_DATE AS ACTIVITY_DATE 										
      ,CALL.CALL_SOURCE_ID									
      ,CALL.PRODUCT_INDICATION_NAME					 
      ,CALL.NO_OF_PRESCRIPTION_UNITS						
      ,HCP.FIRST_RX_DATE AS FIRST_PRESCRIPTION_DATE												
      ,CALL.PLANNED_VISITS_PER_MONTH AS PLANNED_CALL_CNT	
      ,CALL.CALL_DURATION	     						
      ,NULL AS DIAGNOSIS   
      ,CALL.PRESCRIPTION_ID AS PRESCRIPTION_ID
      ,NULL AS NoofPrescritions    
      ,NULL AS NoofPrescribers   
      ,NULL AS EMAIL_NAME	
      ,NULL AS IS_UNIQUE
      ,NULL AS EMAIL_DELIVERED_FLAG	
      ,NULL AS EMAIL_ACTIVITY_TYPE
      ,NVL(CALL.IS_ACTIVE,HCP.IS_ACTIVE) AS IS_ACTIVE
      ,'Y' AS TRANSACTION_FLAG 					
      ,NVL(CALL.FIELD_REP_ACTIVE,HCP.FIELD_REP_ACTIVE) AS FIELD_REP_ACTIVE		
  	  ,CALL.VENTASYS_CONTACT_TYPE AS CALL_TYPE
  	  ,CALL.TERRITORY_ID AS TERRITORY_ID 
      ,CALL.SAMPLE_ID 
	  ,CALL.CATEGORY
      ,current_timestamp() AS CRT_DTTM
      ,current_timestamp() AS UPDT_DTTM	  
FROM (select * from (  SELECT H.*, MAP.team_brand_name as brand_name, TERR.* ,CASE WHEN EMP1.IS_ACTIVE = 'Y' THEN  'Y' ELSE 'N' END AS field_rep_active
         FROM vw_edw_hcp360_ventasys_hcp_dim H
             ,EDW_HCP360_IN_VENTASYS_BRAND_MAP MAP
             ,EDW_HCP360_IN_VENTASYS_TERRITORY_DIM TERR
             ,(SELECT DISTINCT case when team_brand_name = 'JB' then 'JBABY' else team_brand_name end as team_brand_name, emp_terrid, IS_ACTIVE FROM EDW_HCP360_IN_VENTASYS_EMPLOYEE_DIM EMP WHERE EMP.IS_ACTIVE = 'Y') EMP1 
          WHERE H.HCP_ID = MAP.HCP_ID 
           AND  H.TERRITORY_ID = TERR.TERRITRY_ID 
           AND MAP.team_brand_name = TERR.team_brand_name
           AND TERR.TERRITRY_ID = EMP1.EMP_TERRID (+)
           AND TERR.team_brand_name = EMP1.team_brand_name(+)
      )) HCP
      
  ,WKS_EDW_HCP360_VENTASYS_CALL_RX CALL        
  ,(select distinct hcp_master_id,hcp_id_ventasys,organization_l3_name
      From vw_edw_hcp360_hcpmaster_dim ) vw_master
WHERE CALL.HCP_ID = HCP.HCP_ID (+)
  AND CALL.BRAND = HCP.BRAND_NAME (+) 
  AND HCP.HCP_MASTER_ID = vw_master.HCP_MASTER_ID (+)
  AND HCP.HCP_ID = vw_master.hcp_id_ventasys (+)
  AND to_date(HCP.VALID_TO) = '9999-12-31'
),
tempb as 
(
    SELECT  	
        TEMP.COUNTRY
      ,	TEMP.SOURCE_SYSTEM
      ,	TEMP.CHANNEL
      ,	TEMP.ACTIVITY_TYPE
      ,	TEMP.HCP_ID
      ,	TEMP.HCP_MASTER_ID
      , TEMP.EMPLOYEE_ID
      ,	TEMP.BRAND
      , TEMP.BRAND_CATEGORY	
      ,	TEMP.SPECIALITY
      ,	TEMP.CORE_NONCORE
      ,	TEMP.CLASSIFICATION
      ,	TEMP.TERRITORY
      ,	TEMP.REGION
      ,	TEMP.REGION_HQ
      ,	TEMP.ZONE
      ,	TEMP.HCP_CREATED_DATE
      ,	TEMP.ACTIVITY_DATE
      , TEMP.CALL_SOURCE_ID									
      , TEMP.PRODUCT_INDICATION_NAME		 			
      , TEMP.NO_OF_PRESCRIPTION_UNITS		
      ,	TEMP.FIRST_PRESCRIPTION_DATE
      ,	TEMP.PLANNED_CALL_CNT
      , TEMP.CALL_DURATION						
      , TEMP.DIAGNOSIS
      , TEMP.PRESCRIPTION_ID 
      , TEMP.NOOFPRESCRITIONS 
      , TEMP.NOOFPRESCRIBERS 
      , TEMP.EMAIL_NAME		
      , TEMP.IS_UNIQUE
      , TEMP.EMAIL_DELIVERED_FLAG	
      , TEMP.EMAIL_ACTIVITY_TYPE
      , TEMP.IS_ACTIVE
      ,	TEMP.TRANSACTION_FLAG
      , TEMP.field_rep_active
	    , CALL.VENTASYS_CONTACT_TYPE AS  CALL_TYPE 
		  ,TEMP.TERRITORY_ID  
        ,null as SAMPLE_ID
         ,null as CATEGORY
      ,	TEMP.CRT_DTTM
      ,	TEMP.UPDT_DTTM

FROM (
SELECT 	COUNTRY
      ,	SOURCE_SYSTEM
      ,	CHANNEL
      ,	ACTIVITY_TYPE
      ,	HCP_ID
      ,	HCP_MASTER_ID
      , EMPLOYEE_ID
      ,	BRAND
     	, BRAND_CATEGORY
      ,	SPECIALITY
      ,	CORE_NONCORE
      ,	CLASSIFICATION
      ,	TERRITORY
      ,	REGION
      ,	REGION_HQ
      ,	ZONE
      ,	HCP_CREATED_DATE
      ,	ACTIVITY_DATE
      , CALL_SOURCE_ID									
      , PRODUCT_INDICATION_NAME		 			
      , NO_OF_PRESCRIPTION_UNITS		
      ,	FIRST_PRESCRIPTION_DATE
      ,	PLANNED_CALL_CNT
      , CALL_DURATION						
      , DIAGNOSIS
      , PRESCRIPTION_ID 
      , NOOFPRESCRITIONS 
      , NOOFPRESCRIBERS 
      , EMAIL_NAME		
      , IS_UNIQUE
      , EMAIL_DELIVERED_FLAG	
      , EMAIL_ACTIVITY_TYPE
      , IS_ACTIVE
      ,	TRANSACTION_FLAG
      , field_rep_active
	 -- , CALL_TYPE --Change as per Phase 2 (AEBU-5396)
	     ,TERRITORY_ID
      ,	CRT_DTTM
      ,	UPDT_DTTM

FROM (     
            SELECT 
             'IN' AS COUNTRY												
            ,'VENTASYS' AS SOURCE_SYSTEM					
            ,'NON_DIGITAL' AS CHANNEL	 --Check KPI Sheet 				
            ,'F2F' AS ACTIVITY_TYPE		 --Check KPI Sheet			
            ,HCP.HCP_ID											
            ,HCP.HCP_MASTER_ID									
            ,NULL AS EMPLOYEE_ID								
            ,CASE WHEN MAP.TEAM_BRAND_NAME ='JB' THEN 'JBABY' ELSE MAP.TEAM_BRAND_NAME END AS BRAND												
           	,NULL AS BRAND_CATEGORY									
            --,vw_master.SPECIALITY				
            , CASE WHEN  MAP.TEAM_BRAND_NAME ='ORSL' THEN 
						case   when HCP.speciality in ( 'Physician','Phy','GP - MBBS')   then 'GP - MBBS'
							   when HCP.speciality in ('GP - Non-MBBS','GP - Non MBBS') then  'GP - Non-MBBS'
							   when HCP.speciality in ('pedia','Pedia','Pediatrician') then 'Pediatrics'
							   when HCP.speciality in ( 'Gynecologist','Gyne','Gynac') then 'Gynecology'
							   when HCP.speciality not in ('Physician','Phy','GP - MBBS','GP - Non-MBBS','pedia','Pedia','Pediatrician','Gynecologist','Gyne','Gynac') then 'Others' 
						end  
					WHEN  MAP.TEAM_BRAND_NAME IN ('JB','JBABY') THEN 
						case  when  HCP.speciality in ( 'Gyne') then 'Gynecology'  
							  when  HCP.speciality in ('pedia','Pedia') then 'Pediatrics' 
							  when  HCP.speciality not in ('Gyne','pedia','Pedia') then 'Others'
				        end
					WHEN  MAP.TEAM_BRAND_NAME ='DERMA' THEN   
						case when  HCP.speciality in ( 'Derma') then 'Dermatologist'  
							 when  HCP.speciality in ('pedia','Pedia') then 'Pediatrics'
						     when  HCP.speciality in ('Cosmo') then 'Cosmo'
							 when  HCP.speciality not in ('Derma','pedia','Pedia','Cosmo') then 'Others'
						end
					END AS Speciality    						
            ,HCP.CORE_NONCORE									
            ,HCP.CLASSIFICATION 								
            ,SUBSTRING(TERR.TERRITORY,6,LENGTH(TERR.TERRITORY)) AS TERRITORY										
            ,SUBSTRING(TERR.REGION_HQ,6,LENGTH(TERR.REGION_HQ)) AS REGION							
            ,VW_MASTER.ORGANIZATION_L3_NAME AS REGION_HQ     				
            ,SUBSTRING(TERR.ZONE,6,LENGTH(TERR.ZONE)) AS  ZONE											
            ,HCP.CUST_ENTERED_DATE AS HCP_CREATED_DATE								
            ,to_date(snap.cal_mnth_id::varchar,'YYYYMM') AS ACTIVITY_DATE 	
           -- ,HCP.VALID_FROM as valid_from 
            ,SNAP.VALID_TO	AS VALID_TO												 
           	, NULL AS CALL_SOURCE_ID									
            , NULL  AS PRODUCT_INDICATION_NAME		 			
            , NULL AS NO_OF_PRESCRIPTION_UNITS				
            ,HCP.first_rx_date AS FIRST_PRESCRIPTION_DATE												
            ,HCP.PLANNED_VISITS_PER_MONTH AS PLANNED_CALL_CNT
            ,NULL AS CALL_DURATION						
            ,NULL AS DIAGNOSIS
            ,NULL AS PRESCRIPTION_ID 
            ,NULL AS NOOFPRESCRITIONS 
            ,NULL AS NOOFPRESCRIBERS 
            ,NULL AS EMAIL_NAME		
            ,NULL AS IS_UNIQUE
            ,NULL AS EMAIL_DELIVERED_FLAG	
            ,NULL AS EMAIL_ACTIVITY_TYPE		
            ,HCP.IS_ACTIVE AS IS_ACTIVE				
            ,'N' AS TRANSACTION_FLAG	   
            ,CASE WHEN TERR.IS_ACTIVE = 'Y' THEN  'Y' ELSE 'N' END AS field_rep_active 		
			--,NULL AS CALL_TYPE   --Change as per Phase 2 (AEBU-5396)
			     ,TERR.TERRITRY_ID AS TERRITORY_ID --Change as per Phase2 (Tocuhpoint report)
            ,current_timestamp() AS CRT_DTTM
            ,current_timestamp() AS UPDT_DTTM	  
        FROM edw_hcp360_ventasys_hcp_dim_snapshot HCP
        ---HCP Snapshot in each month
            ,wks_edw_hcp360_ventasys_hcp_status SNAP

          	,(SELECT TERR1.* , EMP1.IS_ACTIVE 
          	    FROM EDW_HCP360_IN_VENTASYS_TERRITORY_DIM TERR1 
          	       ,(SELECT DISTINCT case when team_brand_name = 'JB' then 'JBABY' else team_brand_name end as team_brand_name, emp_terrid, IS_ACTIVE 
          	           FROM EDW_HCP360_IN_VENTASYS_EMPLOYEE_DIM EMP 
          	           WHERE EMP.IS_ACTIVE = 'Y') EMP1 
          	   WHERE TERR1.TERRITRY_ID = EMP1.EMP_TERRID (+) 
          	     AND TERR1.team_brand_name = EMP1.team_brand_name(+)
          	    ) TERR
          	,EDW_HCP360_IN_VENTASYS_BRAND_MAP MAP 
          	,(select distinct hcp_master_id,hcp_id_ventasys,organization_l3_name
                From vw_edw_hcp360_hcpmaster_dim ) VW_MASTER
			
        WHERE TERR.TERRITRY_ID = HCP.TERRITORY_ID
          AND MAP.HCP_ID 			= HCP.HCP_ID
          AND MAP.TEAM_BRAND_NAME = TERR.TEAM_BRAND_NAME 	
          AND HCP.HCP_MASTER_ID = vw_master.hcp_master_id (+) 
          and hcp.hcp_id = vw_master.hcp_id_ventasys (+)
          and hcp.hcp_id = snap.hcp_id 
          and hcp.valid_to = snap.valid_to
          --and hcp.hcp_id = 'C2027088'
          AND TO_CHAR(ACTIVITY_DATE,'YYYYMM') <= SNAP.CAL_MNTH_ID
          ) BASE
    ) TEMP,
    (SELECT DISTINCT ventasys_contact_type FROM EDW_HCP360_CALL_FACT) CALL      

),
final as 
(
    select * from tempa
    union all
    select * from tempb
)
select * from final