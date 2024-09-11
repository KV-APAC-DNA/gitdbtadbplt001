with edw_hcp360_call_fact as
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_call_fact') }}
),
edw_hcp360_ventasys_hcp_dim_snapshot as
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_ventasys_hcp_dim_snapshot') }}
),
edw_hcp360_in_ventasys_brand_map as
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_in_ventasys_brand_map') }}
),
edw_hcp360_in_ventasys_territory_dim as
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_in_ventasys_territory_dim') }}
),
edw_hcp360_in_ventasys_employee_dim as
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_in_ventasys_employee_dim') }}
),
edw_hcp360_in_ventasys_prescription_fact as
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_in_ventasys_prescription_fact') }}
),
edw_hcp360_in_ventasys_samples_fact as
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_in_ventasys_samples_fact') }}
),
cte as
(
    SELECT   CALL.COUNTRY_CODE 
            ,CALL.HCP_ID
            ,CALL.CALL_DATE AS ACTIVITY_DATE 										
            ,CALL.CALL_SOURCE_ID		
            ,CALL.EMPLOYEE_ID 
            ,NULL as prescription_id							
            ,CALL.PRODUCT_INDICATION_NAME	
            ,CALL.CALL_DURATION	  
            ,Cast(NULL as INTEGER) as NO_OF_PRESCRIPTION_UNITS 
            ,H.PLANNED_VISITS_PER_MONTH 	
            ,H.IS_ACTIVE
            ,H.field_rep_active
            ,CALL.Brand 
			      ,CALL.ventasys_contact_type --Change as per Phase 2 (AEBU-5396)
            ,NULL AS SAMPLE_ID
            ,H.TERRITRY_ID AS TERRITORY_ID
			,NULL AS CATEGORY   --New Column Added AEBU-9353
      FROM EDW_HCP360_CALL_FACT CALL,
                        
             (  SELECT H.*, MAP.team_brand_name as brand_name, TERR.* ,CASE WHEN EMP1.IS_ACTIVE = 'Y' THEN  'Y' ELSE 'N' END AS field_rep_active
                 FROM edw_hcp360_ventasys_hcp_dim_snapshot H
                     ,EDW_HCP360_IN_VENTASYS_BRAND_MAP MAP
                     ,EDW_HCP360_IN_VENTASYS_TERRITORY_DIM TERR
                     ,(SELECT DISTINCT case when team_brand_name = 'JB' then 'JBABY' else team_brand_name end as team_brand_name, emp_terrid, IS_ACTIVE FROM EDW_HCP360_IN_VENTASYS_EMPLOYEE_DIM EMP WHERE EMP.IS_ACTIVE = 'Y') EMP1 
                  WHERE H.HCP_ID = MAP.HCP_ID 
                   AND  H.TERRITORY_ID = TERR.TERRITRY_ID 
                   AND MAP.team_brand_name = TERR.team_brand_name
                   AND TERR.TERRITRY_ID = EMP1.EMP_TERRID (+)
                   AND TERR.team_brand_name = EMP1.team_brand_name(+)
                  
              ) H
     WHERE COUNTRY_CODE = 'IN' 
       AND DATA_SOURCE_NAME = 'VENTASYS'  
       AND CALL.HCP_ID = H.HCP_ID 
       AND CALL.CALL_DATE  >=  to_date(H.VALID_FROM) AND CALL.CALL_DATE <= to_date(H.VALID_TO)
       AND CALL.BRAND = H.team_brand_name
),
cte1 as
(
  SELECT 'IN'
		 ,TOTAL. HCP_ID
         ,TOTAL.prescription_date
         ,NULL as CALL_SOURCE_ID 
         ,NULL as EMPLOYEE_ID 
         ,TOTAL.prescription_id
         ,TOTAL.product
         ,NULL  as CALL_DURATION 
         ,AVG_SEL.NO_OF_PRESCRIPTION_UNITS
         ,NULL as PLANNED_VISITS_PER_MONTH 
         ,NULL AS IS_ACTIVE
         ,NULL AS FILED_REP_ACTIVE
         ,TOTAL.BRAND
		 ,NULL AS ventasys_contact_type , --Change as per Phase 2 (AEBU-5396)
          NULL AS sample_id
          ,NULL AS TERRITORY_ID
		  ,NULL AS CATEGORY   --New Column Added AEBU-9353
  FROM 
      ( SELECT  
                    PRES.HCP_ID
                    --,to_date(PRES.prescription_date,'YYYY-MM') as prescription_date
                    ,to_date(left(to_char(PRES.prescription_date, 'YYYY-MM'), 7) || '-01', 'YYYY-MM-DD') as prescription_date
                    ,min(PRES.prescription_id) as prescription_id
                    ,PRES.product as product
                    ,trunc(avg(PRES.NO_OF_PRESCRIPTION_UNITS),2) * 4 as NO_OF_PRESCRIPTION_UNITS
                    ,TEAM_BRAND_NAME as Brand 
              FROM EDW_HCP360_IN_VENTASYS_PRESCRIPTION_FACT PRES 
             WHERE TEAM_BRAND_NAME = 'ORSL' 
                and lower(PRES.product) not like 'benadryl%'
                GROUP BY TEAM_BRAND_NAME,HCP_ID, product, 
                --to_date(PRES.prescription_date,'YYYY-MM')
                to_date(left(to_char(PRES.prescription_date, 'YYYY-MM'), 7) || '-01', 'YYYY-MM-DD')
             ) AVG_SEL 
        ,
       ( SELECT 
                    PRES.HCP_ID
                    --,to_date(PRES.prescription_date,'YYYY-MM') as prescription_date
                    ,to_date(left(to_char(PRES.prescription_date, 'YYYY-MM'), 7) || '-01', 'YYYY-MM-DD') as prescription_date
                    ,PRES.prescription_id as prescription_id
                    ,PRES.product as product
                    ,TEAM_BRAND_NAME as Brand 
              FROM EDW_HCP360_IN_VENTASYS_PRESCRIPTION_FACT PRES 
             WHERE TEAM_BRAND_NAME = 'ORSL' 
              and lower(PRES.product) not like 'benadryl%' 
            ) Total
     WHERE   TOTAL.HCP_ID              = AVG_SEL.HCP_ID            (+)
         AND TOTAL.prescription_date  =  AVG_SEL.prescription_date (+)
         AND TOTAL.prescription_id   =   AVG_SEL.prescription_id   (+)
         AND TOTAL.product          =    AVG_SEL.product           (+)
         AND TOTAL.Brand            =    AVG_SEL.Brand             (+)
),
cte2 as
(
           SELECT  'IN' 
                ,PRES.HCP_ID
                ,PRES.prescription_date
                ,NULL as CALL_SOURCE_ID 
                ,NULL as EMPLOYEE_ID 
                ,PRES.prescription_id
                ,PRES.product
                ,NULL  as CALL_DURATION   
                ,PRES.NO_OF_PRESCRIPTION_UNITS
                ,NULL as PLANNED_VISITS_PER_MONTH 
                ,NULL AS IS_ACTIVE
                ,NULL AS FILED_REP_ACTIVE
                ,TEAM_BRAND_NAME as Brand 
				,NULL AS ventasys_contact_type  --Change as per Phase 2 (AEBU-5396)
				,NULL AS SAMPLE_ID
				,NULL AS TERRITORY_ID
				,NULL AS CATEGORY   --New Column Added AEBU-9353
          FROM EDW_HCP360_IN_VENTASYS_PRESCRIPTION_FACT PRES
         WHERE TEAM_BRAND_NAME != 'ORSL' 
),
cte3 as
(  
SELECT 'IN' AS COUNTRY_CODE,
       HCP_ID,
       SAMPLE_DATE AS ACTIVITY_DATE,
       NULL AS CALL_SOURCE_ID,
       EMPLOYEE_ID,
       NULL AS PRESCRIPTION_ID,
       SAMPLE_PRODUCT AS PRODUCT_INDICATION_NAME,
       NULL AS CALL_DURATION,
       SAMPLE_UNITS AS NO_OF_PRESCRIPTION_UNITS,
       NULL AS PLANNED_VISITS_PER_MONTH,
       NULL AS IS_ACTIVE,
       NULL AS FIELD_REP_ACTIVE,
       CASE WHEN TEAM_BRAND_NAME = 'ORSL' THEN 'ORSL'
            WHEN TEAM_BRAND_NAME = 'JB' THEN 'JBABY'
            WHEN TEAM_BRAND_NAME = 'DERMA' THEN 'DERMA'
       END AS BRAND,
       NULL AS ventasys_contact_type,
      SAMPLE_ID
      ,NULL AS TERRITORY_ID
	  ,CATEGORY   
FROM EDW_HCP360_IN_VENTASYS_SAMPLES_FACT
),
transformed as 
(
    select * from cte
    union all
    select * from cte1
    union all
    select * from cte2
    union all
    select * from cte3
),
final as
(
    select
        country_code::varchar(20) as country_code,
        hcp_id::varchar(50) as hcp_id,
        activity_date::date as activity_date,
        call_source_id::varchar(20) as call_source_id,
        employee_id::varchar(20) as employee_id,
        prescription_id::varchar(50) as prescription_id,
        product_indication_name::varchar(100) as product_indication_name,
        call_duration::varchar(255) as call_duration,
        no_of_prescription_units::number(38,2) as no_of_prescription_units,
        planned_visits_per_month::varchar(10) as planned_visits_per_month,
        is_active::varchar(10) as is_active,
        field_rep_active::varchar(1) as field_rep_active,
        brand::varchar(50) as brand,
        ventasys_contact_type::varchar(50) as ventasys_contact_type,
        sample_id::varchar(20) as sample_id,
        territory_id::varchar(20) as territory_id,
        category::varchar(50) as category
    from transformed
)
select * from final
