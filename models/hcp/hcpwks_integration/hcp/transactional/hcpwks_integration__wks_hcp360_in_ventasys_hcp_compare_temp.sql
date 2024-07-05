with edw_hcp360_in_ventasys_hcp_dim as
(
    select * from {{ source('snapindedw_integration', 'edw_hcp360_in_ventasys_hcp_dim') }}
),
itg_hcp360_in_ventasys_hcp_master as
(
    select * from {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_hcp_master') }}
),
final as
(
    SELECT TEMP.HCP_ID,
			TEMP.HCP_MASTER_ID,
			TEMP.MD5_ITG,
			TEMP.MD5_EDW,
			CASE
			   WHEN TEMP.MD5_ITG = TEMP.MD5_EDW THEN 'NO_CHANGE' ELSE 'UPDATED' END AS COMPARE
	  FROM (SELECT 
					EDW.HCP_ID
					 ,EDW.HCP_MASTER_ID
				   ,MD5( NVL(LOWER(CUSTOMER_NAME),'') || NVL(LOWER(CUSTOMER_TYPE),'') || NVL(LOWER(QUALIFICATION),'') || NVL(LOWER(SPECIALITY),'') || NVL(LOWER(CORE_NONCORE),'')	|| NVL(LOWER(CLASSIFICATION),'')|| NVL(LOWER(IS_FBM_ADOPTED),'') || NVL(LOWER(PLANNED_VISITS_PER_MONTH),'')|| NVL(LOWER(CELL_PHONE),'') || NVL(LOWER(PHONE),'')|| NVL(LOWER(EMAIL),'')|| NVL(LOWER(CITY),'')	|| NVL(LOWER(STATE),'')|| NVL(LOWER(IS_ACTIVE),'')||  nvl(to_char(FIRST_RX_DATE, 'DD-MM-YYYY'), '')) MD5_EDW
				   ,MD5_ITG
			FROM edw_hcp360_in_ventasys_hcp_dim EDW
				 ,(SELECT V_CUSTID
						 ,MD5( NVL(LOWER(CUST_NAME),'') || NVL(LOWER(CUST_TYPE),'') || NVL(LOWER(CUST_QUAL),'') || NVL(LOWER(CUST_SPEC),'') || NVL(LOWER(CORE_NONCORE),'')	|| NVL(LOWER(CLASSIFICATION),'')|| NVL(LOWER(IS_FBM_ADOPTED),'') || NVL(LOWER(VISITS_PER_MONTH),'')|| NVL(LOWER(CELL_PHONE),'') || NVL(LOWER(PHONE),'')|| NVL(LOWER(EMAIL),'')|| NVL(LOWER(CITY),'')	|| NVL(LOWER(STATE),'')|| NVL(LOWER(IS_ACTIVE),'')||  nvl(to_char(FIRST_RX_DATE, 'DD-MM-YYYY'), ''))  MD5_ITG
				   FROM itg_hcp360_in_ventasys_hcp_master ITG
				   ) AS ITG
			 WHERE ITG.V_CUSTID = EDW.HCP_ID
			  AND EDW.VALID_TO = '31-DEC-9999'
			 ) TEMP
)
select * from final