{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append"     
    )
}}

with itg_hcp360_in_ventasys_hcp_master as
(
    select * from {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_hcp_master') }}
),
final as
(
    SELECT
    ITG.V_CUSTID as hcp_id
   ,CASE WHEN ITG.TEAM_NAME ='JB' THEN 'JBABY' ELSE ITG.TEAM_NAME END AS TEAM_BRAND_NAME  
   ,convert_timezone('UTC',current_timestamp())::timestamp_ntz AS CRT_DTTM	
   ,convert_timezone('UTC',current_timestamp())::timestamp_ntz AS UPDT_DTTM
   FROM itg_hcp360_in_ventasys_hcp_master ITG
    LEFT JOIN {{this}} MAP
    ON ITG.V_CUSTID = MAP.HCP_ID
    AND CASE WHEN ITG.TEAM_NAME ='JB' THEN 'JBABY' ELSE ITG.TEAM_NAME END = MAP.TEAM_BRAND_NAME 
    WHERE MAP.TEAM_BRAND_NAME IS NULL AND MAP.HCP_ID IS NULL
)
select hcp_id::varchar(20) as hcp_id,
    team_brand_name::varchar(20) as team_brand_name,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
 from final