with itg_hcp360_in_ventasys_territory_master as
(
    select * from {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_territory_master') }}
),
final as
(
    SELECT case when  TEAM_NAME ='JB' THEN 'JBABY' ELSE  TEAM_NAME END AS TEAM_BRAND_NAME  
    ,V_TERRID as territry_id
    ,LVL1 as region_hq
    ,LVL2 as zone
    ,LVL3 as territory
    ,HQ as name
	,convert_timezone('UTC',current_timestamp())::timestamp_ntz AS CRT_DTTM	
  	,convert_timezone('UTC',current_timestamp())::timestamp_ntz AS UPDT_DTTM 	
FROM itg_hcp360_in_ventasys_territory_master
)
select team_brand_name::varchar(20) as team_brand_name,
    territry_id::varchar(20) as territry_id,
    region_hq::varchar(50) as region_hq,
    zone::varchar(50) as zone,
    territory::varchar(50) as territory,
    name::varchar(100) as name,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
 from final