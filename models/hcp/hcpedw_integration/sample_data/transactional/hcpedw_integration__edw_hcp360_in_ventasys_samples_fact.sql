with itg_hcp360_in_ventasys_sampledata as
(
    select * from {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_sampledata') }}
),
final as
(
    SELECT TEAM_NAME as	team_brand_name		
	,V_SAMPLEID as sample_id  	
	,V_EMPID as employee_id
	,V_CUSTID as hcp_id
	,DCR_DT	as sample_date
	,SAMPLE_PRODUCT as sample_product
	,SAMPLE_UNITS as sample_units
	,CATEGORY as category
	,convert_timezone('UTC',current_timestamp())::timestamp_ntz AS CRT_DTTM	
  	,convert_timezone('UTC',current_timestamp())::timestamp_ntz AS UPDT_DTTM 	
FROM itg_hcp360_in_ventasys_sampledata
)
select team_brand_name::varchar(20) as team_brand_name,
    sample_id::varchar(20) as sample_id,
    employee_id::varchar(20) as employee_id,
    hcp_id::varchar(20) as hcp_id,
    sample_date::date as sample_date,
    sample_product::varchar(50) as sample_product,
    sample_units::number(15,2) as sample_units,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm,
    category::varchar(50) as category
 from final