with itg_hcp360_in_ventasys_rxdata as
(
    select * from {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_rxdata') }}
),
final as
(
    SELECT case when team_name ='JB' THEN 'JBABY' ELSE team_name END AS team_brand_name 		
	,V_RXID	as prescription_id		
	,V_EMPID as employee_id	
	,V_CUSTID as hcp_id
	,DCR_DT as	prescription_date
	,RX_PRODUCT as product
	,RX_UNITS as no_of_prescription_units
	,convert_timezone('UTC',current_timestamp())::timestamp_ntz AS CRT_DTTM	
  	,convert_timezone('UTC',current_timestamp())::timestamp_ntz AS UPDT_DTTM 	
FROM ITG_HCP360_IN_VENTASYS_RXDATA
)
select team_brand_name::varchar(20) as team_brand_name,
    prescription_id::varchar(50) as prescription_id,
    employee_id::varchar(50) as employee_id,
    hcp_id::varchar(50) as hcp_id,
    prescription_date::date as prescription_date,
    product::varchar(50) as product,
    no_of_prescription_units::number(15,2) as no_of_prescription_units,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
 from final