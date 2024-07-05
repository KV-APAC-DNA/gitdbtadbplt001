with itg_hcp360_in_ventasys_hcp_master as
(
    select * from {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_hcp_master') }}
),
edw_hcp360_hcp_master_key_by_brand as
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_hcp_master_key_by_brand') }}
),
final as
(
    SELECT 
    ITG.V_CUSTID as HCP_ID,
    master_hcp_key as HCP_MASTER_ID,
    ITG.V_TERRID as TERRITORY_ID,
    ITG.CUST_NAME as CUSTOMER_NAME,
    ITG.CUST_TYPE as CUSTOMER_TYPE,
    ITG.CUST_QUAL as QUALIFICATION,
    ITG.CUST_SPEC as SPECIALITY,
    ITG.CORE_NONCORE as CORE_NONCORE,
    ITG.CLASSIFICATION as CLASSIFICATION,
    ITG.IS_FBM_ADOPTED as IS_FBM_ADOPTED,
    ITG.VISITS_PER_MONTH as PLANNED_VISITS_PER_MONTH,
    cell_phone as cell_phone,
    ITG.PHONE as PHONE,
    ITG.EMAIL as EMAIL,
    ITG.CITY as CITY,
    ITG.STATE as STATE,
    ITG.IS_ACTIVE as IS_ACTIVE,
    ITG.FIRST_RX_DATE as FIRST_RX_DATE,
    ITG.cust_entered_date as cust_entered_date,
    ITG.cust_entered_date as VALID_FROM,    
    '31-DEC-9999' AS VALID_TO,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz AS CRT_DTTM,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz AS UPDT_DTTM,
    consent_flag,
    consent_update_datetime
FROM itg_hcp360_in_ventasys_hcp_master ITG
LEFT JOIN (SELECT DISTINCT master_hcp_key, ventasys_team_name, ventasys_custid
             FROM edw_hcp360_hcp_master_key_by_brand 
            WHERE ventasys_custid is not null ) master_hcp 
ON ITG.v_custid =ventasys_custid 
AND ITG.team_name = ventasys_team_name
)
select hcp_id::varchar(20) as hcp_id,
    hcp_master_id::varchar(50) as hcp_master_id,
    territory_id::varchar(20) as territory_id,
    customer_name::varchar(150) as customer_name,
    customer_type::varchar(20) as customer_type,
    qualification::varchar(50) as qualification,
    speciality::varchar(20) as speciality,
    core_noncore::varchar(20) as core_noncore,
    classification::varchar(50) as classification,
    is_fbm_adopted::varchar(20) as is_fbm_adopted,
    planned_visits_per_month::varchar(10) as planned_visits_per_month,
    cell_phone::varchar(50) as cell_phone,
    phone::varchar(100) as phone,
    email::varchar(100) as email,
    city::varchar(50) as city,
    state::varchar(50) as state,
    is_active::varchar(10) as is_active,
    first_rx_date::date as first_rx_date,
    cust_entered_date::date as cust_entered_date,
    valid_from::timestamp_ntz(9) as valid_from,
    valid_to::timestamp_ntz(9) as valid_to,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm,
    consent_flag::varchar(10) as consent_flag,
    consent_update_datetime::timestamp_ntz(9) as consent_update_datetime
from final