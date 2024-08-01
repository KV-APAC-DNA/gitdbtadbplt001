{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}}
        WHERE (TEAM_NAME , V_CUSTID ) IN (SELECT SEMP.TEAM_NAME , SEMP.V_CUSTID 
        FROM {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_hcp_master') }} SEMP
        INNER JOIN {{this}} IEMP
        ON SEMP.V_CUSTID = IEMP.V_CUSTID
        AND SEMP.TEAM_NAME = IEMP.TEAM_NAME);
        {% endif %}"
    )
}}

with source as
(
    select * from {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_hcp_master') }}
),
final as
(
    SELECT team_name			
    ,v_custid			
    ,v_terrid			
    ,cust_name			
    ,cust_type			
    ,cust_qual			
    ,cust_spec			
    ,core_noncore		
    ,classification		
    ,is_fbm_adopted		
    ,visits_per_month	
    ,cell_phone
    ,phone				
    ,email				
    ,city				
    ,state				
    ,is_active			
    ,first_rx_date
	,cust_entered_date
	,consent_flag
	,consent_update_datetime
    ,crt_dttm
    ,convert_timezone('UTC',current_timestamp())::timestamp_ntz as updt_dttm
    ,filename
    FROM source
)
select team_name::varchar(20) as team_name,
    v_custid::varchar(20) as v_custid,
    v_terrid::varchar(20) as v_terrid,
    cust_name::varchar(50) as cust_name,
    cust_type::varchar(20) as cust_type,
    cust_qual::varchar(50) as cust_qual,
    cust_spec::varchar(20) as cust_spec,
    core_noncore::varchar(20) as core_noncore,
    classification::varchar(50) as classification,
    is_fbm_adopted::varchar(20) as is_fbm_adopted,
    visits_per_month::varchar(10) as visits_per_month,
    cell_phone::varchar(50) as cell_phone,
    phone::varchar(100) as phone,
    email::varchar(100) as email,
    city::varchar(50) as city,
    state::varchar(50) as state,
    is_active::varchar(10) as is_active,
    first_rx_date::date as first_rx_date,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm,
    filename::varchar(50) as filename,
    cust_entered_date::date as cust_entered_date,
    consent_flag::varchar(10) as consent_flag,
    consent_update_datetime::timestamp_ntz(9) as consent_update_datetime
    from final