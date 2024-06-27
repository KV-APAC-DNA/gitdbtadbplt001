{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}}
        WHERE (TEAM_NAME , V_EMPID ) IN (SELECT SEMP.TEAM_NAME , SEMP.V_EMPID 
        FROM {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_employee_master') }} SEMP
        INNER JOIN {{this}} IEMP
        ON SEMP.V_EMPID = IEMP.V_EMPID
        AND SEMP.TEAM_NAME = IEMP.TEAM_NAME);
        {% endif %}"
    )
}}

with source as
(
    select * from {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_employee_master') }}
),
final as
(
    SELECT TEAM_NAME  
    ,V_EMPID   	
    ,V_TERRID   
    ,V_MGR_EMPID
    ,NAME       
    ,DSG        
    ,PHONE		
    ,EMAIL		
    ,CITY		
    ,STATE		
    ,JOIN_DT	
    ,IS_ACTIVE	
    ,REL_DT
    ,CRT_DTTM
    ,convert_timezone('UTC',current_timestamp())::timestamp_ntz as updt_dttm
    ,filename
    FROM source
)
select team_name::varchar(20) as team_name,
    v_empid::varchar(20) as v_empid,
    v_terrid::varchar(20) as v_terrid,
    v_mgr_empid::varchar(20) as v_mgr_empid,
    name::varchar(50) as name,
    dsg::varchar(20) as dsg,
    phone::varchar(50) as phone,
    email::varchar(100) as email,
    city::varchar(50) as city,
    state::varchar(50) as state,
    join_dt::date as join_dt,
    is_active::varchar(100) as is_active,
    rel_dt::date as rel_dt,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm,
    filename::varchar(50) as filename
    from final