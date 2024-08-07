{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}}
        WHERE TO_CHAR(dcr_dt,'YYYYMM') IN (SELECT  TO_CHAR(sdl.dcr_dt,'YYYYMM') 
             FROM {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_dcrdata') }} sdl);
        {% endif %}"
    )
}}

with source as
(
    select *, dense_rank() over (partition by TO_CHAR(dcr_dt,'YYYYMM') order by filename desc) rn 
    from {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_dcrdata') }}
    qualify rn=1
),
final as
(
    SELECT TEAM_NAME	 
    ,V_DCRID		
    ,V_EMPID		
    ,V_CUSTID		
    ,DCR_DT		
    ,DCR_TYPE		
    ,CONTACT_TYPE	
    ,CRT_DTTM
    ,convert_timezone('UTC',current_timestamp())::timestamp_ntz as updt_dttm
    ,filename
    FROM source
)
select team_name::varchar(20) as team_name,
    v_dcrid::varchar(20) as v_dcrid,
    v_empid::varchar(20) as v_empid,
    v_custid::varchar(20) as v_custid,
    dcr_dt::date as dcr_dt,
    dcr_type::varchar(20) as dcr_type,
    contact_type::varchar(50) as contact_type,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm,
    filename::varchar(50) as filename
    from final