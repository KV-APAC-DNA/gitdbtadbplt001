{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}}
        WHERE TO_CHAR(dcr_dt,'YYYYMM') IN (SELECT  TO_CHAR(sdl.dcr_dt,'YYYYMM') 
             FROM {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_detailingdata') }} sdl);
        {% endif %}"
    )
}}

with source as
(
    select * from {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_detailingdata') }}
),
final as
(
    SELECT TEAM_NAME		
    ,V_DTLID         
    ,V_EMPID         
    ,V_CUSTID     	
    ,DCR_DT          
    ,P1_DTL				
    ,P2_DTL       
    ,P3_DTL       
    ,P4_DTL
    ,CRT_DTTM
    ,convert_timezone('UTC',current_timestamp())::timestamp_ntz as updt_dttm
    ,filename
    FROM source
)
select team_name::varchar(20) as team_name,
    v_dtlid::varchar(20) as v_dtlid,
    v_empid::varchar(20) as v_empid,
    v_custid::varchar(20) as v_custid,
    dcr_dt::date as dcr_dt,
    p1_dtl::varchar(100) as p1_dtl,
    p2_dtl::varchar(100) as p2_dtl,
    p3_dtl::varchar(50) as p3_dtl,
    p4_dtl::varchar(50) as p4_dtl,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm,
    filename::varchar(50) as filename
    from final