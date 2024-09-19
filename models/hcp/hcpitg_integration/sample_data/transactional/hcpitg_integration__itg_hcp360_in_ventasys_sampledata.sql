{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}}
        WHERE TO_CHAR(dcr_dt,'YYYYMM') IN (SELECT  TO_CHAR(sdl.dcr_dt,'YYYYMM') 
        FROM {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_sampledata') }} sdl
        where sdl.filename not in (
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_sampledata__null_test') }}
            union all
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_sampledata__duplicate_test') }}
        )
        );
        {% endif %}"
    )
}}

with source as
(
    select *, dense_rank() over (partition by TO_CHAR(dcr_dt,'YYYYMM') order by filename desc) rn 
    from {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_sampledata') }}
    where filename not in (
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_sampledata__null_test') }}
            union all
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_sampledata__duplicate_test') }}
    ) qualify rn=1
),
final as
(
    SELECT TEAM_NAME			
    ,V_SAMPLEID        
    ,V_EMPID           
    ,V_CUSTID          
    ,DCR_DT				
    ,SAMPLE_PRODUCT
    ,SAMPLE_UNITS  
    ,CATEGORY
    ,CRT_DTTM
    ,convert_timezone('UTC',current_timestamp())::timestamp_ntz as updt_dttm
    ,filename
    FROM source
)
select team_name::varchar(20) as team_name,
    v_sampleid::varchar(20) as v_sampleid,
    v_empid::varchar(20) as v_empid,
    v_custid::varchar(20) as v_custid,
    dcr_dt::date as dcr_dt,
    sample_product::varchar(100) as sample_product,
    sample_units::number(15,2) as sample_units,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm,
    filename::varchar(50) as filename,
    category::varchar(50) as category
    from final