{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}}
        WHERE TO_CHAR(dcr_dt,'YYYYMM') IN (SELECT TO_CHAR(sdl.dcr_dt,'YYYYMM') 
        FROM {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_rxdata') }} sdl
        where sdl.filename not in (
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_rxdata__null_test') }}
            union all
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_rxdata__duplicate_test') }}
        )   
        );
        {% endif %}"
    )
}}

with source as
(
    select *, dense_rank() over (partition by TO_CHAR(dcr_dt,'YYYYMM') order by filename desc) rn 
    from {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_rxdata') }}
    where filename not in (
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_rxdata__null_test') }}
            union all
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_rxdata__duplicate_test') }}
    ) qualify rn=1
),
final as
(
    SELECT TEAM_NAME		
    ,V_RXID		
    ,V_EMPID	
    ,V_CUSTID	
    ,DCR_DT			
    ,RX_PRODUCT	
    ,RX_UNITS
    ,CRT_DTTM
    ,convert_timezone('UTC',current_timestamp())::timestamp_ntz as updt_dttm
    ,filename
    FROM source
)
select team_name::varchar(20) as team_name,
    v_rxid::varchar(50) as v_rxid,
    v_empid::varchar(50) as v_empid,
    v_custid::varchar(50) as v_custid,
    dcr_dt::date as dcr_dt,
    rx_product::varchar(50) as rx_product,
    rx_units::number(15,2) as rx_units,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm,
    filename::varchar(50) as filename
    from final