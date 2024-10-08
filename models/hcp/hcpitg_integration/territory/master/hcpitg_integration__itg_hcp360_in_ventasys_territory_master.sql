{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}}
        WHERE (TEAM_NAME , V_TERRID ) IN (SELECT SEMP.TEAM_NAME , SEMP.V_TERRID 
        FROM {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_territory_master') }} SEMP
        INNER JOIN {{this}} IEMP
        ON SEMP.V_TERRID = IEMP.V_TERRID
        AND SEMP.TEAM_NAME = IEMP.TEAM_NAME
        where SEMP.filename not in (
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_territory_master__null_test') }}
            union all
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_territory_master__duplicate_test') }}
        )
        );
        {% endif %}"
    )
}}

with source as
(
    select *, dense_rank() over (partition by TEAM_NAME,V_TERRID order by filename desc) rn 
    from {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_territory_master') }}
    where filename not in (
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_territory_master__null_test') }}
            union all
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_territory_master__duplicate_test') }}
    ) qualify rn=1
),
final as
(
    SELECT TEAM_NAME
    ,V_TERRID 
    ,LVL1     
    ,LVL2     
    ,LVL3     
    ,HQ 
    ,CRT_DTTM
    ,convert_timezone('UTC',current_timestamp())::timestamp_ntz as updt_dttm
    ,filename
    FROM source
)
select team_name::varchar(20) as team_name,
    v_terrid::varchar(20) as v_terrid,
    lvl1::varchar(50) as lvl1,
    lvl2::varchar(50) as lvl2,
    lvl3::varchar(50) as lvl3,
    hq::varchar(100) as hq,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm,
    filename::varchar(50) as filename
    from final