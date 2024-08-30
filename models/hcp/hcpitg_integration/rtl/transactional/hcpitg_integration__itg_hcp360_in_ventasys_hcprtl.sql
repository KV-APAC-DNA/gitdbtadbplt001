
{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                    delete from {{this}} where (team_name, v_custid_dr, v_custid_rtl) in 
                    (select  sdl.team_name,  sdl.v_custid_dr,  sdl.v_custid_rtl 
                    from {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_hcprtl') }} sdl 
                    inner join {{this}} itg on rtrim(sdl.team_name) = rtrim(itg.team_name)  
                    and rtrim(sdl.v_custid_dr) = rtrim(itg.v_custid_dr)  and rtrim(sdl.v_custid_rtl) = rtrim(itg.v_custid_rtl)
                    where sdl.filename not in (
                        select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_hcprtl__null_test') }}
                        union all
                        select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_hcprtl__duplicate_test') }}
                    )
                    )
        {% endif %}"
    )
}}
with sdl_hcp360_in_ventasys_hcprtl as (
    select *,dense_rank() over (partition by team_name, v_custid_dr, v_custid_rtl order by filename desc ) as rn 
    from {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_hcprtl') }} 
    where filename not in (
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_hcprtl__null_test') }}
            union all
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_hcprtl__duplicate_test') }}
    ) qualify rn=1
),
final as (
    select
        team_name::varchar(20) as team_name,
        v_custid_dr::varchar(50) as v_custid_dr,
        v_custid_rtl::varchar(50) as v_custid_rtl,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm,
        filename::varchar(50) as filename
    from
        sdl_hcp360_in_ventasys_hcprtl
)
select * from final

