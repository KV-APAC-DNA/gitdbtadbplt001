{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} WHERE (team_name,v_custid_rtl) IN (SELECT sdl.team_name,sdl.v_custid_rtl
                                   FROM {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_rtlmaster') }} sdl 
                                   INNER JOIN {{this}}  itg
                                   ON rtrim(sdl.team_name) = rtrim(itg.team_name)
                                   AND rtrim(sdl.v_custid_rtl) = rtrim(itg.v_custid_rtl)
                                   where sdl.filename not in (
                                  select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_rtlmaster__null_test') }}
                                  union all
                                  select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_rtlmaster__duplicate_test') }}
                                  )
                                  )
        {% endif %}"
    )
}}
with sdl_hcp360_in_ventasys_rtlmaster as(
    select *, dense_rank() over (partition by team_name,v_custid_rtl order by filename desc ) as rn 
    from {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_rtlmaster') }} 
    where filename not in (
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_rtlmaster__null_test') }}
            union all
            select distinct file_name from {{ source('hcpwks_integration', 'TRATBL_sdl_hcp360_in_ventasys_rtlmaster__duplicate_test') }}
    ) qualify rn=1
),
final as 
(
select
  team_name::varchar(20) as team_name,
  v_custid_rtl::varchar(50) as v_custid_rtl,
  v_terrid::varchar(50) as v_terrid,
  cust_name::varchar(500) as cust_name,
  cust_type::varchar(50) as cust_type,
  is_active::varchar(1) as is_active,
  cust_endtered_date::date as cust_endtered_date,
  urc::number(30,0) as urc,
  crt_dttm::timestamp_ntz(9) as crt_dttm,
  convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm,
  filename::varchar(50) as filename
from
  sdl_hcp360_in_ventasys_rtlmaster
)
select
  *
from
  final
