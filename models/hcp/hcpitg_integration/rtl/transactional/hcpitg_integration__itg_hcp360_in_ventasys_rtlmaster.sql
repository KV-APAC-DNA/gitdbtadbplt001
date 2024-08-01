{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} WHERE (team_name,v_custid_rtl) IN (SELECT sdl.team_name,sdl.v_custid_rtl
                                   FROM {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_rtlmaster') }} sdl 
                                   INNER JOIN {{this}}  itg
                                   ON sdl.team_name = itg.team_name
                                   AND sdl.v_custid_rtl = itg.v_custid_rtl
                                   )
        {% endif %}"
    )
}}
with sdl_hcp360_in_ventasys_rtlmaster as(
    select * from {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_rtlmaster') }}
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
