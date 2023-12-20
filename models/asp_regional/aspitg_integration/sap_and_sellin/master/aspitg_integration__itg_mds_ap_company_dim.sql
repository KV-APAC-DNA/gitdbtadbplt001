{{
  config(
    alias="itg_mds_ap_company_dim",
    tags= [""]
  )
}}
with
source as (

    select * from {{ source('aspsdl_raw', 'sdl_mds_ap_company_dim') }}

),

final as
(
  select
    name,
    code,
    ctry_key,
    ctry_group,
    cluster,
    current_timestamp()::timestamp_ntz(9) as crt_dttm
  from source
)
select * from final
