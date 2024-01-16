with
source as (

    select * from {{ source('aspsdl_raw', 'sdl_mds_ap_company_dim') }}

),

final as
(
  select
    name::varchar(500) as name,
    code::varchar(500) as code,
    ctry_key::varchar(200) as ctry_key,
    ctry_group::varchar(200) as ctry_group,
    cluster::varchar(200) as cluster,
    current_timestamp()::timestamp_ntz(9) as crtd_dttm
  from source
)
select * from final
