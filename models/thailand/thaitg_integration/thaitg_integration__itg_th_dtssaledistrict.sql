with source as(
    select * from {{ source('thasdl_raw','sdl_mds_th_ref_city') }}
),
trans as (
select
  trim(code)::varchar(10) as sls_dist,
  trim(name)::varchar(100) as city,
  trim(region_code)::varchar(20) as region,
  trim(cityenglish)::varchar(100) as city_eng,
  null as blng_to_dstrbtr,
  to_char(cast(current_timestamp() as timestampntz), 'yyyymmddhh24missms')::varchar(255) as cdl_dttm,
  current_timestamp()::timestamp_ntz(9) as crtd_dttm,
  current_timestamp()::timestamp_ntz(9) as updt_dttm
from source
),

final as (
select
    sls_dist,
    city,
    region,
    city_eng,
    blng_to_dstrbtr,
    cdl_dttm,
    crtd_dttm,
    updt_dttm
from trans
)

select * from final