with source as(
    select * from {{ source('thasdl_raw','sdl_mds_th_ref_city') }}
),
final as (
select
  trim(code)::varchar(10) as sls_dist,
  trim(name)::varchar(100) as city,
  trim(region_code)::varchar(20) as region,
  trim(cityenglish)::varchar(100) as city_eng,
  null as blng_to_dstrbtr,
  to_char(cast(current_timestamp() as timestampntz), 'yyyymmddhh24missff3')::varchar(255) as cdl_dttm,
  current_timestamp()::timestamp_ntz(9) as crtd_dttm,
  current_timestamp()::timestamp_ntz(9) as updt_dttm
from source
)

select * from final