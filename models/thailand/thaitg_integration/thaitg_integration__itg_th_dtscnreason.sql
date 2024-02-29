with source as(
    select * from {{ source('thasdl_raw','sdl_mds_th_distributor_cn_reason') }}
),
final as (
select
  trim(code)::varchar(5) as cn_reason,
  trim(name)::varchar(250) as cn_th_desc,
  trim(cn_en_desc)::varchar(250) as cn_en_desc,
  to_char(cast(current_timestamp() as timestampntz(9)), 'yyyymmddhh24missff3')::varchar(255) as cdl_dttm,
  current_timestamp()::timestamp_ntz(9) as crtd_dttm,
  current_timestamp()::timestamp_ntz(9) as updt_dttm
from source
)
select * from final